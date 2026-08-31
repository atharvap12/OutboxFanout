# Verifying Phase 4 — Billing + Shipping Consumers (Postgres UNIQUE Idempotency)

Commands to prove the Phase 4 STOP condition on demand:

> **Manually redeliver a message twice; only one row is ever inserted, for both
> Billing and Shipping independently.**

**What changed.** Phase 3 left three pigeonholes filling up with nobody
collecting. Now two of them have a reader each. The readers are the first part
of this system that must survive being told the same thing twice — everything
upstream is allowed to repeat itself, and this is where that stops being
someone else's problem.

**The one idea in this phase.** You cannot stop duplicates arriving. SQS is
at-least-once by design, the relay republishes after a crash, boto3 retries a
lost acknowledgement. So the question is never "how do I prevent the second
delivery?" but **"how do I make the second delivery do nothing?"** — and the
answer here is to let the database refuse it, rather than to check in code.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a

PSQL="docker exec outboxFanout-postgres psql -U $PG_USER -d $PG_DB -At"

# Row counts for one order, from both consumers at once.
counts() {
  $PSQL -c "SELECT 'billing_records='||count(*) FROM billing_records WHERE order_id='$1'
            UNION ALL
            SELECT 'shipments='||count(*)       FROM shipments       WHERE order_id='$1';"
}

# Create an order, echo its id.
neworder() {
  curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
    -d "$1" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])'
}
```

Bring the stack up:

```bash
docker compose up -d --build
docker compose ps -a
```

Expect `bootstrap` **Exited (0)** and `order`, `relay`, `billing`, `shipping`
all up. `redis` is up but unused until Phase 5.

---

## Step 1 — The tables exist, with the constraint that does the work

```bash
docker exec outboxFanout-postgres psql -U "$PG_USER" -d "$PG_DB" \
  -c '\d billing_records' -c '\d shipments'
```

The lines that matter:

```
"billing_records_order_id_key" UNIQUE CONSTRAINT, btree (order_id)
"shipments_order_id_key"       UNIQUE CONSTRAINT, btree (order_id)
```

**Two separate constraints on two separate tables**, not one shared dedup
table. They share nothing: if every `billing_records` row were deleted
tomorrow, Shipping would still refuse to ship the same order twice, because its
evidence lives in its own table. That is the design doc's "each consumer owns
its own idempotency store" — and it is what stops one consumer's bad deploy
from becoming everyone's outage.

Note also what is **absent**: no foreign key to `orders`. These consumers share
a Postgres instance with the Order Service only as a convenience of this
project. A FK would mean Billing secretly depends on another service's schema,
which would make the "independent failure domains" claim false.

---

## Step 2 — One order, one row each

```bash
OID=$(neworder '{"customer_id":"cust-stop4","item":"Standing desk","amount":"899.00"}')
echo "$OID"
sleep 5
docker compose logs billing shipping --no-log-prefix | grep -E 'BILLED|SHIPPED'
```

```
💳 BILLED  order cff92d9b-… — cust-stop4 899.00 (event 4c8cacf5-…)
📦 SHIPPED order cff92d9b-… — 'Standing desk' tracking TRK-998684568AEA (event 4c8cacf5-…)
```

Both consumers saw the same event and each took its own notes from it — Billing
recorded the amount, Shipping recorded the item and a tracking number. Neither
stored the other's fields, because neither needs them.

---

## Step 3 — THE STOP CONDITION: redeliver the same event twice

You could publish to SNS by hand, but there is a more honest way: make the
**relay** republish, which is literally what happens after a Scenario A crash.
Setting `published = false` puts the row back in the relay's in-tray.

```bash
for round in 1 2; do
  echo "--- redelivery $round ---"
  $PSQL -c "UPDATE outbox SET published=false, published_at=NULL WHERE order_id='$OID';"
  until [ "$($PSQL -c "SELECT published FROM outbox WHERE order_id='$OID'")" = "t" ]; do sleep 1; done
  sleep 7
  counts "$OID"
done
```

**Recorded output — three deliveries of one event:**

```
13:52:15  💳 BILLED    order cff92d9b-…  (event 4c8cacf5-…)
13:52:15  📦 SHIPPED   order cff92d9b-…  (event 4c8cacf5-…)
13:52:17  🔁 DUPLICATE ignored — already billed   (event 4c8cacf5-…)
13:52:17  🔁 DUPLICATE ignored — already shipped  (event 4c8cacf5-…)
13:52:27  🔁 DUPLICATE ignored — already billed   (event 4c8cacf5-…)
13:52:27  🔁 DUPLICATE ignored — already shipped  (event 4c8cacf5-…)

billing_records=1
shipments=1
```

✅ **STOP condition met.**

**Read the `event_id` in all six lines: it is identical.** That is the proof
this was ONE logical event delivered three times, and it is only possible
because the dedup key comes from our own domain. Had we keyed on a transport
id, all three deliveries would have carried a different one:

| identifier | changes when? | usable as a dedup key? |
| --- | --- | --- |
| `event_id` (our outbox row id) | never | ✅ yes |
| SNS MessageId | every publish | ❌ a relay republish looks like a new event |
| SQS MessageId | per queue, per copy | ❌ worse — one event has three of them |

The SQS MessageId is the nastiest of the three, because it **fails
asymmetrically**: an SQS redelivery reuses it (so dedup appears to work), but a
relay republish mints a new one (so dedup silently fails). It would pass
exactly the tests you would naturally write, and break only during a real
crash.

---

## Step 4 — ⚠️ Why `rowcount` is not used, though the design doc suggests it

`ON CONFLICT DO NOTHING` raises no exception in either case, so *something*
must tell "I inserted it" apart from "it was already there". The design doc
suggests checking `rowcount`. On this stack — SQLAlchemy 2.0, psycopg 3, an ORM
entity, an INSERT — that silently does not work.

Run both mechanisms side by side:

```bash
docker compose exec -T billing python -c "
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy.dialects.postgresql import insert as pg_insert
from shared.db import session_scope
from billing.models import BillingRecord

def attempt(order_id, use_returning):
    with session_scope() as s:
        stmt = (pg_insert(BillingRecord).values(
            id=uuid.uuid4(), order_id=order_id, event_id=uuid.uuid4(),
            customer_id='probe', amount=Decimal('1.00'),
            processed_at=datetime.now(timezone.utc),
        ).on_conflict_do_nothing(index_elements=['order_id']))
        if use_returning:
            r = s.execute(stmt.returning(BillingRecord.id)).scalar_one_or_none()
            return 'None (duplicate)' if r is None else f'{r} (fresh)'
        return s.execute(stmt).rowcount

a = uuid.uuid4()
print('rowcount  -- 1st insert (should say fresh):', attempt(a, False))
print('rowcount  -- 2nd insert (should say dup)  :', attempt(a, False))
b = uuid.uuid4()
print('RETURNING -- 1st insert (should say fresh):', attempt(b, True))
print('RETURNING -- 2nd insert (should say dup)  :', attempt(b, True))
"
```

**Recorded:**

```
rowcount  -- 1st insert (should say fresh): -1
rowcount  -- 2nd insert (should say dup)  : -1
RETURNING -- 1st insert (should say fresh): 7d9f5bbb-… (fresh)
RETURNING -- 2nd insert (should say dup)  : None (duplicate)
```

`-1` does not mean "zero rows". It means **"I do not have that information for
you."** So `rowcount == 1` is never true, and every event — including genuinely
fresh ones — gets reported as a duplicate. That is exactly what happened on the
first run of this phase: the log said `🔁 DUPLICATE` while `SELECT count(*)`
showed the row *had* been inserted.

**Why this bug is worth more than the fix.** The database was never wrong. The
UNIQUE constraint did its job and exactly one row existed either way. What was
wrong was only the **report** — which means:

> `SELECT count(*) = 1` **passes on completely broken duplicate-detection
> code.** The obvious test cannot see this bug at all.

The only symptom is a log line that lies, and the log line is the entire
deliverable of Phase 4 ("logs should make it obvious when a duplicate was
correctly caught"). **Assert on the mechanism, not only on the outcome.**

It is harmless *here* purely because the dedup check and the side effect are
the same write. In Phase 5 they are separate — the flag decides whether the
notification is sent — and the identical bug would silently skip every email.

SQLAlchemy documents `rowcount` as meaningful only for UPDATE and DELETE:
https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.CursorResult.rowcount

---

## Step 5 — ⚠️ Long polling versus the HTTP read timeout

On the first run, **every** receive failed instantly:

```
botocore.exceptions.ReadTimeoutError: Read timeout on endpoint URL: "http://localstack:4566/"
```

Nothing was wrong with the queue. `shared/aws.py` set `read_timeout=10` for all
clients back in Phase 2, and a 20-second long poll **deliberately** holds the
HTTP response open for 20 seconds:

```
t=0s    consumer: "any messages? I'll wait up to 20s."
t=10s   boto3:    "no answer in 10s, this is broken"  -> hangs up
t=20s   SQS:      "...no, nothing."  (to a closed socket)
```

**We hung up on a service that was doing exactly what we asked.** The fix is a
separate SQS client config in `shared/aws.py`:

```python
_SQS_CONFIG = _BOTO_CONFIG.merge(Config(read_timeout=config.SQS_WAIT_TIME_SECONDS + 10))
```

Confirm it stays fixed:

```bash
docker compose logs billing shipping --since 3m | grep -c ReadTimeout    # -> 0
```

**The general lesson:** a timeout is an assertion about *expected latency*, and
long polling deliberately inverts that expectation. One shared client config
across services with different latency profiles is a trap that springs the
moment you add your first intentionally-slow call.

---

## Step 6 — A poison message must not kill the loop

```bash
QURL=$(docker compose exec -T billing python -c \
  "from shared import aws,config; print(aws.queue_url(config.BILLING_QUEUE_NAME))")

docker compose exec -T billing python -c "
from shared import aws
aws.sqs().send_message(QueueUrl='$QURL', MessageBody='{not valid json at all')"
```

```
13:57:33 ERROR [billing] shared.consumer: unparsable body on delivery #1 — left on the queue for the DLQ
json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes
```

```bash
docker compose ps billing --format '{{.Status}}'     # -> Up 9 minutes
```

Two deliberate choices are visible here:

1. **The consumer does not die.** One malformed message must not take out the
   service; the other nine in the batch and every message after it are fine.
2. **The message is not deleted.** Retrying cannot help — it will be exactly as
   unparsable in 30 seconds — but deleting it destroys the evidence. Leaving it
   lets SQS count deliveries and, once Phase 6 attaches a DLQ, move it there
   automatically: off the main queue, but *saved*.

> ⚠️ **Until Phase 6 exists, a poison message redelivers forever** (every 30s).
> This is correct behaviour with the safety net not yet attached. Purge after
> testing:

```bash
docker compose exec -T billing python -c "
from shared import aws; aws.sqs().purge_queue(QueueUrl='$QURL')"
```

Then create one more order to confirm the pipeline still works.

---

## Step 7 — The consumers are genuinely independent (Scenario C preview)

```bash
docker compose stop shipping
OID=$(neworder '{"customer_id":"cust-solo","item":"Webcam","amount":"75.00"}')
sleep 6
counts "$OID"
```

**Recorded:**

```
billing_records=1
shipments=0
shipping-queue depth while consumer is down: 1
```

Billing processed immediately and was never aware Shipping was down. The event
was not lost, not retried, not blocked — it simply **sat in shipping-queue**.

```bash
docker compose start shipping
sleep 10
counts "$OID"
```

```
14:00:41 📦 SHIPPED order 81b934d9-… — 'Webcam' tracking TRK-AC64F7F52F57
shipments=1
```

This works only because each queue holds its own independent copy with its own
delivery state and no shared cursor. It is the payoff for fanning out through
SNS rather than having the relay call three consumers itself — and it is why
`billing` and `shipping` deliberately have **no `depends_on: order` or
`depends_on: relay`** in compose.

---

## Step 8 — Graceful shutdown, and why `stop_grace_period` is 25s

```bash
docker compose stop shipping
docker compose logs shipping --no-log-prefix | grep -E 'SIGTERM|stopped cleanly' | tail -2
```

**Recorded:**

```
13:58:57 received SIGTERM — finishing the current message, then stopping
13:59:07 consumer stopped cleanly
```

Ten seconds — because SIGTERM arrived during a long poll, and the loop can only
notice it once `receive_message()` returns. **Docker's default grace period is
exactly 10 seconds**, so this stop was right at the edge; a slightly later
signal would have been SIGKILLed mid-wait.

That is not merely untidy. SIGKILL during a handler is precisely the
crash-between-process-and-delete case, so a too-short grace period would
**manufacture the failure we want to trigger deliberately — on every single
deploy.** Hence `stop_grace_period: 25s`, comfortably longer than the 20s poll.

---

## Phase 4 checklist

- [x] `billing_records` and `shipments` each have a UNIQUE constraint on `order_id`
- [x] The two constraints are independent; neither consumer reads the other's table
- [x] One order → exactly one row in each table
- [x] Redelivered twice → still exactly one row in each table
- [x] The same `event_id` appears in all deliveries of one event
- [x] Logs clearly distinguish fresh from duplicate, per consumer
- [x] The fresh/duplicate decision comes from `RETURNING`, never `rowcount`
- [x] The SQS message is deleted *after* the transaction commits, never before
- [x] An unparsable message neither kills the loop nor gets deleted
- [x] Stopping one consumer does not affect the other; its backlog drains on restart
- [x] Neither consumer image contains `order/` — the boundary is a build fact

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `ReadTimeoutError` on every receive | `read_timeout` < `SQS_WAIT_TIME_SECONDS` — Step 5 |
| Every event logs as DUPLICATE | using `rowcount` instead of `RETURNING` — Step 4 |
| `NonExistentQueue` | bootstrap never ran, or LocalStack restarted: `docker compose up -d bootstrap` |
| Consumer idle while the queue has depth | the SNS filter policy no longer matches `event_type` |
| A constraint change had no effect | `create_all()` never ALTERs an existing table; `docker compose down -v` |
| Poison message repeats forever | expected until Phase 6 adds DLQs; purge the queue |
| Consumer SIGKILLed on every stop | `stop_grace_period` shorter than the long poll — Step 8 |
