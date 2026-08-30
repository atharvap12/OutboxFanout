# Verifying Phase 4 — Billing + Shipping Consumers (Postgres UNIQUE Idempotency)

**Goal.** Two consumers that each process a given `order_id` at most once,
using a database-level guarantee rather than an in-code check.

**STOP condition.** Redeliver the same message twice; only one row is ever
inserted, for Billing and Shipping independently.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a

PSQL="docker exec outboxFanout-postgres psql -U $PG_USER -d $PG_DB -At"
```

Bring everything up:

```bash
docker compose up -d --build
docker compose ps
```

Expect `bootstrap` **exited (0)** and `order`, `relay`, `billing`, `shipping`
all up.

---

## Step 1 — The tables and their constraints exist

```bash
docker exec outboxFanout-postgres psql -U "$PG_USER" -d "$PG_DB" \
  -c '\d billing_records' -c '\d shipments'
```

The line that matters, on each:

```
"billing_records_order_id_key" UNIQUE CONSTRAINT, btree (order_id)
"shipments_order_id_key"       UNIQUE CONSTRAINT, btree (order_id)
```

Two separate constraints on two separate tables. Deleting a `billing_records`
row must not let Shipping ship twice — the consumers share no dedup state.

---

## Step 2 — One order, one row each

```bash
ORDER=$(curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-stop4","item":"Standing desk","amount":"899.00"}')
OID=$(echo "$ORDER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
echo "$OID"

docker compose logs billing shipping --no-log-prefix --since 1m | grep -E 'BILLED|SHIPPED'
```

```
💳 BILLED  order 274af00a-… — cust-stop4 899.00 (event 1a2acb08-…)
📦 SHIPPED order 274af00a-… — 'Standing desk' tracking TRK-8F580D938B93 (event 1a2acb08-…)
```

Both consumers saw the same event, independently.

---

## Step 3 — THE STOP CONDITION: redeliver twice

The most faithful way to force a duplicate is the one that happens for real —
make the relay republish the row, exactly as it would after a Scenario A crash:

```bash
for round in 1 2; do
  $PSQL -c "UPDATE outbox SET published=false, published_at=NULL WHERE order_id='$OID';"
  until [ "$($PSQL -c "SELECT published FROM outbox WHERE order_id='$OID'")" = "t" ]; do sleep 1; done
  sleep 6
  $PSQL -c "SELECT 'billing_records='||count(*) FROM billing_records WHERE order_id='$OID'
            UNION ALL SELECT 'shipments='||count(*) FROM shipments WHERE order_id='$OID';"
done
```

**Recorded output — three deliveries of one event:**

```
15:25:49  💳 BILLED    order 274af00a-… (event 1a2acb08-…)
15:25:49  📦 SHIPPED   order 274af00a-… (event 1a2acb08-…)
15:27:02  🔁 DUPLICATE ignored — already billed  (event 1a2acb08-…)
15:27:02  🔁 DUPLICATE ignored — already shipped (event 1a2acb08-…)
15:27:10  🔁 DUPLICATE ignored — already billed  (event 1a2acb08-…)
15:27:10  🔁 DUPLICATE ignored — already shipped (event 1a2acb08-…)

billing_records=1
shipments=1
```

Note the **`event_id` is identical in all six lines**. The outbox row's id does
not change however many times it is republished, which is what makes it a
usable identity — unlike the SNS MessageId (fresh per publish) or the SQS
MessageId (fresh per queue, per delivery).

✅ **STOP condition met.**

---

## Step 4 — ⚠️ `rowcount` does NOT work here

The design doc says to "check whether a row was actually inserted (rowcount)".
On this stack — SQLAlchemy 2.0 + psycopg 3 + an ORM-entity `INSERT` — that
silently does not work. Measured:

```bash
docker compose exec -T billing python -c "
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy.dialects.postgresql import insert as pg_insert
from shared.db import session_scope
from billing.models import BillingRecord

oid = uuid.uuid4()
for attempt in (1, 2):
    with session_scope() as s:
        stmt = (pg_insert(BillingRecord).values(
            id=uuid.uuid4(), order_id=oid, event_id=uuid.uuid4(),
            customer_id='probe', amount=Decimal('1.00'),
            processed_at=datetime.now(timezone.utc),
        ).on_conflict_do_nothing(index_elements=['order_id']))
        r = s.execute(stmt)
        print(f'attempt {attempt}: rowcount={r.rowcount!r}')
"
```

```
attempt 1: rowcount=-1
attempt 2: rowcount=-1
```

`-1` means **"not available"**, not "zero rows". So `rowcount == 1` is never
true, and the first, genuinely fresh event gets logged as a duplicate. That is
exactly what happened on the first run of this phase:

```
15:14:52 🔁 DUPLICATE ignored for order b08dfb6c-… — already billed
```

…while `SELECT count(*)` showed the row **had** been inserted.

**Why this is worse than it looks.** The database was never wrong — the UNIQUE
constraint did its job and exactly one row exists either way. What was wrong
was the *reported outcome*, which is the only visible evidence that duplicate
detection works at all. Here the side effect (the row) and the dedup check are
the same write, so a wrong flag only corrupts the log. In Phase 5 the side
effect is separate from the check, and the same class of bug would skip the
work entirely.

**The fix is `RETURNING`.** `ON CONFLICT DO NOTHING` raises no exception on
either branch, so something has to distinguish them. A `RETURNING` clause emits
one row on insert and none on conflict — unambiguous, driver-independent:

```python
.on_conflict_do_nothing(index_elements=["order_id"])
.returning(BillingRecord.id)
...
fresh = session.execute(stmt).scalar_one_or_none() is not None
```

SQLAlchemy is explicit that `rowcount` is only meaningful for UPDATE and
DELETE: https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.CursorResult.rowcount

---

## Step 5 — ⚠️ Long polling vs. the HTTP read timeout

First run of the consumers, every single receive failed:

```
botocore.exceptions.ReadTimeoutError: Read timeout on endpoint URL: "http://localstack:4566/"
```

Nothing was wrong with the queue. `shared/aws.py` set `read_timeout=10` for all
clients, and a 20-second long poll deliberately holds the HTTP response open
for 20 seconds — so **the client hung up before the server was finished
waiting**. Long polling and a short read timeout are mutually exclusive.

The fix is a separate SQS client config:

```python
_SQS_CONFIG = _BOTO_CONFIG.merge(Config(read_timeout=config.SQS_WAIT_TIME_SECONDS + 10))
```

Verify no timeouts recur:

```bash
docker compose logs billing shipping --since 2m | grep -c ReadTimeout   # -> 0
```

---

## Step 6 — The loop survives a poison message

```bash
QURL=$(docker compose exec -T billing python -c \
  "from shared import aws,config; print(aws.queue_url(config.BILLING_QUEUE_NAME))")

docker compose exec -T billing python -c "
from shared import aws
aws.sqs().send_message(QueueUrl='$QURL', MessageBody='{not valid json at all')"
```

```
15:29:51 ERROR [billing] shared.consumer: unparsable body on delivery #1 — left on the queue for the DLQ
json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes
```

```bash
docker compose ps billing --format '{{.Status}}'     # -> Up 8 minutes
```

Two deliberate choices visible here:

- The consumer **does not die**. One bad message must not take out the service.
- The message is **not deleted**. Retrying cannot help, but deleting would
  destroy the evidence. Leaving it lets SQS count deliveries and move it to a
  DLQ — which does not exist yet. **Until Phase 6 adds DLQs, a poison message
  redelivers forever**, so purge it after testing:

```bash
docker compose exec -T billing python -c "
from shared import aws; aws.sqs().purge_queue(QueueUrl='$QURL')"
```

Confirm the system still works afterwards by creating one more order.

---

## Step 7 — The consumers are genuinely independent

Stop one and prove the other is unaffected (a preview of Scenario C):

```bash
docker compose stop shipping
ORDER=$(curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-solo","item":"Webcam","amount":"75.00"}')
OID=$(echo "$ORDER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')

$PSQL -c "SELECT count(*) FROM billing_records WHERE order_id='$OID';"
$PSQL -c "SELECT count(*) FROM shipments      WHERE order_id='$OID';"
```

**Recorded:**

```
after (shipping still down):  billing=1   shipments=0
shipping-queue depth:         1 message(s) waiting
```

Billing processed immediately and was never aware Shipping was down. The event
was not lost, not retried, not blocked — it simply sat in shipping-queue.
Restart, and the backlog drains itself:

```bash
docker compose start shipping
```

```
14:36:26 📦 SHIPPED order e5c5c169-… — 'Webcam' tracking TRK-BBE25304AAFF
after restart:  shipments=1
```

This works only because each queue holds its own independent copy with its own
delivery state. It is Scenario C in miniature, and it is the payoff for fanning
out through SNS instead of having the relay call three consumers itself.

---

## Phase 4 checklist

- [x] `billing_records` and `shipments` each have a UNIQUE constraint on `order_id`
- [x] One order → exactly one row in each table
- [x] Redelivered twice → still exactly one row in each table
- [x] Logs distinguish fresh from duplicate, per consumer
- [x] The fresh/duplicate decision comes from `RETURNING`, not `rowcount`
- [x] SQS message is deleted after the transaction commits, never before
- [x] An unparsable message does not kill the loop and is not deleted
- [x] Neither consumer image contains `order/` — the boundary is a build fact
- [x] Stopping one consumer does not affect the other; its backlog drains on restart

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `ReadTimeoutError` on every receive | `read_timeout` < `SQS_WAIT_TIME_SECONDS` — see Step 5 |
| Every event logs as DUPLICATE | using `rowcount` instead of `RETURNING` — see Step 4 |
| `NonExistentQueue` | bootstrap did not run, or LocalStack restarted; `docker compose up -d bootstrap` |
| Consumer idle, queue has depth | check the SNS filter policy still matches `event_type` |
| Constraint change had no effect | `create_all()` never ALTERs; `docker compose down -v` |
| Poison message repeats forever | expected until Phase 6 adds DLQs; purge the queue |
