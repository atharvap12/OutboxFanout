# Verifying Phase 2 — Outbox Relay (Poll + Publish to SNS)

Commands to prove the Phase 2 STOP condition on demand:

> **The relay picks up a row, you can see the SNS publish succeed in LocalStack
> logs, and the outbox row is correctly marked published.**

Plus a full run of **Scenario A** — kill the relay between the publish and the
mark — which is the single most important proof in the whole project.

**What Phase 2 is NOT.** No SQS queues, no subscriptions, no consumers. One
topic, one publisher. The throwaway queue in Step 4 exists only so you can read
a message with your own eyes; it is deleted immediately and Phase 3 replaces it
properly.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a
```

Paste once per shell:

```bash
until_ready() {
  for _ in $(seq 1 60); do
    curl -sf localhost:8000/health >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "order service did not become ready" >&2; return 1
}

# awslocal is LocalStack's AWS CLI wrapper: it pre-points --endpoint-url at
# 4566, so you never have to type it.
LS="docker compose exec -T localstack awslocal"

psql_() { docker compose exec -T postgres psql -U $PG_USER -d $PG_DB "$@"; }
```

---

## Step 1 — Clean slate and start

```bash
docker compose down -v
docker compose up -d --build order relay
until_ready
docker compose ps
```

Expect `postgres (healthy)`, `localstack (healthy)`, `order Up`, `relay Up`.

```bash
docker compose logs relay
```

```
relay starting: poll every 2s, batch 10, topic 'order-events' at http://localstack:4566
SNS topic 'order-events' resolved to arn:aws:sns:us-east-1:000000000000:order-events
```

Two things to notice.

**The topic was created, not found.** `create_topic` is idempotent — an existing
name returns its ARN, a missing one creates it — so the relay bootstraps its own
topic on every start. That is not a convenience; LocalStack has no free
persistence, so the topic is *gone* after every restart. Any setup step you
would have to run by hand would be a step you eventually forget.

**Then silence.** The relay polls every 2 seconds forever, and logs nothing when
there is no work. A poll loop that logs each empty tick produces 43,200 useless
lines a day and buries the ones that matter.

---

## Step 2 — The happy path

```bash
curl -s -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-p2","item":"Phase 2 test","amount":"250.00"}' | jq
```

The `201` comes back immediately — the Order Service still does not know the
relay exists. Wait one poll interval:

```bash
sleep 4
docker compose logs relay --tail 3
```

```
relay.service: published outbox row 0b86… (OrderCreated, order c86a…) -> SNS MessageId 42b0…
relay: poll: published 1 of 1 pending row(s)
```

### 2a. The outbox row flipped

```bash
psql_ -c "select event_type, published, published_at from outbox;"
```

```
  event_type  | published |          published_at
--------------+-----------+-------------------------------
 OrderCreated | t         | 2026-08-17 19:55:12.34+00
```

### 2b. LocalStack agrees — **this is the STOP condition**

```bash
docker compose logs localstack | grep -E 'sns\.(CreateTopic|Publish)'
```

```
AWS sns.CreateTopic => 200
AWS sns.Publish => 200
```

The MessageId in the relay's log is what *boto3 was told*. This line is what
*the broker recorded*. They should agree, and checking both is the habit worth
building — an SDK that returns successfully is a claim, not a fact.

---

## Step 3 — The relay is a courier, not a source of truth

Stop the relay and keep ordering:

```bash
docker compose stop relay
for n in 1 2 3; do
  curl -s -o /dev/null -X POST localhost:8000/orders \
    -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"backlog-$n\",\"item\":\"Queued $n\",\"amount\":\"$n.00\"}"
done
psql_ -tAc "select count(*) from outbox where published = false;"   # 3
```

**Order creation was completely unaffected** — three more `201`s with the
publisher dead. That independence is the reason the relay is a separate process.
The events are not lost, they are *pending*: durable in Postgres, in the same
transaction as the orders themselves.

```bash
docker compose start relay
sleep 4
psql_ -tAc "select count(*) from outbox where published = false;"   # 0
```

The backlog drains with no intervention. Nothing had to be replayed by hand,
because nothing was ever lost.

---

## Step 4 — See the actual message (throwaway queue)

The MessageId proves *something* was accepted. This proves *what*.

> Strictly Phase 3 machinery, used here as a debugging lens. Created and
> destroyed inside this step; Phase 3 builds the real thing.

```bash
TOPIC=$($LS sns create-topic --name order-events --query TopicArn --output text)
QURL=$($LS sqs create-queue --queue-name phase2-peek --query QueueUrl --output text)
QARN=$($LS sqs get-queue-attributes --queue-url "$QURL" \
        --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
SUB=$($LS sns subscribe --topic-arn "$TOPIC" --protocol sqs \
        --notification-endpoint "$QARN" \
        --attributes RawMessageDelivery=true --query SubscriptionArn --output text)

curl -s -o /dev/null -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-peek","item":"See the real message","amount":"99.95"}'
sleep 4

$LS sqs receive-message --queue-url "$QURL" --query 'Messages[0].Body' \
   --output text | python3 -m json.tool
```

```json
{
    "event_id": "47c862a2-ac58-4d32-bcd7-87b517b2b518",
    "event_type": "OrderCreated",
    "order_id": "df2d164d-aa4f-4fef-a751-18147675bd1e",
    "occurred_at": "2026-08-17T19:58:10.505148+00:00",
    "payload": {
        "item": "See the real message",
        "amount": "99.95",
        "order_id": "df2d164d-aa4f-4fef-a751-18147675bd1e",
        "created_at": "2026-08-17T19:58:10.500579+00:00",
        "customer_id": "cust-peek"
    }
}
```

Check three things:

- **`amount` is still the string `"99.95"`.** It survived Python → JSONB →
  Python → JSON → SNS → SQS without ever becoming a float.
- **`event_id` is the outbox row id**, not the SNS MessageId. Step 5 shows why
  that distinction is the difference between working and broken idempotency.
- **`occurred_at` is when the order was created, not when it was published.**
  The gap between them is relay lag. A consumer that cares about staleness needs
  the former; a consumer given only the latter cannot tell a fresh event from a
  three-hour-old backlog replay.

> `RawMessageDelivery=true` delivers the body as-is. Without it SNS wraps the
> message in its own envelope (`{"Type":"Notification","Message":"…"}`) and your
> JSON arrives as a *string inside a field*. Phase 3 has to decide this
> deliberately — it changes the parsing code in all three consumers.

```bash
$LS sns unsubscribe --subscription-arn "$SUB"
$LS sqs delete-queue --queue-url "$QURL"
```

---

## Step 5 — SCENARIO A: crash between publish and mark

**The single most important proof in this project.** Everything else is
plumbing; this is the failure the outbox pattern exists to survive.

Keep a throwaway queue subscribed so you can *count* the duplicate:

```bash
TOPIC=$($LS sns create-topic --name order-events --query TopicArn --output text)
QURL=$($LS sqs create-queue --queue-name scenarioA --query QueueUrl --output text)
QARN=$($LS sqs get-queue-attributes --queue-url "$QURL" \
        --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
SUB=$($LS sns subscribe --topic-arn "$TOPIC" --protocol sqs \
        --notification-endpoint "$QARN" \
        --attributes RawMessageDelivery=true --query SubscriptionArn --output text)
```

### 5a. Arm the fault and fire

```bash
CRASH_AFTER_PUBLISH=1 docker compose up -d relay
sleep 4
curl -s -o /dev/null -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-crash","item":"Scenario A","amount":"777.00"}'
sleep 5
```

### 5b. The relay is dead

```bash
docker compose ps -a relay
docker inspect --format '{{.State.ExitCode}}' $(docker compose ps -aq relay)   # 17
docker compose logs relay --tail 2
```

```
CRITICAL relay.service: CRASH_AFTER_PUBLISH — event e1ed… is on SNS but the
outbox row is still unpublished. Restart the relay: it will publish it again,
and every consumer must no-op on the duplicate.
```

Exit code **17**, not 0 and not 1 — a distinctive code so the test asserts the
process died *where we intended* rather than for some unrelated reason.

> **Why `os._exit()` and not `sys.exit()` or `raise`.** Those unwind the stack,
> so `session_scope`'s `except` would roll back and its `finally` would close
> the connection: a tidy, deliberate shutdown. That is not a crash. A simulation
> that runs its cleanup handlers proves nothing about a process that never got
> the chance to run them. `os._exit()` skips `finally` blocks, `atexit`, and
> buffer flushing — which is what SIGKILL or a yanked power cable actually looks
> like.
>
> Same standard as `BREAK_OUTBOX_INSERT` in Phase 1: make the *real* mechanism
> fail, not your own control flow.
>
> (`logging.shutdown()` runs first, purely so the log line explaining the crash
> survives. Flushing the record of the crash is instrumentation; the transaction
> is still abandoned exactly as violently as intended.)

### 5c. The row is still unpublished — but the message is already gone

```bash
psql_ -c "select o.item, ob.published, ob.published_at
          from outbox ob join orders o on o.id = ob.order_id
          where o.customer_id = 'cust-crash';"
```

```
    item    | published | published_at
------------+-----------+--------------
 Scenario A | f         |
```

```bash
$LS sqs get-queue-attributes --queue-url "$QURL" \
   --attribute-names ApproximateNumberOfMessages --query 'Attributes'
```

```json
{ "ApproximateNumberOfMessages": "1" }
```

**This is the inconsistent state, captured.** Postgres says "not sent." SNS
already sent it. Neither is lying — the process died in the gap between them,
and no amount of care in the relay can close that gap, because there is no
transaction spanning a database and a message broker.

### 5d. Restart — the duplicate appears

```bash
docker compose up -d relay      # flag off
sleep 6
psql_ -c "select o.item, ob.published from outbox ob
          join orders o on o.id = ob.order_id where o.customer_id = 'cust-crash';"
```

Now `published = t`. And in the queue:

```bash
$LS sqs receive-message --queue-url "$QURL" --max-number-of-messages 10 \
   --visibility-timeout 0 --output json | python3 -c '
import json,sys
msgs = json.load(sys.stdin).get("Messages", [])
print(f"messages in queue: {len(msgs)}")
for i, m in enumerate(msgs, 1):
    b = json.loads(m["Body"])
    print(f"  copy {i}: SQS MessageId {m[\"MessageId\"]}  event_id {b[\"event_id\"]}")
'
```

```
messages in queue: 2
  copy 1: SQS MessageId 2d7f3efb-…  event_id e1ed8a8b-d288-4ca5-b58e-5484c2320c66
  copy 2: SQS MessageId bf3cd0db-…  event_id e1ed8a8b-d288-4ca5-b58e-5484c2320c66
```

✅ **Pass.** Two messages. **Two different MessageIds. One `event_id`.**

That line is the entire lesson of Phase 2, and it is why Phases 4–6 exist:

- **The broker's own id is useless for deduplication.** SNS mints a fresh
  MessageId on every publish, so the two copies of one logical event look like
  two unrelated events to anyone keying on it. A consumer that dedupes on
  MessageId dedupes nothing and double-bills the customer.
- **Your id survives.** `event_id` is the outbox row's primary key, and it does
  not change no matter how many times the row is republished. `order_id` is
  stable for the same reason. **A dedup key must come from your domain, not from
  the transport.**
- **The event was duplicated, never lost.** Compare the alternative ordering:
  had the relay marked the row published *before* publishing, this same crash
  would have produced a row claiming to be sent, with nothing on SNS, and no
  retry would ever fire. Silent, permanent loss — discovered weeks later when a
  customer was not billed.

Duplicates are loud, visible, and fixable with one check at the receiving end.
Losses are silent and unrecoverable. **The outbox pattern does not eliminate the
dual-write problem; it converts an unrecoverable failure into a recoverable
one.** That trade is the whole design, and it only pays off because the
consumers you build next are idempotent.

```bash
$LS sns unsubscribe --subscription-arn "$SUB"; $LS sqs delete-queue --queue-url "$QURL"
```

---

## Step 6 — Graceful shutdown

A crash is one way to stop. This is the other, and they must look different.

```bash
time docker compose stop relay
docker inspect --format '{{.State.ExitCode}}' $(docker compose ps -aq relay)
docker compose logs relay --tail 2
```

```
real    0m1.3s
exit code: 0
received SIGTERM — finishing the current row, then stopping
relay stopped cleanly
```

Three things being checked at once:

- **Exit 0**, versus 17 for the injected crash. A demo where both look identical
  proves nothing.
- **~1.3s, not 5s and not 10s.** The loop sleeps on `_shutdown.wait(2)`, not
  `time.sleep(2)`, so the signal handler's `set()` wakes it immediately. With
  `time.sleep` the signal is *handled* at once but not *acted on* until the
  interval expires — and Docker's grace period is finite. Silent SIGKILLs during
  ordinary deploys are exactly how a "reliable" relay develops mystery
  duplicates.
- **The handler does not exit.** It asks the loop to stop after the current row,
  so the outbox row and SNS end up agreeing. Killing the process the instant a
  signal arrives would reproduce Scenario A every time you press Ctrl-C.

```bash
docker compose start relay
```

---

## Step 7 — The partial index really is used

Phase 1 confirmed the index *exists*. This confirms Postgres actually *uses* it
— a different question, with a genuinely surprising answer.

Load 50k published rows and hide 20 unpublished ones in them (the relay's real
shape: a tiny to-do list inside a huge archive):

```bash
psql_ <<'SQL'
INSERT INTO orders (id, customer_id, item, amount, created_at)
SELECT gen_random_uuid(), 'bulk-'||g, 'item '||g, 10.00, now() - (g||' seconds')::interval
FROM generate_series(1, 50000) g;

INSERT INTO outbox (id, order_id, event_type, payload, published, created_at, published_at)
SELECT gen_random_uuid(), o.id, 'OrderCreated', '{}'::jsonb, true, o.created_at, now()
FROM orders o WHERE o.customer_id LIKE 'bulk-%';

UPDATE outbox SET published = false, published_at = NULL
WHERE id IN (SELECT id FROM outbox ORDER BY random() LIMIT 20);

ANALYZE outbox;
SQL
```

Now compare the two ways of spelling the same condition:

```bash
# A — what relay/service.py emits
psql_ -c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
          SELECT id FROM outbox WHERE published = false ORDER BY created_at LIMIT 10;"

# B — what SQLAlchemy's .is_(False) emits
psql_ -c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
          SELECT id FROM outbox WHERE published IS false ORDER BY created_at LIMIT 10;"
```

```
A)  Limit
      ->  Index Scan using ix_outbox_unpublished on outbox
    Execution Time: 0.134 ms

B)  Limit
      ->  Sort
            Sort Key: created_at
            ->  Seq Scan on outbox
                  Filter: (published IS FALSE)
                  Rows Removed by Filter: 49983
    Execution Time: 18.054 ms
```

**135× slower, from a spelling difference.** `published = false` and
`published IS false` are semantically identical for a `NOT NULL` boolean — but a
partial index is only usable when Postgres can *prove* the query's `WHERE`
implies the index predicate, and its prover is deliberately limited rather than
a general theorem prover. It matches `= false` against `WHERE published = false`
and does not bridge `IS false`.

Notice what plan B is actually doing: reading all 50,003 rows, discarding
49,983, then **sorting** the survivors — because without the index there is no
pre-ordered path to `ORDER BY created_at`. That is the cost the relay would pay
every 2 seconds, forever, growing with your history.

This is why `relay/service.py` and `order/routes.py` both use
`== False  # noqa: E712` rather than the more idiomatic `.is_(False)`. The
linter suggestion is right about Python style and wrong about this query.

> **The transferable lesson: an index that exists is not an index that is used.**
> `EXPLAIN` is the only way to know. Reach for it whenever a query touches an
> index you deliberately designed, and especially whenever you write a partial
> index — its whole value depends on a proof you cannot see from the schema.

Clean up:

```bash
psql_ -c "DELETE FROM outbox WHERE order_id IN
            (SELECT id FROM orders WHERE customer_id LIKE 'bulk-%');
          DELETE FROM orders WHERE customer_id LIKE 'bulk-%';
          ANALYZE outbox;"
```

> **That delete takes minutes, and the reason is worth knowing.**
> `outbox.order_id` is a foreign key with **no index on it**. To delete a parent
> row Postgres must prove no child references it — and with no index that is a
> sequential scan of `outbox` *per deleted order*. 50,000 deletes × a 50,000-row
> scan is quadratic.
>
> **Postgres indexes the primary key side of a foreign key automatically; it
> never indexes the referencing side.** Nearly every slow `DELETE`/`UPDATE` on a
> parent table in a production Postgres traces back to this. We have not added
> the index because this project never deletes orders — but recognising the
> symptom is worth more than the index would be.

---

## Step 8 — Two relays, no double-publishing

Not required by the STOP condition. Run it anyway: it is the only way to see
what `SELECT … FOR UPDATE SKIP LOCKED` actually buys, and it takes 30 seconds.

Build a backlog with the relay stopped, then start two instances at once:

```bash
docker compose stop relay
for n in $(seq 1 40); do
  curl -s -o /dev/null -X POST localhost:8000/orders -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"race-$n\",\"item\":\"Race $n\",\"amount\":\"1.00\"}"
done
psql_ -tAc "select count(*) from outbox where published = false;"    # 40

docker compose up -d --scale relay=2 relay
sleep 10
```

```bash
# work split between the two instances
docker compose logs relay | grep 'published outbox row' | grep -oE '^relay-[0-9]+' | sort | uniq -c

# distinct rows published vs total publish lines — these MUST be equal
docker compose logs relay | grep -oE 'published outbox row [0-9a-f-]+' | sort -u | wc -l
docker compose logs relay | grep -c 'published outbox row'
```

Measured:

```
     21 relay-1
     20 relay-2

distinct rows published : 41
total publish lines     : 41      <-- equal, so nothing was published twice
```

Both instances did real work, the backlog drained roughly twice as fast, and
**no row was published by both.** Not luck — `SKIP LOCKED` is what makes it
deterministic:

- **`FOR UPDATE`** locks each row the moment a relay claims it, and the lock is
  held *across the SNS publish*. That is the deliberate trade in
  `relay_one()` — a transaction stays open across a network call, and in
  exchange the claim is airtight.
- **`SKIP LOCKED`** makes the second relay step over a locked row and take the
  next one instead of blocking on it. Without it, plain `FOR UPDATE` would
  queue both relays on the *same* row and a second instance would be pure
  overhead — you would have doubled the process count and not the throughput.
- **Re-checking `published = false` inside the lock** closes the last gap: the
  batch was read without locks, so a row may have been published by the other
  instance between that read and this lock. `one_or_none()` returns `None`, the
  relay shrugs, and moves on.

Note what would happen *without* the lock: both relays read the same backlog,
both publish, both mark. Every row duplicated — a duplicate manufactured by our
own design rather than by a crash. At-least-once tolerates that, but there is no
reason to pay for it.

```bash
docker compose up -d --scale relay=1 relay
```

---

## Phase 2 checklist

- [ ] Relay starts, creates/resolves the `order-events` topic, then logs nothing while idle
- [ ] `POST /orders` still returns `201` immediately, with the relay stopped
- [ ] Within one poll interval the row flips to `published = true` with a `published_at`
- [ ] `docker compose logs localstack` shows `AWS sns.Publish => 200`
- [ ] Stopping the relay does not affect order creation; the backlog drains on restart
- [ ] The received message carries `event_id`, `order_id`, `occurred_at` and a full `payload`
- [ ] `amount` is still a **string** after the full round trip
- [ ] `CRASH_AFTER_PUBLISH=1` → exit code **17**, row still `published = false`, message already on SNS
- [ ] Restart republishes → **2 messages, 2 MessageIds, 1 `event_id`**
- [ ] `docker compose stop relay` → exit **0**, in ~1s, logging a clean shutdown
- [ ] `EXPLAIN` shows `Index Scan using ix_outbox_unpublished` for `= false`
- [ ] `--scale relay=2` splits the backlog and publishes **no row twice**

---

## Troubleshooting

**Relay logs `could not reach SNS ... retrying in the loop`** — LocalStack is
not up yet, or `AWS_ENDPOINT_URL` points at `localhost` instead of
`localstack`. Inside a container, `localhost` is *that container*. Startup is
deliberately non-fatal: the loop retries, because a relay that refuses to boot
when a dependency blinks is worse than one that waits.

**Rows never get published, relay logs nothing** — check the relay is actually
running (`docker compose ps relay`). It exits only on a signal or
`CRASH_AFTER_PUBLISH`; there is **no `restart:` policy anywhere in this project**
on purpose, because a self-healing container would resurrect itself mid-Scenario
and destroy the failure you are trying to demonstrate.

**`relay.service: publish failed ... abandoning batch`** — SNS is unreachable.
The rows stay unpublished, which is the entire point; fix LocalStack and they
drain on the next poll. The batch is abandoned rather than continued because
each failure burns the full boto3 retry budget (~35s), so pressing on would turn
a 2-second poll into a minutes-long stall.

**Topic ARN went stale** — LocalStack restarted and forgot every resource
(persistence is a paid feature). The relay clears its cached ARN on a `NotFound`
and recreates the topic on the next publish; no action needed.

**Two relays double-publishing** — they shouldn't; see Step 8.
