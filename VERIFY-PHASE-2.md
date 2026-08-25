# Verifying Phase 2 — Outbox Relay (Poll + Publish to SNS)

Commands to prove the Phase 2 STOP condition on demand:

> **The relay picks up a row, you can see the SNS publish succeed in
> LocalStack's logs, and the outbox row is correctly marked published.**

Plus a full run of **Scenario A** — kill the relay between posting the letter
and ticking it off. Not required until Phase 6, but it is the most important
proof in the project and the code supports it now.

**What Phase 2 is NOT.** No SQS queues, no consumers. One topic, one publisher.
Step 4 makes a throwaway queue purely so you can read a message; Phase 3 builds
the real thing.

**The model.** Phase 1 built the salesperson who records a sale and drops a
letter in the out-tray. Phase 2 built the **mail clerk** who checks that tray
every 2 seconds, carries letters to the post office (SNS), and ticks them off.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a      # .env does NOT auto-export into your shell
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

# awslocal is LocalStack's AWS CLI wrapper — it pre-points --endpoint-url at
# 4566 so you never type it.
LS="docker compose exec -T localstack awslocal"

psql_() { docker compose exec -T postgres psql -U $PG_USER -d $PG_DB "$@"; }
```

---

## Step 1 — Clean slate and start

```bash
docker compose down -v            # -v wipes the volumes too
docker compose up -d --build order relay
until_ready
docker compose ps
docker compose logs relay
```

```
relay starting: poll every 2s, batch 10, topic 'order-events' at http://localstack:4566
SNS topic 'order-events' resolved to arn:aws:sns:us-east-1:000000000000:order-events
```

**The topic was created, not found.** `create_topic` is *idempotent* — doing it
twice has the same effect as once. So the relay sets up its own topic on every
start. That is necessary, not lazy: LocalStack's free tier **forgets every topic
on restart**, so any setup you type by hand is setup you will eventually forget
to type.

**Then silence.** Check again in 30 seconds — nothing. 86,400 seconds a day ÷ 2
= **43,200 polls**. One line per empty poll buries the five that matter.

---

## Step 2 — The happy path

```bash
curl -s -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-p2","item":"Phase 2 test","amount":"250.00"}' | jq
sleep 4
docker compose logs relay --tail 3
```

The `201` came back immediately — the Order Service wrote two rows to its own
database and stopped; it does not know the relay exists.

```
relay.service: published outbox row 2276b803-… (OrderCreated, order 94c33076-…) -> SNS MessageId 0a2fd029-…
relay: poll: published 1 of 1 pending row(s)
```

### 2a. The row was ticked off

```bash
psql_ -c "select event_type, published, published_at from outbox;"
```

```
  event_type  | published |        published_at
--------------+-----------+---------------------------
 OrderCreated | t         | 2026-08-22 10:18:29.4+00
```

### 2b. LocalStack agrees — **this is the STOP condition**

```bash
docker compose logs localstack | grep -E 'sns\.(CreateTopic|Publish)'
```

```
AWS sns.CreateTopic => 200
AWS sns.Publish => 200
```

The relay's MessageId is *what boto3 was told*. This is *what the broker
recorded*. Checking the far side is a habit worth building — an SDK returning
successfully is a claim, not a fact.

---

## Step 3 — The relay is a courier, not a source of truth

```bash
docker compose stop relay

for n in 1 2 3; do
  curl -s -o /dev/null -w "order $n -> %{http_code}\n" \
    -X POST localhost:8000/orders -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"backlog-$n\",\"item\":\"Queued $n\",\"amount\":\"$n.00\"}"
done

psql_ -tAc "select count(*) from outbox where published = false;"   # 3
```

**Three `201`s with the publisher dead.** The shop kept taking money with the
courier off sick. Compare the naive design where the endpoint calls SNS itself:
there, a slow SNS makes checkout slow and an SNS outage makes it *fail*.

The events are not lost — they are **pending**, durable in Postgres, written in
the same transaction as the orders.

```bash
docker compose start relay
sleep 4
psql_ -tAc "select count(*) from outbox where published = false;"   # 0
```

The backlog drains by itself. Nothing was replayed by hand because nothing was
lost.

---

## Step 4 — See the actual message (throwaway queue)

> Phase 3 machinery borrowed as a debugging lens; deleted at the end of the step.

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
    "event_id": "8fa5d528-d24e-4b70-a4cb-17d4ae6fddef",
    "event_type": "OrderCreated",
    "order_id": "561524bb-7ca8-47ed-a6bf-2acf5decdcc4",
    "occurred_at": "2026-08-22T10:20:16.505148+00:00",
    "payload": {
        "item": "See the real message",
        "amount": "99.95",
        "order_id": "561524bb-7ca8-47ed-a6bf-2acf5decdcc4",
        "created_at": "2026-08-22T10:20:16.500579+00:00",
        "customer_id": "cust-peek"
    }
}
```

- **`amount` is still the string `"99.95"`** after Decimal → JSONB → Python →
  JSON → SNS → SQS. As a JSON number it would eventually return as
  `99.95000000000001`. Money travels as text.
- **`event_id` is the outbox row's id, not the SNS MessageId** — Step 5 shows
  why that is the difference between idempotency working and quietly not.
- **`occurred_at` is creation time, not send time.** The gap is relay lag; given
  only send time you cannot tell a fresh event from a backlog replay.

> **`RawMessageDelivery=true` is a real fork in the road.** With it, the body
> arrives exactly as written. Without it SNS wraps it —
> `{"Type":"Notification","Message":"…"}` — and your JSON arrives as *a string
> inside a field*, needing two parses. Phase 3 must choose deliberately; it
> changes the code in all three consumers.

```bash
$LS sns unsubscribe --subscription-arn "$SUB"; $LS sqs delete-queue --queue-url "$QURL"
```

---

## Step 5 — SCENARIO A: crash between posting and ticking off

**The most important proof in this project.** Keep a throwaway queue subscribed
so you can *count* the duplicate.

```bash
TOPIC=$($LS sns create-topic --name order-events --query TopicArn --output text)
QURL=$($LS sqs create-queue --queue-name scenarioA --query QueueUrl --output text)
QARN=$($LS sqs get-queue-attributes --queue-url "$QURL" \
        --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
SUB=$($LS sns subscribe --topic-arn "$TOPIC" --protocol sqs \
        --notification-endpoint "$QARN" \
        --attributes RawMessageDelivery=true --query SubscriptionArn --output text)
```

### 5a. Arm and fire

```bash
CRASH_AFTER_PUBLISH=1 docker compose up -d relay
sleep 4
curl -s -o /dev/null -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-crash","item":"Scenario A","amount":"777.00"}'
sleep 5
```

> Use `up -d`, not `restart`. `restart` reuses the container with the
> environment it was *created* with, so the flag would do nothing. `up -d`
> notices the config changed and recreates it. Confirm with
> `docker compose exec relay printenv CRASH_AFTER_PUBLISH`.

### 5b. The relay is dead

```bash
docker inspect --format '{{.State.ExitCode}}' $(docker compose ps -aq relay)
docker compose logs relay --tail 2
```

```
17

CRITICAL relay.service: CRASH_AFTER_PUBLISH — event 8fa5d528-… is already on SNS
but its outbox row is still unpublished. Restart the relay: it will send the
event again, and every consumer must ignore the duplicate.
```

Exit **17** — not 0 (success), not 1 (the generic failure everything uses. A
distinctive code lets a test assert *it died where we aimed it*.

> **Why `os._exit()` and not `sys.exit()` or `raise`.** Those unwind the stack,
> so `session_scope`'s `finally` runs and the transaction is politely rolled
> back — a **shutdown**, not a **crash**. It is the difference between resigning
> with notice and being hit by a bus: the end state can look similar, but only
> one tells you how the company copes. `os._exit()` is the bus.
>
> Same standard as Phase 1's `BREAK_OUTBOX_INSERT`, where we let *Postgres*
> reject the row rather than raising in Python: **make the real mechanism fail,
> not your own control flow.**
>
> (`logging.shutdown()` runs first only so the explanatory line survives —
> flushing the *record* of a crash is instrumentation, like a black box
> surviving the plane.)

### 5c. The inconsistent state, captured

```bash
psql_ -c "select o.item, ob.published, ob.published_at
          from outbox ob join orders o on o.id = ob.order_id
          where o.customer_id = 'cust-crash';"

$LS sqs get-queue-attributes --queue-url "$QURL" \
   --attribute-names ApproximateNumberOfMessages --query 'Attributes'
```

```
    item    | published | published_at
------------+-----------+--------------
 Scenario A | f         |

{ "ApproximateNumberOfMessages": "1" }
```

**Read those together.** Postgres says the letter was never posted; SNS already
delivered it. Neither is lying — the process died in the gap. And no care in the
relay can close that gap, because **no transaction spans a database and a
message broker**. A hard limit, not a bug.

### 5d. Restart — the duplicate appears

```bash
docker compose up -d relay      # flag off
sleep 6

cat > /tmp/show.py <<'PY'
import json, sys
msgs = json.load(sys.stdin).get("Messages", [])
print("messages in queue:", len(msgs), "\n")
for i, m in enumerate(msgs, 1):
    b = json.loads(m["Body"])
    print("  copy %d: SQS MessageId %s" % (i, m["MessageId"]))
    print("           event_id     %s" % b["event_id"])
print()
print("distinct SQS MessageIds :", len({m["MessageId"] for m in msgs}))
print("distinct event_ids      :",
      len({json.loads(m["Body"])["event_id"] for m in msgs}), "  <-- ONE logical event")
PY

$LS sqs receive-message --queue-url "$QURL" --max-number-of-messages 10 \
   --visibility-timeout 0 --output json | python3 /tmp/show.py
```

```
messages in queue: 2

  copy 1: SQS MessageId cc29fdd2-5e0d-4b94-8612-94e9cb7ce445
           event_id     8fa5d528-d24e-4b70-a4cb-17d4ae6fddef
  copy 2: SQS MessageId 8286be7c-fed9-4387-a138-317e99675a65
           event_id     8fa5d528-d24e-4b70-a4cb-17d4ae6fddef

distinct SQS MessageIds : 2
distinct event_ids      : 1   <-- ONE logical event
```

✅ **Pass.** Two messages. **Two MessageIds. One `event_id`.** Three lessons:

**The broker's id is useless for deduplication.** SNS mints a fresh MessageId on
every publish, so two copies of one event look unrelated. Worse, it fails
*asymmetrically*:

| Duplicate source | Same MessageId? | Dedup works? |
| --- | --- | --- |
| SQS redelivery (slow consumer, visibility timeout expired) | yes | ✅ |
| Relay republish after a crash (this test) | no | ❌ silently |

So it passes exactly the tests you would naturally write and fails only during a
real crash, in production, at 3am.

**Your own id survives.** `event_id` is the outbox row's primary key and never
changes. So does `order_id`.

> **A dedup key must come from your domain, never from the transport.** The
> transport mints an id per *delivery*; you need one per *real-world fact*.

**The event was duplicated, never lost.** Reverse the order — tick off, then
publish — and this same crash leaves a row claiming to be sent, nothing on SNS,
and **no retry ever firing**. You would only find out from *outside* the system:
a reconciliation job, an uncharged customer, accounts that don't balance. That
is what silent loss means — not hard to find, but requiring a second source of
truth you built in advance.

**The outbox pattern converts an unrecoverable failure into a recoverable one.**
That trade only pays off because the consumers you build next are idempotent.

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
real    0m1.2s
0
received SIGTERM — finishing the current row, then stopping
relay stopped cleanly
```

**Exit 0**, versus 17 for the crash. A demo where both look identical proves
nothing.

**~1.2s, not 5 or 10.** The loop sleeps on `_shutdown.wait(2)`, not
`time.sleep(2)`. With a 30s interval and Docker's 10s grace:

```
t=0s   poll ends, time.sleep(30)
t=1s   SIGTERM arrives; handler sets the flag ✓
       ...but sleep goes back to sleeping for 29 more seconds
t=11s  Docker gives up -> SIGKILL. Dead mid-nap.
```

The flag was set at t=1s; the loop never got to *look* at it. And SIGKILL does
not pick a convenient moment — land it between publish and tick-off and that is
**Scenario A by accident, on every deploy**. That is how a production mystery is
born.

**The handler does not exit**, it asks the loop to stop after the current row —
otherwise Ctrl-C would reproduce Scenario A every time.

```bash
docker compose start relay
```

---

## Step 7 — The partial index really is used

Phase 1 confirmed the index **exists**. Whether Postgres **uses** it is a
different question with a surprising answer.

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

20 unpublished needles in a 50k haystack — the relay's real shape.

```bash
# A — what relay/service.py emits
psql_ -c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
          SELECT id FROM outbox WHERE published = false ORDER BY created_at LIMIT 10;"

# B — what .is_(False) emits
psql_ -c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
          SELECT id FROM outbox WHERE published IS false ORDER BY created_at LIMIT 10;"
```

```
A)  Limit -> Index Scan using ix_outbox_unpublished on outbox
    Execution Time: 0.134 ms

B)  Limit -> Sort (Sort Key: created_at)
              -> Seq Scan on outbox
                   Filter: (published IS FALSE)
                   Rows Removed by Filter: 49983
    Execution Time: 18.054 ms
```

**135× slower from a spelling difference.** A partial index holds only *some*
rows, so before using it Postgres must be certain your query wants only rows
inside it. It checks by comparing the conditions as **patterns**, not by
reasoning about meaning:

```
index:      published = false
you wrote:  published = false     -> match, use the index
you wrote:  published IS false    -> different, don't risk it
```

A pattern-matcher, not a logician. Note what plan B does: reads all 50,003 rows,
discards 49,983, then **sorts** — because without the index there is no
pre-ordered path to `ORDER BY created_at`. That is the cost every 2 seconds,
forever, growing with your history.

This is why `relay/service.py` and `order/routes.py` both use
`== False  # noqa: E712`. The linter is right about Python style and wrong about
this query.

> **An index that exists is not an index that is used.** `EXPLAIN` is the only
> way to know — always for a partial index, whose whole value rests on a proof
> invisible in the schema.

```bash
psql_ -c "DELETE FROM outbox WHERE order_id IN
            (SELECT id FROM orders WHERE customer_id LIKE 'bulk-%');
          DELETE FROM orders WHERE customer_id LIKE 'bulk-%';
          ANALYZE outbox;"
```

> **That delete takes minutes, and the reason is worth knowing.**
> `outbox.order_id` is a foreign key with **no index on it**. To delete a parent
> row Postgres must prove no child references it — with no index, a full scan of
> `outbox` per deleted order. 50,000 × 50,000 is quadratic.
>
> **Postgres indexes the primary-key side of a foreign key automatically, never
> the referencing side.** Nearly every mysteriously slow `DELETE` on a parent
> table traces back to this. We have not added the index (this project never
> deletes orders); recognising the symptom is worth more.

---

## Step 8 — Two relays, no double-publishing

Not required by the STOP condition. Do it anyway — 30 seconds, and it is the
only way to *see* what `SKIP LOCKED` buys.

```bash
docker compose stop relay
for n in $(seq 1 30); do
  curl -s -o /dev/null -X POST localhost:8000/orders -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"race-$n\",\"item\":\"Race $n\",\"amount\":\"1.00\"}"
done
psql_ -tAc "select count(*) from outbox where published = false;"    # 30

docker compose up -d --scale relay=2 relay
sleep 10

docker compose logs relay | grep 'published outbox row' | grep -oE '^relay-[0-9]+' | sort | uniq -c
docker compose logs relay | grep -oE 'published outbox row [0-9a-f-]+' | sort -u | wc -l
docker compose logs relay | grep -c 'published outbox row'
```

```
     11 relay-1
     20 relay-2

distinct rows published : 31
total publish lines     : 31      <-- equal, so nothing was published twice
```

Both instances did real work and **no row was published by both**. Three
mechanisms make that deterministic:

**`FOR UPDATE`** reserves a row the moment a relay claims it — a hand on the
letter before picking it up. Crucially the reservation is held *across the SNS
call*, because **a lock lasts exactly as long as its transaction** and there is
no way to release one early.

**`SKIP LOCKED`** makes the second relay step over a reserved row instead of
waiting. Plain `FOR UPDATE` would queue both relays on the *same* row — double
the processes, identical throughput.

> "Skipped" is **not** "dropped". The row stays unpublished: either the holder
> finishes in milliseconds, or it dies and the lock dies with its connection,
> and the row is picked up next poll.

**Re-checking `published = false` inside the lock** closes the last gap. The
batch was read *without* locks, so the other relay may have published and
released in between. Asking for "this row, *if still unpublished*" means the
loser gets nothing and moves on.

Without any lock, both relays read the same list, both publish, both tick off —
every row duplicated by our own design rather than by a crash.

```bash
docker compose up -d --scale relay=1 relay
```

---

## Phase 2 checklist

- [ ] Relay starts, resolves/creates the topic, then logs nothing while idle
- [ ] `POST /orders` still returns `201` immediately with the relay stopped
- [ ] Within one poll the row flips to `published = true` with a `published_at`
- [ ] `docker compose logs localstack` shows `AWS sns.Publish => 200`
- [ ] Stopping the relay doesn't affect orders; the backlog drains on restart
- [ ] The message carries `event_id`, `order_id`, `occurred_at` and full `payload`
- [ ] `amount` is still a **string** after the round trip
- [ ] `CRASH_AFTER_PUBLISH=1` → exit **17**, row `published = false`, message already on SNS
- [ ] Restart republishes → **2 messages, 2 MessageIds, 1 `event_id`**
- [ ] `docker compose stop relay` → exit **0** in about a second
- [ ] `EXPLAIN` shows `Index Scan using ix_outbox_unpublished` for `= false`
- [ ] `--scale relay=2` splits the backlog and publishes **no row twice**

---

## Troubleshooting

**`could not reach SNS ... retrying in the loop`** — LocalStack isn't up, or
`AWS_ENDPOINT_URL` says `localhost` instead of `localstack` (inside a container,
localhost is *that container*). Startup failure is deliberately non-fatal: a
relay that refuses to boot when a dependency blinks is worse than one that waits.

**Rows never publish and the relay logs nothing** — check `docker compose ps
relay`. It exits only on a signal or `CRASH_AFTER_PUBLISH`; there is **no
`restart:` policy anywhere** on purpose, since a self-healing container would
destroy the failure you are trying to observe.

**`publish failed ... abandoning batch`** — SNS unreachable. Rows stay
unpublished, which is the point; they drain on the next poll. The batch is
abandoned because each failure burns the full ~35s boto3 retry budget.

> **Known flaw, left in on purpose.** That holds when the whole world is broken,
> not when *one row* is. A payload SNS always rejects is the oldest row, so
> oldest-first ordering puts it first in **every** batch forever and `break`
> abandons everything behind it — the outbox stops draining permanently while
> the relay looks healthy. This is **head-of-line blocking** (the row is a
> *poison message*). The fix is classifying errors and parking permanent
> failures after N attempts — a dead-letter queue for the outbox table, which is
> Phase 6's natural home. See the comment in `relay/service.py`.

**Topic ARN went stale** — LocalStack restarted and forgot everything
(persistence is paid). The relay clears its cached address on `NotFound` and
recreates the topic next publish. No action needed.

**Two relays double-publishing** — they shouldn't; see Step 8.
