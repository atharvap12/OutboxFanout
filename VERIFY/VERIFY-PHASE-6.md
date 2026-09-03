# Verifying Phase 6 — Scenarios A–D by hand

Commands to demonstrate the Phase 6 STOP condition manually:

> **All 4 scenarios reproducible on demand.**

Everything below was run against a clean stack and the output is recorded
verbatim. `pytest` runs the same four scenarios automatically; this file is for
driving them yourself — for a demo, a screen share, or an interview.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a

PSQL="docker exec outboxFanout-postgres psql -U $PG_USER -d $PG_DB -At"
REDIS="docker exec outboxFanout-redis redis-cli"
LS="docker compose exec -T localstack awslocal"

qurl()   { $LS sqs get-queue-url --queue-name "$1" --output text | tr -d '\r'; }
qdepth() { $LS sqs get-queue-attributes --queue-url "$(qurl $1)" \
             --attribute-names ApproximateNumberOfMessages \
             --query 'Attributes.ApproximateNumberOfMessages' --output text | tr -d '\r'; }

# Create an order, echo its id.
neworder() {
  curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
    -d "$1" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])'
}

# The three counters, for one order.
effects() {
  $PSQL -c "SELECT 'billing_records='||count(*) FROM billing_records WHERE order_id='$1'
            UNION ALL SELECT 'shipments='||count(*) FROM shipments WHERE order_id='$1';"
  echo "redis keys=$($REDIS KEYS "notify:processed:$1" | wc -l)"
}
```

Start from a clean stack so the counts are unambiguous:

```bash
docker compose down -v
docker compose up -d --build
docker compose ps -a
```

```
localstack     Up (healthy)
postgres       Up (healthy)
redis          Up (healthy)
order          Up
relay          Up
billing        Up
shipping       Up
notifications  Up
bootstrap      Exited (0)
```

> ⚠️ Phase 6 **added columns** to `outbox` (`attempts`, `last_error`,
> `failed_at`). `create_all()` creates missing tables but never ALTERs existing
> ones, so an older stack needs `down -v` — not just `down` — or the relay will
> fail on a column that isn't there.

---

## Scenario A — Relay crashes mid-loop

*"This is the single most important proof in the whole project."*

The relay publishes to SNS and **then** marks the row published. No transaction
spans those two steps, so a crash in between leaves a row that **was** published
but still looks unsent — and on restart it is published a second time.

### A1. Arm the relay to die in the gap

```bash
CRASH_AFTER_PUBLISH=1 docker compose up -d relay
```

### A2. Create an order

```bash
OID=$(neworder '{"customer_id":"bob","item":"Lamp","amount":"42.00"}')
echo "$OID"
```

### A3. Watch it die exactly where it was aimed

```bash
docker inspect -f '{{.State.ExitCode}}' outboxfanout-relay-1
docker compose logs relay --no-log-prefix | grep CRASH_AFTER_PUBLISH
$PSQL -c "SELECT published FROM outbox WHERE order_id='$OID';"
```

```
17
CRITICAL [relay] relay.service: CRASH_AFTER_PUBLISH — event 2aebd8ba-… is on SNS
  but the outbox row is still unpublished. Restart the relay: it will publish it
  again, and every consumer must no-op on the duplicate.
f
```

Exit code **17**, not 1 — a dedicated code proves the process died at the
injected crash rather than falling over for some unrelated reason. The row still
reads `published = f`, which is exactly what makes the republish happen.

### A4. The message was nevertheless already delivered

```bash
effects "$OID"
```

```
billing_records=1
shipments=1
redis keys=1
```

This is the inconsistency the outbox pattern deliberately creates: the event is
downstream, but the database does not know it. Under mark-then-publish the same
crash would have produced the opposite and far worse state — a row claiming
delivery that never happened, with nothing inside the system able to detect it.

### A5. Restart normally — the row is published again

```bash
CRASH_AFTER_PUBLISH=0 docker compose up -d relay
sleep 8
$PSQL -c "SELECT published FROM outbox WHERE order_id='$OID';"
```

```
t
```

### A6. All three consumers no-op on the duplicate

```bash
docker compose logs billing shipping notifications --no-log-prefix | grep "$OID"
```

```
11:59:47  💳 BILLED     order 9e599687-… — bob 42.00 (event 2aebd8ba-…)
11:59:47  📦 SHIPPED    order 9e599687-… — 'Lamp' tracking TRK-15CB57030944 (event 2aebd8ba-…)
11:59:47  📧 EMAIL SENT to bob — order 9e599687-… confirmed (Lamp, 42.00)
12:01:08  🔁 DUPLICATE ignored — already billed    (event 2aebd8ba-…)
12:01:08  🔁 DUPLICATE ignored — already shipped   (event 2aebd8ba-…)
12:01:08  🔁 DUPLICATE ignored — already notified by event 2aebd8ba-… (this delivery: 2aebd8ba-…)
```

```bash
effects "$OID"
```

```
billing_records=1
shipments=1
redis keys=1
```

✅ **Scenario A passes.** The duplicate is *expected* — it is the price of
choosing at-least-once. What is proved is that it is **harmless**, caught by two
different mechanisms (a Postgres UNIQUE constraint twice, a Redis `SET NX` once).

Note the **`event_id` is identical in all six lines**. That only works because
the dedup key is the outbox row's own id, which does not change however many
times the row is republished — unlike an SNS MessageId (fresh per publish) or an
SQS MessageId (fresh per queue, per copy).

---

## Scenario B — Forced duplicate delivery

Scenario A produced one duplicate as a side effect of a crash. This attacks the
idempotency checks directly: deliver the same event repeatedly on purpose.

### B1. One order, processed normally

```bash
OID=$(neworder '{"customer_id":"alice","item":"Standing desk","amount":"899.00"}')
sleep 6
effects "$OID"

$REDIS GET notify:processed:$OID          # which event claimed it
$PSQL -c "SELECT id FROM outbox WHERE order_id='$OID';"
```

```
billing_records=1
shipments=1
redis keys=1

9e65553d-eea9-4a5a-9533-a8345b7ceb57      <- Redis key value
9e65553d-eea9-4a5a-9533-a8345b7ceb57      <- outbox row id  (the same)
```

The Redis key stores the **claiming event_id**, and it is the outbox row id.
That is what lets the duplicate log line say *who got there first*.

### B2. Force delivery #2 and #3

Resetting `published` puts the row back in the relay's in-tray — precisely what
a Scenario A crash leaves behind, so this is the honest way to duplicate:

```bash
for n in 2 3; do
  $PSQL -c "UPDATE outbox SET published=false, published_at=NULL WHERE order_id='$OID';"
  sleep 8
  effects "$OID"
done
```

```
--- delivery #2 ---        --- delivery #3 ---
billing_records=1          billing_records=1
shipments=1                shipments=1
redis keys=1               redis keys=1
```

### B3. The log shows the duplicates being caught

```bash
docker compose logs billing shipping notifications --no-log-prefix | grep "$OID" | sort
```

```
12:02:32  💳 BILLED     order 24ef2697-… — alice 899.00 (event 9e65553d-…)
12:02:32  📦 SHIPPED    order 24ef2697-… — 'Standing desk' tracking TRK-E8D99C232AC8
12:02:32  📧 EMAIL SENT to alice — order 24ef2697-… confirmed (Standing desk, 899.00)
12:02:34  🔁 DUPLICATE ignored — already billed / shipped / notified
12:02:42  🔁 DUPLICATE ignored — already billed / shipped / notified
```

```bash
$REDIS GET notify:processed:$OID      # unchanged — the claim never changed hands
```

✅ **Scenario B passes.** Three deliveries, one side effect each — never two.

---

## Scenario C — One consumer down, others healthy

### C1. Stop Notifications, keep ordering

```bash
docker compose stop notifications
docker compose ps -a notifications --format '{{.Status}}'

for n in 1 2 3; do
  neworder "{\"customer_id\":\"carol-$n\",\"item\":\"Mouse pad\",\"amount\":\"9.0$n\"}"
done
sleep 8
```

```
Exited (0)
  f298ec72-95e1-4241-858d-639456e121e0
  853cc909-07a0-4e52-acbf-199e5287287a
  a98dcdf7-34c2-49b0-9a49-66f9a61991c5
```

### C2. Billing and Shipping neither know nor care

```bash
$PSQL -c "SELECT 'billing_records='||count(*) FROM billing_records
          UNION ALL SELECT 'shipments='||count(*) FROM shipments;"
echo "redis keys: $($REDIS KEYS 'notify:processed:*' | wc -l)"
echo "notify-queue depth: $(qdepth notify-queue)"
$PSQL -c "SELECT count(*) FROM outbox WHERE published=false;"
```

```
billing_records=5      <- 2 from earlier scenarios + the 3 new ones
shipments=5
redis keys: 2          <- still only the earlier two; none of the 3 new
notify-queue depth: 3  <- the backlog, waiting
0                      <- nothing stuck upstream
```

The three points that matter, in one snapshot:

- Billing and Shipping processed **immediately** — a stopped consumer is not an
  event to them, it is not even visible to them.
- The work is **waiting, not lost**: notify-queue is holding it.
- **Zero unpublished outbox rows** — a dead consumer applies no back-pressure to
  the producer. The relay published and moved on.

### C3. Restart — the backlog drains itself

```bash
docker compose start notifications
sleep 10
echo "redis keys: $($REDIS KEYS 'notify:processed:*' | wc -l)"
echo "notify-queue depth: $(qdepth notify-queue)"
docker compose logs notifications --no-log-prefix --since 60s | grep 'EMAIL SENT'
```

```
redis keys: 5
notify-queue depth: 0

12:04:48  📧 EMAIL SENT to carol-1 — order f298ec72-… confirmed (Mouse pad, 9.01)
12:04:48  📧 EMAIL SENT to carol-2 — order 853cc909-… confirmed (Mouse pad, 9.02)
12:04:48  📧 EMAIL SENT to carol-3 — order a98dcdf7-… confirmed (Mouse pad, 9.03)
```

✅ **Scenario C passes.** No message loss, no manual replay.

This works only because each queue holds its **own** durable copy with its own
delivery state and no shared cursor. It is the payoff for fanning out through
SNS rather than having the relay call three consumers itself.

---

## Scenario D — Poison message lands in the DLQ

### D1. Confirm the redrive policy is attached

```bash
$LS sqs get-queue-attributes --queue-url "$(qurl billing-queue)" \
   --attribute-names RedrivePolicy --query 'Attributes.RedrivePolicy' --output text
```

```
{"deadLetterTargetArn": "arn:aws:sqs:us-east-1:000000000000:billing-dlq", "maxReceiveCount": 5}
```

### D2. Speed up redelivery so the demo takes seconds

The queue's visibility timeout is 30s, so five deliveries would be 2.5 minutes
of waiting. Dropping it to 1s changes the **clock**, not the mechanism — the
redrive policy is still what does the work.

```bash
$LS sqs set-queue-attributes --queue-url "$(qurl billing-queue)" \
   --attributes VisibilityTimeout=1
qdepth billing-dlq        # 0
```

### D3. Push a malformed message straight onto the queue

```bash
$LS sqs send-message --queue-url "$(qurl billing-queue)" \
   --message-body '{not valid json at all' --query 'MessageId' --output text
```

```
98cbe511-0cac-4a79-8f46-9ebcc522383a
```

### D4. Watch it get retried, then moved aside

```bash
sleep 20
echo "billing-dlq depth  : $(qdepth billing-dlq)"
echo "billing-queue depth: $(qdepth billing-queue)"
echo "billing consumer   : $(docker compose ps billing --format '{{.Status}}')"
docker compose logs billing --no-log-prefix --since 120s | grep -c 'unparsable body'
```

```
billing-dlq depth  : 1
billing-queue depth: 0
billing consumer   : Up 8 minutes
5                       <- exactly maxReceiveCount attempts
```

**Exactly 5 rejections, then SQS gave up.** And the consumer is still running —
one bad message must never take down the service.

### D5. The message is in the DLQ, intact

```bash
$LS sqs receive-message --queue-url "$(qurl billing-dlq)" \
   --max-number-of-messages 1 --visibility-timeout 0 \
   --attribute-names ApproximateReceiveCount \
   --query 'Messages[0].[Body,Attributes.ApproximateReceiveCount]' --output text
```

```
{not valid json at all	6
```

Our exact body, preserved for inspection. The consumer **deliberately does not
delete** an unparsable message — deleting would destroy the evidence — so
without the redrive policy this would have redelivered every 30 seconds until
the 4-day retention expired. **The DLQ is what makes "don't delete it" a safe
choice rather than a leak.**

### D6. Healthy traffic was never affected

```bash
OID=$(neworder '{"customer_id":"dave","item":"Webcam","amount":"75.00"}')
sleep 7
docker compose logs billing --no-log-prefix --since 30s | grep "$OID"
```

```
12:08:29  💳 BILLED order 04cc5422-… — dave 75.00 (event 5be4eefc-…)
```

✅ **Scenario D passes.**

### D7. Restore

```bash
$LS sqs set-queue-attributes --queue-url "$(qurl billing-queue)" --attributes VisibilityTimeout=30
$LS sqs purge-queue --queue-url "$(qurl billing-dlq)"
```

---

## Phase 6 checklist

- [x] **A** — relay exits 17 mid-loop; row stays unpublished; restart republishes; all 3 consumers no-op; one side effect each
- [x] **B** — same event delivered 3×; one billing row, one shipment, one Redis key; the claim never changes hands
- [x] **C** — Notifications down; billing/shipping unaffected; notify-queue holds the backlog; no unpublished outbox rows; drains on restart
- [x] **D** — poison message retried exactly `maxReceiveCount` times, moved to that queue's own DLQ intact, consumer survives, healthy traffic unaffected

All four also run automatically:

```bash
set -a; source .env; set +a
.myenv/bin/pytest
```

```
7 passed in 120.84s
```

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `column outbox.attempts does not exist` | Phase 6 added columns; `create_all()` never ALTERs — `docker compose down -v` |
| Relay exits 17 unexpectedly | `CRASH_AFTER_PUBLISH=1` still set — `CRASH_AFTER_PUBLISH=0 docker compose up -d relay` |
| Scenario A shows no duplicate | the relay was restarted before the row was reset; check `published` is `f` first |
| Poison message never reaches the DLQ | visibility timeout still 30s (wait longer), or the redrive policy is missing — D1 |
| `PurgeQueueInProgress` | SQS allows one purge per queue per 60s; wait |
| Counts higher than expected | earlier scenarios in the same session; `docker compose down -v` to reset |
