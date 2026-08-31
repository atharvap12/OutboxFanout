# Verifying Phase 5 — Notifications Consumer (Redis Idempotency)

**Goal.** The same guarantee as Phase 4, implemented a different way, so the two
approaches can be compared from direct experience.

**STOP condition.** Redeliver the same message twice; the Redis check correctly
skips the 2nd.

**What is genuinely different.** Not "Redis instead of Postgres". The real
difference is that here **the dedup marker and the side effect live in two
different systems**, so no transaction spans them — the same shape as the
relay's Postgres/SNS gap. Billing's constraint and its row are one write; a
Redis key and a sent email are two operations with a gap between them.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a

PSQL="docker exec outboxFanout-postgres psql -U $PG_USER -d $PG_DB -At"
REDIS="docker exec outboxFanout-redis redis-cli"

neworder() {
  curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
    -d "$1" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])'
}
```

```bash
docker compose up -d --build
docker compose ps -a
```

Expect `bootstrap` **Exited (0)**, and `order`, `relay`, `billing`, `shipping`,
`notifications` all up. Redis must be healthy — unlike Phases 1-4, this one
actually uses it.

---

## Step 1 — It connects to Redis, not Postgres

```bash
docker compose logs notifications --no-log-prefix | head -5
```

```
notifications consumer starting
redis reachable at redis://redis:6379/0 (dedup TTL 172800s)
consumer starting: queue 'notify-queue' at http://localstack:4566 (long poll 20s, batch 10)
queue 'notify-queue' resolved to http://sqs...:4566/000000000000/notify-queue
```

The `ping()` at boot is deliberate: a wrong `REDIS_URL` fails here, loudly, at
startup — not on the first message that arrives an hour later.

Prove the image really has no database driver:

```bash
docker compose exec -T notifications python -c "import sqlalchemy"
docker compose exec -T notifications python -c "import psycopg"
docker compose exec -T notifications python -c "import redis, boto3; print(redis.__version__, boto3.__version__)"
```

```
ModuleNotFoundError: No module named 'sqlalchemy'
ModuleNotFoundError: No module named 'psycopg'
5.3.1 1.43.83
```

The "different idempotency store" claim is true of the **build**, not just the
code. This consumer physically cannot reach Postgres.

---

## Step 2 — THE STOP CONDITION: redeliver twice

```bash
OID=$(neworder '{"customer_id":"cust-stop5","item":"Desk lamp","amount":"42.00"}')
sleep 6
$REDIS GET notify:processed:$OID     # which event claimed it
$REDIS TTL notify:processed:$OID     # ~172798

for round in 1 2; do
  $PSQL -c "UPDATE outbox SET published=false, published_at=NULL WHERE order_id='$OID';"
  until [ "$($PSQL -c "SELECT published FROM outbox WHERE order_id='$OID'")" = "t" ]; do sleep 1; done
  sleep 7
done

docker compose logs notifications --no-log-prefix | grep "$OID"
```

**Recorded output:**

```
18:34:43  📧 EMAIL SENT to cust-stop5 — order a203cd6e-… confirmed (Desk lamp, 42.00)
18:34:47  🔁 DUPLICATE ignored for order a203cd6e-… — already notified by event 2e104f34-… (this delivery: 2e104f34-…)
18:34:57  🔁 DUPLICATE ignored for order a203cd6e-… — already notified by event 2e104f34-… (this delivery: 2e104f34-…)

EMAIL SENT lines : 1
DUPLICATE lines  : 2
redis keys       : 1
```

✅ **STOP condition met.**

The key stores the **claiming event_id**, which is why the duplicate log line
can say *who* got there first. Here both ids match, so this was one event
redelivered — not two different events about one order.

---

## Step 3 — All three consumers, one event, three mechanisms

```bash
docker compose logs billing shipping notifications --no-log-prefix | grep "$OID" | sort
```

```
18:34:43  💳 BILLED     order a203cd6e-…            <- Postgres UNIQUE
18:34:43  📦 SHIPPED    order a203cd6e-…            <- Postgres UNIQUE
18:34:43  📧 EMAIL SENT order a203cd6e-…            <- Redis SET NX
18:34:47  🔁 DUPLICATE ×3   (billing, shipping, notifications)
18:34:57  🔁 DUPLICATE ×3   (billing, shipping, notifications)
```

One publish, three independent copies, three independent dedup stores, two
different strategies — and exactly one side effect each. **This is the whole
project working end to end.**

---

## Step 4 — ⚠️ Why `SET NX` and not `EXISTS` then `SET`

The design doc says one atomic command, not a check followed by a write.
Here is that claim tested rather than trusted — 50 threads racing for one key:

```bash
docker compose exec -T notifications python -c "
import uuid, threading
from shared.redis_client import client

key = f'probe:{uuid.uuid4()}'
winners = []; lock = threading.Lock()
def claim():
    if client().set(key, 'x', nx=True, ex=60):
        with lock: winners.append(1)
ts = [threading.Thread(target=claim) for _ in range(50)]
[t.start() for t in ts]; [t.join() for t in ts]
print(f'  50 concurrent SET NX -> winners: {len(winners)}')

key2 = f'probe:{uuid.uuid4()}'
winners2 = []
def check_then_set():
    if not client().exists(key2):
        client().set(key2, 'x', ex=60)
        with lock: winners2.append(1)
ts = [threading.Thread(target=check_then_set) for _ in range(50)]
[t.start() for t in ts]; [t.join() for t in ts]
print(f'  50 concurrent EXISTS-then-SET -> winners: {len(winners2)}  <-- the race')
client().delete(key, key2)
"
```

**Recorded:**

```
  50 concurrent SET NX -> winners: 1
  50 concurrent EXISTS-then-SET -> winners: 3  <-- the race
```

**Three winners means three emails.** The window between `EXISTS` returning
"absent" and `SET` writing the key is real, is microseconds wide, and is
therefore exactly the bug you cannot reproduce on demand in a normal test and
will meet in production under load.

`SET NX` has no window because Redis executes commands one at a time — the
check happens *inside* the write. Third instance of one idea in this project:

| where | mechanism |
| --- | --- |
| relay | `SELECT … FOR UPDATE SKIP LOCKED` |
| billing / shipping | `INSERT … ON CONFLICT DO NOTHING` |
| notifications | `SET key value NX EX ttl` |

**Do the check inside the thing that claims, never before it.**

---

## Step 5 — ⚠️ THE ORDERING DECISION, and its real cost

Two steps, no transaction spanning them, so the order must be chosen:

| order | crash in the gap | verdict |
| --- | --- | --- |
| send, then mark | no record of the send → next delivery sends **again** | dedup fails in the permissive direction |
| **mark, then send** | key says "sent" when nothing was → notification **lost** | dedup never permissive; loss possible |

We mark first. Prove the cost is real:

```bash
CRASH_AFTER_MARK=1 docker compose up -d notifications
OID=$(neworder '{"customer_id":"cust-crash","item":"Cable tidy","amount":"12.00"}')

# wait for the container to die, then inspect
docker inspect -f '{{.State.ExitCode}}' outboxfanout-notifications-1
$REDIS EXISTS notify:processed:$OID
docker compose logs notifications --no-log-prefix | grep "$OID" | grep -c 'EMAIL SENT'
```

**Recorded:**

```
CRASH_AFTER_MARK — exiting between the Redis SET and the send

exit code : 19    (crashed exactly where aimed)
redis key : 1     (marked as notified)
emails    : 0     (never sent)
```

Now restart cleanly and let the redelivery arrive:

```bash
CRASH_AFTER_MARK=0 docker compose up -d notifications
sleep 10
docker compose logs notifications --no-log-prefix --since 60s | grep "$OID"
```

```
🔁 DUPLICATE ignored for order 7e30da7c-… — already notified by event 661fd428-…

emails for this order after recovery: 0
```

**The notification for that order is permanently lost.** The dedup check is
working exactly as designed — and that is precisely what makes the loss
permanent. Meanwhile the same order billed and shipped normally:

```
billing_records=1
shipments=1
```

**That contrast is the core learning artifact of the whole project.** Billing
survived the same class of crash with no loss and no duplicate, because its
marker and its side effect were one database write. Notifications could not,
because its side effect escapes the database. The mechanism did not fail — the
*situation* is strictly harder, and no amount of Redis cleverness fixes it.

Note also the polarity is **opposite to the relay's**, which chose duplicates
over loss. Both choices are right: the relay's duplicates land on consumers
built to absorb them; a duplicate email lands in a human's inbox where nothing
downstream can undo it. **Choose the failure your next hop can handle.**

### The mitigation that is in the code

`notifications/service.py` deletes the key if the send raises:

```python
try:
    _send_notification(...)
except Exception:
    client().delete(key)     # release the claim so a redelivery can retry
    raise
```

This shrinks the loss window from "any send failure" to "killed between the SET
and the except handler" — i.e. only SIGKILL or a power cut, which is what
`CRASH_AFTER_MARK` simulates with `os._exit()`. It does not close the window,
and cannot: if the send actually succeeded and only the acknowledgement was
lost, deleting the key means the redelivery sends a second email. Same
unresolvable ambiguity the relay has with SNS.

---

## Step 6 — Redis durability, and how it differs from Postgres

```bash
$REDIS DBSIZE
$REDIS CONFIG GET appendonly       # -> yes
docker compose restart redis
$REDIS DBSIZE                      # same
$REDIS TTL notify:processed:$OID   # preserved, counting down
```

**Recorded:**

```
keys before restart: 3    our key: 1    AOF: yes
keys after restart : 3    our key: 1    TTL: 172413s
```

AOF works — keys and their TTLs survive a container restart. But this is a
**weaker** guarantee than Billing's, in three ways worth stating precisely:

1. **`appendfsync everysec`** (the default) can lose up to ~1 second of writes
   on a hard kill. Postgres commits reach the WAL durably before returning.
2. **`docker compose down -v` destroys the volume**, and with it every dedup
   marker. The Postgres consumers lose theirs the same way, but their markers
   *are* the business records — losing them is obviously catastrophic, whereas
   losing Redis keys looks like "just a cache" until it double-sends.
3. **The TTL is an expiry the Postgres side has no equivalent of.** After 48h
   the key is gone and a late redelivery is treated as new. SQS retention
   defaults to **4 days**, which is longer than 48h — so this is possible, not
   merely theoretical. Billing dedupes forever.

---

## Step 7 — Independence (Scenario C, third consumer)

```bash
docker compose stop notifications
OID=$(neworder '{"customer_id":"cust-solo5","item":"Mouse pad","amount":"9.00"}')
sleep 6
$PSQL -c "SELECT count(*) FROM billing_records WHERE order_id='$OID';"   # 1
$REDIS EXISTS notify:processed:$OID                                       # 0
docker compose start notifications
sleep 10
$REDIS EXISTS notify:processed:$OID                                       # 1
```

**Recorded:**

```
notifications DOWN:
  billing_records = 1
  shipments       = 1
  redis key       = 0
  notify-queue depth = 1

after restart:
  redis key       = 1
  📧 EMAIL SENT to cust-solo5 — order b2651f35-… confirmed (Mouse pad, 9.00)
```

Billing and Shipping processed immediately and never knew Notifications was
down. The event was not lost or retried — it sat in notify-queue, and drained
on restart. Three consumers, three independent failure domains.

---

## Phase 5 checklist

- [x] `SET key value NX EX ttl` as ONE command, never `EXISTS` then `SET`
- [x] Atomicity tested under real concurrency (50 threads → 1 winner)
- [x] The racy alternative demonstrated failing (50 threads → 3 winners)
- [x] The claim is made BEFORE the side effect, not after
- [x] The claim is released if the send fails, so a retry can happen
- [x] One order → exactly one "email sent"
- [x] Redelivered twice → still exactly one, 2 duplicates logged
- [x] The key stores the claiming event_id, so duplicates are traceable
- [x] TTL is set and verified (48h), and its risk vs SQS retention understood
- [x] The mark-then-send loss is demonstrated, not just described
- [x] Redis AOF persistence across restart verified (keys + TTLs)
- [x] The image contains no database driver at all

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Connection refused` to Redis at boot | `REDIS_HOST` is `localhost` instead of `redis` |
| Every event treated as new | key namespace or TTL wrong; check `$REDIS KEYS 'notify:*'` |
| Every event treated as duplicate | a stale key from an earlier run; `$REDIS FLUSHDB` |
| Notification never arrives, no error | crashed between mark and send — Step 5 |
| Duplicate emails under load | `EXISTS`-then-`SET` somewhere instead of `SET NX` |
| Keys vanish after `down -v` | expected; the volume is deleted with the stack |
| Poison message repeats forever | expected until Phase 6 adds DLQs; purge the queue |
