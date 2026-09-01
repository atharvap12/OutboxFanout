# Verifying Phase 5 — Notifications Consumer (Redis Idempotency)

Commands to prove the Phase 5 STOP condition on demand:

> **Manually redeliver twice; the Redis check correctly skips the 2nd.**

**What changed.** The third pigeonhole finally has a reader, so all three
consumers are live and the project runs end to end for the first time.

**What is genuinely different — and it is not "Redis instead of Postgres".**
Swapping the storage engine would be a boring difference. The real one:

    Billing        the dedup marker IS the side effect. One row, one UNIQUE
                   constraint, one transaction. No "between" exists.

    Notifications  the marker (a Redis key) and the side effect (an email
                   leaving the building) are in TWO SYSTEMS no transaction
                   spans. Two acts, in an order, with a gap.

That gap is the whole of Phase 5 — the same shape as the relay's Postgres/SNS
problem, one layer further down the pipe. It is why this consumer *can* lose a
notification where Billing cannot lose a billing record, and Step 5 proves that
rather than asserting it.

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
`notifications` all up. Unlike Phases 1–4, **redis must actually be healthy** —
this is the first phase that uses it.

---

## Step 1 — It talks to Redis, and cannot talk to Postgres

```bash
docker compose logs notifications --no-log-prefix | head -5
```

```
notifications consumer starting
redis reachable at redis://redis:6379/0 (dedup TTL 172800s = 48.0h)
consumer starting: queue 'notify-queue' at http://localstack:4566 (long poll 20s, batch 10)
queue 'notify-queue' resolved to http://sqs…:4566/000000000000/notify-queue
```

The `ping()` at boot is deliberate. The classic mistake here is
`REDIS_HOST=localhost` inside a container — which would otherwise not surface
until the first message arrived, as a stack trace buried in the consumer loop
looking like a message-handling bug rather than a config one.

Now prove the separation is structural, not just conventional:

```bash
docker compose exec -T notifications python -c "import sqlalchemy"
docker compose exec -T notifications python -c "import psycopg"
docker compose exec -T notifications python -c "import redis, boto3; print(redis.__version__, boto3.__version__)"
```

```
ModuleNotFoundError: No module named 'sqlalchemy'
ModuleNotFoundError: No module named 'psycopg'
5.3.1 1.43.85
```

**There is no database driver in this image at all.** "Uses a different
idempotency store" is a fact about the *build*, not a claim about the code — it
cannot accidentally grow a Postgres dependency without someone editing
`notifications/requirements.txt` on purpose. Same reasoning as `order/` being
absent from all three consumer images.

---

## Step 2 — THE STOP CONDITION: redeliver twice

As in Phase 4, force the duplicate the honest way — make the **relay**
republish, which is literally what a Scenario A crash causes:

```bash
OID=$(neworder '{"customer_id":"cust-stop5","item":"Desk lamp","amount":"42.00"}')
sleep 6
$REDIS GET notify:processed:$OID      # which event claimed it
$REDIS TTL notify:processed:$OID      # ~172798

for round in 1 2; do
  $PSQL -c "UPDATE outbox SET published=false, published_at=NULL WHERE order_id='$OID';"
  until [ "$($PSQL -c "SELECT published FROM outbox WHERE order_id='$OID'")" = "t" ]; do sleep 1; done
  sleep 7
done

docker compose logs notifications --no-log-prefix | grep "$OID"
```

**Recorded:**

```
redis value (claiming event): ebf99f85-b858-4a9f-ac7c-9ef98b647580
redis TTL: 172798s

EMAIL SENT lines : 1
DUPLICATE lines  : 2
redis keys       : 1
```

✅ **STOP condition met.**

The key stores the **claiming event_id** rather than a bare `1` as the design
doc suggests. Costs nothing, and it lets the duplicate log line say *who got
there first* — so you can distinguish "one event redelivered three times" from
"three different events about one order" without guessing.

---

## Step 3 — All three consumers, one event, two strategies

```bash
docker compose logs billing shipping notifications --no-log-prefix | grep "$OID" | sort
```

```
14:23:28  💳 BILLED     order 9f5c4013-…  (event ebf99f85-…)      <- Postgres UNIQUE
14:23:28  📦 SHIPPED    order 9f5c4013-…  (event ebf99f85-…)      <- Postgres UNIQUE
14:23:28  📧 EMAIL SENT order 9f5c4013-…                          <- Redis SET NX
14:23:32  🔁 DUPLICATE ×3   (billing, shipping, notifications)
14:23:42  🔁 DUPLICATE ×3   (billing, shipping, notifications)

billing_records=1   shipments=1   redis_keys=1
```

One publish → three queues → three independent dedup stores → two different
strategies → exactly one side effect each. **This is the whole project working.**

---

## Step 4 — ⚠️ Why `SET NX` and not `EXISTS` then `SET`

    A CAFÉ WITH ONE TOILET AND ONE KEY ON A HOOK.

    Right way: you TAKE THE KEY. One motion. Hand comes back with it or empty.

    Wrong way: you GLANCE at the hook, see a key, then reach for it. Two people
    glance at the same instant, both see a key, both reach.

The wrong way is `EXISTS` followed by `SET`. Here it is measured — 50 threads,
released simultaneously by a `threading.Barrier`, 20 trials of each:

```bash
docker compose exec -T notifications python -c "
import uuid, threading
from collections import Counter
from shared.redis_client import client

def race(fn, n=50):
    winners = []; lock = threading.Lock(); barrier = threading.Barrier(n)
    def run():
        barrier.wait()                       # release all 50 at once
        if fn():
            with lock: winners.append(1)
    ts = [threading.Thread(target=run) for _ in range(n)]
    [t.start() for t in ts]; [t.join() for t in ts]
    return len(winners)

TRIALS = 20
nx, cts = Counter(), Counter()
for _ in range(TRIALS):
    k = f'probe:{uuid.uuid4()}'
    nx[race(lambda: client().set(k, 'x', nx=True, ex=60))] += 1
    client().delete(k)
    k2 = f'probe:{uuid.uuid4()}'
    def check_then_set(key=k2):
        if not client().exists(key):
            client().set(key, 'x', ex=60); return True
        return False
    cts[race(check_then_set)] += 1
    client().delete(k2)

print('SET NX          winners/trial:', dict(sorted(nx.items())))
print('EXISTS then SET winners/trial:', dict(sorted(cts.items())))
print('duplicate sends:', sum((k-1)*v for k,v in cts.items() if k>1))
"
```

**Recorded:**

```
SET NX          winners/trial: {1: 20}
EXISTS then SET winners/trial: {8: 1, 10: 1, 12: 2, 13: 1, 14: 1, 16: 1,
                                17: 1, 19: 4, 23: 1, 25: 3, 26: 3, 36: 1}

SET NX          trials with >1 winner:  0/20
EXISTS then SET trials with >1 winner: 20/20
EXISTS then SET total duplicate sends: 370
```

**370 duplicate emails across 20 trials.** `SET NX` never once produced a second
winner; the racy version never once produced only one.

### ⚠️ The methodological point, which is half the lesson

The **first** attempt at this test — without the `threading.Barrier` — returned
**1 winner for both versions**. The racy code looked perfectly correct. Thread
startup overhead is larger than a Redis round trip, so the threads ran
effectively one at a time and never actually overlapped.

That is exactly how this bug behaves in real life:

> **A race you cannot reproduce is not a race that is absent.** It is one whose
> window you have not managed to hit yet.

It will pass every test you write by hand and fail under production load, as
"some customers got two emails and I can't reproduce it." The barrier is not
cheating — it is the instrument that makes an intermittent fault deterministic,
and building one is often the only way to prove a concurrency fix works.

### Third time, same idea

| where | mechanism |
| --- | --- |
| relay | `SELECT … FOR UPDATE SKIP LOCKED` |
| billing / shipping | `INSERT … ON CONFLICT DO NOTHING` |
| notifications | `SET key value NX EX ttl` |

**Do the check inside the thing that claims, never before it.**

Note also a small mercy Redis grants that Postgres did not: the return value of
`SET NX` *is* the fresh/duplicate verdict (`True` vs `None`). Phase 4's
`ON CONFLICT DO NOTHING` succeeds silently either way and needed a `RETURNING`
clause — and using `rowcount` instead silently reported every event as a
duplicate.

Reference: Redis SET — https://redis.io/docs/latest/commands/set/

---

## Step 5 — ⚠️ THE ORDERING DECISION, and its real cost

    A LETTER YOU CANNOT UNPOST.

    Billing writes in a ledger, and the ledger entry IS the payment — nothing
    can drift out of sync, because there is only one thing.

    Sending an email is dropping a letter into a postbox. Once gone, no database
    work brings it back. And ticking your notebook is a SEPARATE act from
    dropping the letter. So: tick first, or post first?

| order | crash in the gap | verdict |
| --- | --- | --- |
| post, then tick (send → mark) | letter posted, no record → tomorrow you post a **second** | record-keeping fails in the exact direction it exists to prevent |
| **tick, then post (mark → send)** | notebook claims a letter never posted → **one lost** | never permissive; loss possible |

We tick first. Prove the cost is real rather than theoretical:

```bash
CRASH_AFTER_MARK=1 docker compose up -d notifications
OID=$(neworder '{"customer_id":"cust-crash","item":"Cable tidy","amount":"12.00"}')
# wait for the container to die
docker inspect -f '{{.State.ExitCode}}' outboxfanout-notifications-1
$REDIS EXISTS notify:processed:$OID
docker compose logs notifications --no-log-prefix | grep "$OID" | grep -c 'EMAIL SENT'
```

**Recorded:**

```
CRASH_AFTER_MARK — exiting between the Redis SET and the send

exit code = 19    (crashed exactly where aimed, not a generic failure)
redis key = 1     (marked as notified)
emails    = 0     (never sent)
```

Restart cleanly and let the redelivery arrive:

```bash
CRASH_AFTER_MARK=0 docker compose up -d notifications
sleep 10
docker compose logs notifications --no-log-prefix --since 90s | grep "$OID"
```

```
🔁 DUPLICATE ignored for order ae6c200d-… — already notified by event 6b694969-…

emails for this order: 0
```

**The notification is permanently lost — and the dedup check working exactly as
designed is what makes it permanent.** Meanwhile, the same order, same crash
window, via the other strategy:

```
billing_records=1
shipments=1
```

### This contrast is the core learning artifact of the whole project

Billing survived the identical class of crash with **neither loss nor
duplicate**, because its marker and its side effect were one database write.
Notifications could not, because its side effect escapes the database. **The
mechanism did not fail — the situation is strictly harder**, and no amount of
Redis cleverness fixes it. That is the honest answer to "which idempotency
strategy is better": neither, and the side effect decides.

### ⚠️ Note this is the OPPOSITE polarity to the relay

```
relay          publish, THEN mark   ->  prefers DUPLICATES over loss
notifications  mark,    THEN send   ->  prefers LOSS over duplicates
```

Both correct, from one rule: **choose the failure your next hop can handle.**
The relay's next hop is three consumers built to absorb duplicates. This
consumer's next hop is a human's inbox, where nothing downstream can undo an
email.

### The mitigation that is in the code

`notifications/service.py` releases the claim when the send raises:

```python
except Exception:
    client().delete(key)     # hand the key back to the hook
    raise
```

Without it, one transient SMTP failure suppresses that customer's notification
**for 48 hours**. With it, the loss window shrinks from "any send failure" to
"hard-killed between the SET and this handler" — which is exactly what
`CRASH_AFTER_MARK`'s `os._exit()` simulates. It cannot close the window: if the
send actually succeeded and only the acknowledgement was lost, deleting the key
sends a second email. Same unresolvable ambiguity the relay has with SNS.

---

## Step 6 — Redis durability, and how it differs from Postgres

```bash
$REDIS DBSIZE ; $REDIS CONFIG GET appendonly
docker compose restart redis
$REDIS DBSIZE ; $REDIS TTL notify:processed:$OID
```

**Recorded:**

```
before: DBSIZE=2  our key=1  TTL=157725s  AOF=yes
after : DBSIZE=2  our key=1  TTL=157723s
```

AOF works — keys **and their remaining TTLs** survive a restart, still counting
down. But this is a **weaker** guarantee than Billing's, in three specific ways:

1. **`appendfsync everysec`** (the default) can lose ~1 second of writes on a
   hard kill. Postgres commits reach the WAL durably before returning.
2. **`docker compose down -v` destroys the volume.** Both consumers lose state
   that way — but Billing's markers *are* the business records, so losing them
   is obviously catastrophic, whereas losing Redis keys looks like "just a
   cache" right up until it double-sends.
3. **The TTL has no Postgres equivalent.** After 48h the key is gone and a late
   redelivery is treated as new. SQS retention defaults to **4 days** — longer
   than the TTL — so that window is real, not theoretical. Billing dedupes
   forever. (Documented in `shared/config.py`; a genuine unfixed sharp edge,
   recorded rather than quietly tuned away.)

---

## Step 7 — Independence (Scenario C, third consumer)

```bash
docker compose stop notifications
OID=$(neworder '{"customer_id":"cust-solo5","item":"Mouse pad","amount":"9.00"}')
sleep 6
$PSQL -c "SELECT count(*) FROM billing_records WHERE order_id='$OID';"
$REDIS EXISTS notify:processed:$OID
docker compose start notifications
```

**Recorded:**

```
notifications DOWN:
  billing_records = 1
  shipments       = 1
  redis key       = 0
  notify-queue depth = 1

after restart:
  redis key = 1
  📧 EMAIL SENT to cust-solo5 — order c83240cb-… confirmed (Mouse pad, 9.00)
```

Billing and Shipping never knew Notifications was down. The event was not lost
or retried — it sat in notify-queue and drained on restart. **Three consumers,
three independent failure domains, two databases, zero shared state.**

Clean shutdown timing, for the same reason as Phase 4:

```
18:36:01 received SIGTERM — finishing the current message, then stopping
18:36:02 consumer stopped cleanly
```

One second here (the signal landed early in the poll), but it can take up to
~20s if it lands just after one begins — hence `stop_grace_period: 25s`. A
SIGKILL mid-handler here would be `CRASH_AFTER_MARK`, i.e. a genuinely lost
notification on every deploy.

---

## Phase 5 checklist

- [x] `SET key value NX EX ttl` as ONE command, never `EXISTS` then `SET`
- [x] Atomicity measured under real concurrency (20 trials × 50 threads → 1 winner every time)
- [x] The racy alternative measured failing (20/20 trials, 370 duplicate sends)
- [x] The claim is made BEFORE the side effect, not after
- [x] The claim is released if the send fails, so a retry can happen
- [x] One order → exactly one "email sent"
- [x] Redelivered twice → still exactly one, 2 duplicates logged
- [x] The key stores the claiming event_id, so duplicates are traceable
- [x] TTL set and verified (48h), and its risk vs 4-day SQS retention understood
- [x] The mark-then-send loss demonstrated with `CRASH_AFTER_MARK` (exit 19)
- [x] The same order billed and shipped fine — the strategies compared under one crash
- [x] Redis AOF persistence verified across restart (keys + TTLs)
- [x] The image contains no database driver at all
- [x] `shared/consumer.py` reused completely unchanged from Phase 4

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Connection refused` to Redis at boot | `REDIS_HOST` is `localhost` instead of `redis` |
| Every event treated as new | wrong key namespace, or TTL expired; `$REDIS KEYS 'notify:*'` |
| Every event treated as duplicate | stale keys from an earlier run; `$REDIS FLUSHDB` |
| Notification never arrives, no error | crashed between mark and send — Step 5 |
| Duplicate emails under load only | `EXISTS`-then-`SET` somewhere instead of `SET NX` |
| Keys vanish after `down -v` | expected; the volume is deleted with the stack |
| Consumer exits at boot, no retry | the `ping()` failed; no `restart:` policy is deliberate |
| Poison message repeats forever | expected until Phase 6 adds DLQs; purge the queue |
