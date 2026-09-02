# OutboxFanout

**Transactional Outbox + SNS/SQS fan-out, with two idempotency strategies
deliberately mixed so they can be compared.**

An Order Service accepts orders and must reliably notify three independent
downstream services — Billing, Shipping and Notifications. This project proves
the hand-off never silently loses an event, and never lets a crash cause a
duplicate side effect that matters.

It is not a product. It is a proof that the two patterns are understood
*including their failure modes*, which is why the fault-injection suite is the
real deliverable rather than the happy path.

Runs entirely locally. No AWS account, no spend.

---

## Architecture

```
Client
  │  POST /orders
  ▼
┌──────────────────┐   ONE transaction:                  ┌────────────┐
│  Order Service   │───INSERT order + INSERT outbox row──▶│  Postgres  │
│    (FastAPI)     │   (never talks to SNS)               └─────┬──────┘
└──────────────────┘                                            │
                                                                │ polls
                          ┌─────────────────┐   unpublished rows│ every 2s
                          │  Outbox Relay   │◀──────────────────┘
                          │ (own process)   │
                          └────────┬────────┘
                                   │ publish, THEN mark published
                                   ▼
                        ┌──────────────────────┐
                        │ SNS topic: order-events │
                        └──────────┬───────────┘
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
             billing-queue  shipping-queue   notify-queue
                    │              │              │
                    ▼              ▼              ▼
              ┌──────────┐   ┌──────────┐   ┌──────────────┐
              │ Billing  │   │ Shipping │   │Notifications │
              ├──────────┤   ├──────────┤   ├──────────────┤
              │ Postgres │   │ Postgres │   │    Redis     │
              │  UNIQUE  │   │  UNIQUE  │   │  SET NX EX   │
              └──────────┘   └──────────┘   └──────────────┘
                    │              │              │
                    ▼              ▼              ▼
              billing-dlq    shipping-dlq    notify-dlq
```

**Why this shape**

- The Order Service **only ever writes to its own database**. That single write
  is what makes the operation atomic — it is the entire point of the outbox
  pattern, and why there is no code path from the API to SNS.
- The relay is a **separate process**, so a slow or crashing relay never blocks
  order creation and can be restarted independently.
- SNS **fans out once** to three queues rather than the relay looping over three
  destinations. Adding a fourth consumer is one entry in the bootstrap script
  and zero changes to the relay.
- **Each consumer owns its own dedup store.** Deleting every `billing_records`
  row must not let Shipping ship twice.

---

## Quick start

Requires Docker with the Compose v2 plugin (`docker compose`, not
`docker-compose`).

```bash
git clone <this repo> && cd OutboxFanout
cp .env.example .env          # then set PG_PASSWORD
docker compose up -d --build
docker compose ps             # bootstrap should be Exited (0); the rest Up
```

Create an order and watch all three consumers handle it:

```bash
curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"alice","item":"Standing desk","amount":"899.00"}'

sleep 5
docker compose logs billing shipping notifications --no-log-prefix | tail -3
```

```
💳 BILLED     order 9f5c4013-… — alice 899.00 (event ebf99f85-…)
📦 SHIPPED    order 9f5c4013-… — 'Standing desk' tracking TRK-976F6BBF344F
📧 EMAIL SENT to alice — order 9f5c4013-… confirmed (Standing desk, 899.00)
```

Tear down with `docker compose down` (keeps data) or `down -v` (wipes it).

---

## The two idempotency strategies

Implemented **on purpose** as a comparison, not because two were needed.

|  | Postgres UNIQUE | Redis `SET NX EX` |
| --- | --- | --- |
| Used by | Billing, Shipping | Notifications |
| Mechanism | `INSERT … ON CONFLICT (order_id) DO NOTHING` | `SET notify:processed:{id} <event_id> NX EX 172800` |
| Durability | DB is the source of truth | AOF survives restart; `everysec` can lose ~1s |
| Expiry | never — dedupes forever | 48h TTL |
| If a code path forgets the check | **impossible** — the constraint refuses | silently double-processes |
| Best fit | when the side effect **is** the DB write | when it is not (email, SMS, third-party API) |

**The finding that matters.** These are not interchangeable, and which is
"better" is not a matter of taste — **the side effect decides**:

- Billing's dedup marker **is** its side effect. One row, one constraint, one
  transaction. There is no gap for a crash to land in, because marking and
  doing are a single act.
- Notifications' marker (a Redis key) and its side effect (an email leaving the
  building) are in two systems no transaction spans. Two acts, in an order,
  with a gap.

Scenario A and the `CRASH_AFTER_MARK` demo below crash both consumers in the
same window. Billing survives with neither loss nor duplicate. Notifications
loses a notification. **The mechanism did not fail — the situation is strictly
harder**, and no amount of Redis cleverness fixes it.

---

## Running the fault-injection suite

The four scenarios are the actual deliverable. They are pytest tests that drive
the real stack — killing the relay, stopping a consumer, injecting poison
messages — and restore it afterwards.

```bash
python -m venv .myenv && source .myenv/bin/activate
pip install -r tests/requirements.txt

set -a; source .env; set +a       # tests read PG_PASSWORD etc.
docker compose up -d --build      # the suite needs a running stack

pytest
```

```
tests/test_outbox_head_of_line.py::test_poison_outbox_row_does_not_block_healthy_rows PASSED
tests/test_scenario_a_relay_crash.py::test_relay_crash_between_publish_and_mark      PASSED
tests/test_scenario_b_duplicate_delivery.py::test_repeated_delivery_produces_one…    PASSED
tests/test_scenario_b_duplicate_delivery.py::test_duplicate_delivered_straight_to…   PASSED
tests/test_scenario_c_consumer_offline.py::test_notifications_offline_others_unaff…  PASSED
tests/test_scenario_d_poison_dlq.py::test_poison_message_lands_in_dlq                PASSED
tests/test_scenario_d_poison_dlq.py::test_poison_message_does_not_block_healthy_tr…  PASSED

7 passed in 127.92s
```

Run one at a time with `pytest tests/test_scenario_a_relay_crash.py`.

> The tests run on the **host**, not in a container — deliberately. Scenario A
> has to kill the relay and Scenario C has to stop a consumer, and a test inside
> a container cannot stop the container it lives in.

### Scenario A — relay crashes between publishing and marking

*"The single most important proof in the whole project."*

The relay publishes to SNS, then marks the row published. No transaction spans
those two steps, so a crash in between leaves a row that **was** published but
still looks unsent — and on restart it is published again.

```bash
CRASH_AFTER_PUBLISH=1 docker compose up -d relay
curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"bob","item":"Lamp","amount":"42.00"}'

docker inspect -f '{{.State.ExitCode}}' outboxfanout-relay-1     # 17
docker compose logs relay --no-log-prefix | grep CRASH

CRASH_AFTER_PUBLISH=0 docker compose up -d relay                 # republishes
sleep 8
docker compose logs billing shipping notifications --no-log-prefix | grep DUPLICATE
```

The duplicate is **expected** — it is the price of at-least-once. What is being
proved is that it is harmless: all three consumers no-op, via two different
mechanisms. Exit code 17 rather than 1 so the test can assert the process died
exactly where it was aimed.

### Scenario B — forced duplicate delivery

```bash
# Make the relay republish a row (exactly what a Scenario A crash leaves behind)
docker exec outboxFanout-postgres psql -U "$PG_USER" -d "$PG_DB" \
  -c "UPDATE outbox SET published=false, published_at=NULL WHERE order_id='<ORDER_ID>';"
```

Expected: exactly one `billing_records` row, one `shipments` row, one Redis key
— never two, however many times it is delivered.

### Scenario C — one consumer down, others healthy

```bash
docker compose stop notifications
curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"carol","item":"Mouse pad","amount":"9.00"}'
sleep 6

# Billing and Shipping processed it; notify-queue is holding the backlog
docker exec outboxFanout-postgres psql -U "$PG_USER" -d "$PG_DB" -At \
  -c "SELECT count(*) FROM billing_records;"
docker exec outboxFanout-redis redis-cli KEYS 'notify:processed:*' | wc -l

docker compose start notifications      # drains the backlog, nothing lost
```

This works only because each queue holds its own durable copy with its own
delivery state and no shared cursor. It is the payoff for fanning out through
SNS instead of having the relay call three consumers itself.

### Scenario D — poison message reaches the DLQ

```bash
QURL=$(docker compose exec -T billing python -c \
  "from shared import aws,config; print(aws.queue_url(config.BILLING_QUEUE_NAME))")

# Speed up redelivery so five attempts take seconds, not 2.5 minutes
docker compose exec -T billing python -c "
from shared import aws
aws.sqs().set_queue_attributes(QueueUrl='$QURL', Attributes={'VisibilityTimeout':'1'})"

docker compose exec -T billing python -c "
from shared import aws
aws.sqs().send_message(QueueUrl='$QURL', MessageBody='{not valid json at all')"

sleep 30
docker compose exec -T billing python -c "
from shared import aws
url = aws.queue_url('billing-dlq')
print(aws.sqs().get_queue_attributes(QueueUrl=url,
      AttributeNames=['ApproximateNumberOfMessages'])['Attributes'])"
```

The consumer **does not delete** an unparsable message — deleting would destroy
the evidence. Leaving it lets SQS count deliveries and, after
`maxReceiveCount=5`, move it to that queue's own DLQ. Without the redrive
policy the same message would redeliver until the 4-day retention expired.

### Bonus — the notification that gets lost

Not one of the four scenarios, but the sharpest demonstration of why the two
idempotency strategies are not interchangeable:

```bash
CRASH_AFTER_MARK=1 docker compose up -d notifications
curl -s -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"dave","item":"Cable tidy","amount":"12.00"}'

docker inspect -f '{{.State.ExitCode}}' outboxfanout-notifications-1   # 19
docker exec outboxFanout-redis redis-cli KEYS 'notify:processed:*'     # key IS set
docker compose logs notifications --no-log-prefix | grep 'EMAIL SENT'  # never sent

CRASH_AFTER_MARK=0 docker compose up -d notifications
sleep 8
# the redelivery is correctly skipped as a duplicate -> notification LOST
# meanwhile the same order billed and shipped normally
```

---

## Layout

```
order/           FastAPI service. POST /orders writes order + outbox in ONE txn
relay/           Standalone poller: claims a row, publishes to SNS, marks it
bootstrap/       One-shot idempotent setup: topic, queues, DLQs, subscriptions
billing/         Consumer — Postgres UNIQUE idempotency
shipping/        Consumer — Postgres UNIQUE idempotency (same, different table)
notifications/   Consumer — Redis SET NX idempotency
shared/          config, db, aws, redis_client, log, messages, consumer loop
tests/           Scenarios A–D as pytest tests
VERIFY/          Per-phase verification walkthroughs with recorded output
```

`shared/consumer.py` holds the receive → handle → delete loop that all three
consumers share. It knows about queues, timers, redeliveries and shutdown, and
nothing about what a message means. That it needed **zero** changes to support a
consumer with a completely different dedup store is the evidence the boundary
was drawn in the right place.

---

## Design decisions worth knowing

**Publish, then mark (at-least-once).** Mark-then-publish loses events
*silently and permanently* — the row claims it was sent, so no retry ever fires
and detecting the loss requires an oracle outside the system. A duplicate is
loud, visible, and contained by one check at the receiving end. The outbox
pattern does not remove the dual-write problem; it **converts an unrecoverable
failure into a recoverable one**.

**Per-row transactions, not per-batch.** A batch-wide `UPDATE` is atomic, which
is precisely why a crash before its commit republishes the *whole* batch.
Per-row keeps the blast radius at exactly one duplicate.

**`SELECT … FOR UPDATE SKIP LOCKED`**, with the `published = false` check
re-read *inside* the lock. The batch read is deliberately lock-free, so another
relay may publish a row in the gap. Three places in this codebase solve the same
problem the same way:

| where | mechanism |
| --- | --- |
| relay | `SELECT … FOR UPDATE SKIP LOCKED` |
| billing / shipping | `INSERT … ON CONFLICT DO NOTHING` |
| notifications | `SET key value NX EX ttl` |

**Do the check inside the thing that claims, never before it.**

**The dedup key comes from the domain, never the transport.** `event_id` is the
outbox row's primary key, which does not change however many times the row is
republished. An SNS MessageId is minted fresh per publish; an SQS MessageId per
queue per copy. Deduping on either fails *asymmetrically* — correct for an SQS
redelivery, silently wrong for a relay republish — so it passes exactly the
tests you would naturally write and breaks only during a real crash.

**Head-of-line blocking, found and fixed.** `relay_batch()` used a bare `break`
on any failure. One row SNS would never accept sorted first in every batch
forever and abandoned every healthy row behind it — the outbox stopped draining
permanently while the relay logged an error every 2s and looked healthy.
Fixed with an `attempts` counter plus `failed_at`: a row that keeps failing is
**parked** and stops being selected, and known-permanent SNS errors park on the
first attempt. A dead-letter queue for the outbox table, which is why it landed
here alongside the SQS DLQs. Parked rows are never marked published and never
deleted — silently discarding an event is the one thing this architecture exists
to prevent.

Longer write-ups, including every measurement and two bugs the obvious test was
blind to, are in `BLUEPRINT.md` and `VERIFY/`.

---

## Known limitations

Recorded rather than hidden — each is a deliberate scope decision.

- **Ingress is not idempotent.** A client retry after a lost `201` mints a new
  `order_id`, and every downstream check correctly passes it. The system is
  idempotent everywhere except the one place that creates identity. The fix is
  a client-supplied `Idempotency-Key` behind a UNIQUE constraint.
- **The 48h Redis TTL is shorter than SQS's 4-day retention.** A message
  redelivered on day 3 would find the key expired and send a second email. Real,
  not theoretical.
- **The queue policies are correct but unproven locally.** LocalStack does not
  evaluate IAM unless `ENFORCE_IAM=1`, so every fan-out test here would pass
  with no policies at all. The verification recipe is in `BLUEPRINT.md`.
- **`create_all()` never ALTERs.** Any change to an existing table needs
  `docker compose down -v`. Alembic is the real answer.
- **Bind-mounted source shadows the baked image.** Right for development, wrong
  for shipping — a real deployment must run the baked-in code.
- **One relay.** `SKIP LOCKED` makes multiple relays safe (verified with
  `--scale relay=2`), but the SNS publish happens inside the transaction holding
  the row lock. Defensible at one relay, not at fifty.

---

## Tech

Python 3.13 · FastAPI · SQLAlchemy 2 · psycopg 3 · PostgreSQL 16 · Redis 7 ·
LocalStack 3 (SNS + SQS) · boto3 · Docker Compose · pytest
