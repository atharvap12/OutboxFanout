# Verifying Phase 1 — Order Service + Atomic Outbox Write

Commands to prove the Phase 1 STOP condition on demand:

> **Manually break the 2nd insert and prove the 1st one also rolls back.**

Everything here is re-runnable. Nothing requires editing code — the sabotage is
a configuration flag, so this doubles as the script for a demo or an interview.

**What Phase 1 is NOT.** No SNS, no SQS, no relay. The Order Service only ever
writes to its own database. That single restriction is the whole outbox
pattern; every later phase is a consequence of it.

---

## Step 0 — Load your environment

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a      # .env does NOT auto-export into your shell
```

---

## Step 1 — Start the stack

```bash
docker compose up -d order       # pulls in postgres via depends_on
docker compose ps
```

Wait for `postgres (healthy)` and `order Up`.

```bash
curl -s localhost:8000/health    # {"status":"ok"}
```

Interactive API docs: <http://localhost:8000/docs>

> If `order` exits immediately, read `docker compose logs order`. The usual
> cause is Postgres not being ready — which is exactly what
> `condition: service_healthy` prevents, so it should not happen.

---

## Step 2 — Clean slate

An idempotency-style proof is meaningless against leftover data. Reset first:

```bash
docker compose down -v           # -v deletes the volumes too
docker compose up -d order
sleep 10
```

Confirm both tables are empty:

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -tAc "select 'orders=' || count(*) from orders; select 'outbox=' || count(*) from outbox;"
```

Expect `orders=0`, `outbox=0`.

---

## Step 3 — The happy path

```bash
curl -s -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-42","item":"Mechanical keyboard","amount":"499.99"}' | jq
```

Expect `201` and a body with a generated `id` and `created_at`.

**The response comes back immediately.** It does not wait for a relay, SNS, or
any consumer — because the event is already durably recorded in the same
database as the order, so it cannot be lost even though it has not been sent.

### Both rows exist

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -c "select id, customer_id, item, amount, created_at from orders;"

docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -c "select event_type, published, published_at from outbox;"
```

Expect one order, and one outbox row with `published = f` and
`published_at` empty. **Nothing has been sent yet — that is correct.** The
delivery note is sitting in the out-tray waiting for a courier we have not
built.

### The payload is a full snapshot, not just an id

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -c "select jsonb_pretty(payload) from outbox;"
```

```json
{
    "item": "Mechanical keyboard",
    "amount": "499.99",
    "order_id": "…",
    "created_at": "2026-08-07T15:02:50.199843+00:00",
    "customer_id": "cust-42"
}
```

Two things to notice:

- **The whole order is in there, duplicated from the `orders` table.** That
  duplication is deliberate: this is a photograph of the order at that
  instant. A consumer reading it later gets what was true *then*, not what the
  order says now. If it only had the id and looked the order up, an amended or
  deleted order would mean billing the wrong amount or crashing.
- **`amount` is the string `"499.99"`, not the number `499.99`.** JSON has
  exactly one number type and it is a float; `499.99` would eventually
  round-trip as `499.99000000000001`. Money travels as text.

### Or peek without psql

```bash
curl -s 'localhost:8000/outbox?unpublished_only=true' | jq
```

---

## Step 4 — THE PROOF: break the second insert

`BREAK_OUTBOX_INSERT=1` nulls a `NOT NULL` column on the outbox row, so
**Postgres** rejects it.

> Why null a column instead of raising in Python? A Python `raise` would only
> prove that Python stopped early. Making the *database* reject the row proves
> the database also threw away the order insert that preceded it. We are
> testing Postgres's guarantee, not our own control flow.

### 4a. Record the counts before

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -tAc "select 'orders=' || count(*) from orders; select 'outbox=' || count(*) from outbox;"
```

### 4b. Turn on the sabotage

```bash
BREAK_OUTBOX_INSERT=1 docker compose up -d order
sleep 8
docker compose logs order --tail 5 | grep BREAK
```

You should see the loud warning banner.

### 4c. Send an order that must fail

```bash
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-99","item":"THIS MUST NOT PERSIST","amount":"1234.56"}'
```

Expect:

```
{"detail":"Order could not be stored; nothing was saved."}
HTTP 422
```

### 4d. Prove the ORDER row also vanished

**This is the STOP condition.**

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -tAc "select 'orders=' || count(*) from orders; select 'outbox=' || count(*) from outbox;"

docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -c "select * from orders where customer_id = 'cust-99';"
```

✅ **Pass:** counts are unchanged from 4a, and the `cust-99` query returns
`(0 rows)`.

❌ **Fail:** if `orders` went up by one, the two inserts were in *separate*
transactions — the dual-write bug, reproduced by accident.

### 4e. Watch the database do it

The counts prove the outcome; this shows the mechanism.

```bash
SQL_ECHO=1 BREAK_OUTBOX_INSERT=1 docker compose up -d order
sleep 8

curl -s -o /dev/null -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-echo","item":"watch the rollback","amount":"10.00"}'

docker compose logs order --since 30s \
  | grep -E 'BEGIN|INSERT INTO|ROLLBACK|COMMIT|null value'
```

```
BEGIN (implicit)
INSERT INTO orders (id, customer_id, item, amount, created_at) ...
INSERT INTO outbox (id, order_id, event_type, payload, ...) ...
ROLLBACK
ERROR: null value in column "event_type" of relation "outbox"
       violates not-null constraint
```

**Two INSERTs, one BEGIN, one ROLLBACK.** No `COMMIT`. That is the atomic
write, visible. Both statements were inside one transaction, so Postgres threw
away both.

Compare with the happy path, which ends `BEGIN … INSERT … INSERT … COMMIT`.

### 4f. Turn it back off and confirm recovery

```bash
docker compose up -d order
sleep 8
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-7","item":"Desk lamp","amount":"89.50"}'
```

Expect `201`. **A test that cannot fail proves nothing, and neither does one
that cannot pass** — you have now shown both directions.

---

## Step 5 — Input validation

Pydantic rejects bad input before any of our code runs, and therefore before
any transaction opens.

```bash
# amount must be > 0
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c","item":"free stuff","amount":"0"}'

# negative amount (a refund pretending to be a purchase)
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c","item":"x","amount":"-5"}'

# missing field
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' -d '{"customer_id":"c"}'

# client tries to dictate the id — silently ignored, not honoured
curl -s -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"id":"00000000-0000-0000-0000-000000000000","customer_id":"c","item":"x","amount":"1.00"}' | jq .id
```

The first three return `422`. The fourth returns `201` with a **freshly
generated** id — the client cannot set it, because `id` is not on the
`OrderCreate` form at all.

Confirm nothing leaked into the database from the rejected requests:

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -tAc "select count(*) from orders where item = 'free stuff';"     # 0
```

---

## Step 6 — Schema checks

### The partial index exists

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB \
  -tAc "select indexdef from pg_indexes where indexname = 'ix_outbox_unpublished';"
```

```sql
CREATE INDEX ix_outbox_unpublished ON public.outbox
  USING btree (created_at) WHERE (published = false)
```

Note the `WHERE`. The relay asks "any unpublished rows?" every 2 seconds
forever, against a table where published rows become the overwhelming
majority. This index only contains the pending ones, and rows **drop out** of
it the moment they are marked published — so it stays the size of your
backlog, not the size of your history. A to-do list, not a diary.

### Column types are what we intended

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB -c "\d orders"
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB -c "\d outbox"
```

Check specifically:

| Column | Must be | Why |
| --- | --- | --- |
| `orders.amount` | `numeric(12,2)` | Not float. 0.1 + 0.2 = 0.30000000000000004 in binary floating point — fine for a sensor, not for a bill. |
| `outbox.payload` | `jsonb` | Parsed once into a binary form; `json` stores raw text and re-parses on every read. |
| `*.created_at` | `timestamp with time zone` | A naive timestamp is a number with no units. |
| `outbox.order_id` | FK → `orders.id` | The database refuses a delivery note for an order that does not exist. |

### The foreign key really bites

```bash
docker compose exec -T postgres psql -U $PG_USER -d $PG_DB -c \
  "insert into outbox (id, order_id, event_type, payload, published, created_at)
   values (gen_random_uuid(), gen_random_uuid(), 'Fake', '{}'::jsonb, false, now());"
```

Expect `ERROR: insert or update on table "outbox" violates foreign key
constraint`. Orphan events are impossible by construction, not by convention.

---

## Step 7 — Log files

Logs go to the terminal **and** to `logs/<service>.log`.

```bash
ls -la logs/
tail -20 logs/order-service.log
```

The two formats differ on purpose:

| Destination | Format | Why |
| --- | --- | --- |
| Terminal | `15:25:04 INFO [order-service] …` | A windscreen. You are watching live; the date is noise. |
| File | `2026-08-07 15:25:04 INFO [order-service] order.routes (routes.py:85): …` | A flight recorder. Read days later, out of context — so it needs the date and the exact source line. |

### Correlation IDs — telling requests apart

Every request gets a short id, carried on **every** log line it produces, and
bracketed with `┌─` / `└─`:

```
[60b8bac6] request:       ┌─ POST /orders
[60b8bac6] order.service: staged order … and outbox event … (uncommitted)
[60b8bac6] order.routes:  order … committed with its outbox event
[60b8bac6] request:       └─ 201 in 21.8ms
```

Look at the two middle lines. The first is the work being staged; the second
is the transaction boundary. **In the failure case you see the first without
the second** — which is exactly what a rollback looks like from outside.

### Why an id, and not just a separator line

Separators only work while requests arrive one at a time. Fire several at
once and the lines genuinely intermix — there is no gap left to draw a line
in:

```bash
for n in 1 2 3 4; do
  curl -s -o /dev/null -X POST localhost:8000/orders \
    -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"conc-$n\",\"item\":\"Concurrent $n\",\"amount\":\"$n.00\"}" &
done; wait

tail -20 logs/order-service.log
```

```
[0a0b84de] ┌─ POST /orders
[0427c71d] ┌─ POST /orders
[a739f703] ┌─ POST /orders
[672f0e98] ┌─ POST /orders
[a739f703] staged order 3abf3e88…
[a739f703] └─ 201 in 14.7ms
[672f0e98] staged order 5be1a569…
[0427c71d] staged order 508e27f3…
```

Four stories told into one microphone. A blank-line separator would have
grouped those completely wrongly. The id survives it — pull any single
request back out:

```bash
grep '\[a739f703\]' logs/order-service.log
```

```
┌─ POST /orders
staged order 3abf3e88-16eb-42c7-a236-12670e4457a3 and outbox event 74834865… (uncommitted)
order 3abf3e88-16eb-42c7-a236-12670e4457a3 committed with its outbox event
└─ 201 in 14.7ms
```

This matters more from Phase 2 onward, when the relay and three consumers all
log at once. `correlation_scope()` is deliberately generic, so a consumer can
scope by `order_id` and a single grep will show one order's entire journey
across five services — including, for Scenario A, the duplicate publish and
all three consumers correctly declining it.

### Other things the format makes easy

```bash
# every request and its outcome
grep -E '┌─|└─' logs/order-service.log

# slowest requests
grep -oE '└─ [0-9]+ in [0-9.]+ms' logs/order-service.log | sort -t' ' -k4 -rn | head

# everything that failed
grep '└─ [45][0-9][0-9]' logs/order-service.log

# requests that started but never finished (crashes)
grep -c '┌─' logs/order-service.log; grep -c '└─' logs/order-service.log
```

### Notes

The id is returned as the **`X-Request-ID`** response header, so a client can
quote the exact id to look up:

```bash
curl -s -o /dev/null -D- -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c","item":"x","amount":"1.00"}' | grep -i x-request-id
```

Send that header **in** and it is used instead of a generated one — that is
how one id follows a request across services.

`--no-access-log` is set: our middleware already logs method, path, status and
duration with the id attached. uvicorn's own access line runs outside the
middleware and would arrive tagged `--------`.

`/health`, `/docs` and `/openapi.json` are excluded from bracketing so probes
don't bury real traffic.

Files survive `docker compose down` because `./logs` is bind-mounted from the
host. They rotate at 10 MB, keeping 4 spares (~50 MB cap per service), so a
polling loop cannot fill your disk.

```bash
# console only, no file
LOG_TO_FILE=0 docker compose up -d order

# more detail
LOG_LEVEL=DEBUG docker compose up -d order
```

---

## Step 8 — Teardown

```bash
docker compose down        # keep data
docker compose down -v     # wipe volumes; needed after any schema change,
                           # because create_all() adds missing TABLES but
                           # never alters existing ones
```

---

## Phase 1 checklist

- [ ] `POST /orders` returns `201` with a generated `id`
- [ ] Exactly one `orders` row and one `outbox` row per successful request
- [ ] `outbox.published = false`, `published_at` null (nothing sent yet)
- [ ] `payload` holds the **full** order, with `amount` as a string
- [ ] `BREAK_OUTBOX_INSERT=1` → `422`, and **`orders` count does not change**
- [ ] `SQL_ECHO=1` shows `BEGIN → INSERT → INSERT → ROLLBACK`, no `COMMIT`
- [ ] Flag off → `201` again (the test can both pass and fail)
- [ ] Invalid input rejected with `422`, nothing written
- [ ] `ix_outbox_unpublished` exists **with** its `WHERE published = false`
- [ ] Orphan outbox insert rejected by the foreign key
- [ ] `logs/order-service.log` written and survives `docker compose down`

---

## Troubleshooting

**`order` container exits immediately** — `docker compose logs order`. If it is
a Postgres connection error, check `PG_HOST: postgres` in the compose
`environment:` block. Inside a container, `localhost` means *that container*.

**`curl: (7) Failed to connect to localhost:8000`** — the container is up but
uvicorn bound to the wrong interface. It must run with `--host 0.0.0.0`;
the default `127.0.0.1` accepts only connections originating inside the
container.

**Schema change had no effect** — `create_all()` creates missing tables, it
never alters existing ones. `docker compose down -v` and start again. Alembic
migrations are the real fix.

**`permission denied` writing `logs/`** — the container's `appuser` is uid
1000; if your host user is not also uid 1000 the bind mount is not writable.
Check with `id -u`. Workaround: `LOG_TO_FILE=0`.

**Counts look wrong** — you probably skipped Step 2. Leftover rows from an
earlier run make every count-based assertion meaningless.
