# Verifying a Local Docker Stack — Reusable Checklist

A generic, copy-paste checklist for proving that a Docker Compose stack of
backing services (Postgres / Redis / LocalStack) is **actually working**, not
just "started".

Written while doing Phase 0 of OutboxFanout, but deliberately parameterised so
it can be reused on any project. Adapt the variables in Step 0 and the rest
should work unchanged.

**The rule this whole file exists to enforce:** `Container started` in the logs
is not evidence. A container can be *running* while the service inside it is
still booting, misconfigured, or writing to the wrong directory. Run one real
command against each service, every time.

---

## Step 0 — Adapt these to your project

Everything below reads from these variables. Define them once, in a `.env` file
next to `docker-compose.yml`:

```bash
# ---- .env ----------------------------------------------------------------

# Service names, exactly as written in docker-compose.yml
PG_SVC=postgres
REDIS_SVC=redis
LS_SVC=localstack

# Postgres — must match the POSTGRES_* values the container was created with
PG_USER=myuser
PG_DB=mydatabase

# LocalStack: the single "edge port" that fronts every emulated AWS API
AWS_ENDPOINT_URL=http://localhost:4566

# Dummy AWS credentials. LocalStack ignores the VALUES, but the AWS CLI and
# boto3 refuse to send a request without credentials present — without these
# you get "Unable to locate credentials" before a single byte reaches
# LocalStack. Any non-empty string works.
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
```

### Loading it — the part that catches everyone

**A `.env` file does not put variables into your shell.**

Docker Compose auto-loads `.env` from the project directory, but *only* to
substitute `${VAR}` placeholders **inside `docker-compose.yml`**. Your terminal
never sees them. So `docker compose up` would work, while
`aws --endpoint-url $AWS_ENDPOINT_URL ...` typed at the prompt silently gets an
empty string.

Three separate mechanisms, commonly confused:

| Mechanism | What it actually does |
| --- | --- |
| `.env` in the project root | Substitutes `${VAR}` **in the compose file itself** |
| `env_file:` on a service | Injects variables **into that container** |
| `environment:` on a service | Same as above, written inline |

None of them touch your shell. Load it explicitly, once per terminal:

```bash
cd /path/to/your/project      # directory containing docker-compose.yml
set -a                        # auto-export every variable assigned from here on
source .env
set +a                        # stop auto-exporting

# sanity check
echo "$AWS_ENDPOINT_URL"          # should print http://localhost:4566
```

> `set -a` exists because `source .env` alone assigns the variables as *shell*
> variables, not *environment* variables — visible to your prompt but not
> inherited by the programs you run. `set -a` makes every assignment an
> implicit `export`. Turning it back off with `set +a` keeps the effect scoped
> to just this file.

To load it automatically on `cd`, use [direnv](https://direnv.net/): put
`dotenv` in a `.envrc`, run `direnv allow`, and it sources and unloads as you
enter and leave the directory.

### Keep `.env` out of version control

```bash
echo ".env" >> .gitignore
```

Commit a `.env.example` with the same keys and placeholder values instead, so
someone cloning the repo knows what to fill in. Do this even when the values
are throwaway (`test`/`test` here) — the habit is what matters, because the
same file grows real credentials on the next project.

> **Bonus: a single source of truth.** Once `POSTGRES_USER`, `POSTGRES_PASSWORD`
> and `POSTGRES_DB` live in `.env`, `docker-compose.yml` can reference them as
> `${POSTGRES_USER}` etc. Then the credentials in your compose file and in your
> verification commands can never drift apart — which otherwise produces a
> baffling `password authentication failed` after you edit one and forget the
> other.

> Reading the endpoint from a variable instead of hardcoding it is also what
> lets the *same application code* run against LocalStack locally and real AWS
> in production. Do this from day one.

---

## Step 1 — Bring the stack up and confirm health

```bash
docker compose up -d
docker compose ps
```

Wait until every service reports `(healthy)`, not merely `Up`. Different
services boot at very different speeds — Postgres and Redis are ready in a few
seconds, LocalStack typically takes 20-30.

```bash
# Watch until they settle, instead of guessing
watch -n 2 'docker compose ps --format "table {{.Service}}\t{{.Status}}"'
```

If something is stuck, read the last five probe results — this tells you *why*
a healthcheck is failing, which is almost impossible to guess:

```bash
docker inspect --format '{{json .State.Health}}' <container_name> | jq
```

> A service with no `healthcheck:` in compose shows only `Up` and can never be
> depended on with `condition: service_healthy`. If a service has no
> healthcheck, that is a gap in your compose file, not a detail.

---

## Step 2 — Postgres

Connecting is not enough. Prove you can **write**, since that is what your
application will do.

```bash
# 1. Create a throwaway table
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB \
  -c "CREATE TABLE throwaway (id INT, note TEXT);"

# 2. Write a row
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB \
  -c "INSERT INTO throwaway VALUES (1, 'verification works');"

# 3. Read it back
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB \
  -c "SELECT * FROM throwaway;"

# 4. List tables
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB -c "\dt"

# 5. Clean up
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB \
  -c "DROP TABLE throwaway;"
```

Expect: `CREATE TABLE`, `INSERT 0 1`, one row, the table listed, `DROP TABLE`.

Interactive session instead (`\q` to quit, `\dt` tables, `\d <table>` schema):

```bash
docker compose exec -it $PG_SVC psql -U $PG_USER -d $PG_DB
```

> **Why `docker compose exec` rather than a host `psql`?** The client already
> ships inside the official image, so there is no version skew between client
> and server, and no machine-specific install step. Anyone cloning the repo
> needs Docker and nothing else.

---

## Step 3 — Redis

```bash
docker compose exec $REDIS_SVC redis-cli PING          # -> PONG
```

If the project uses Redis for **idempotency / deduplication**, verify the
atomic check-and-set too, because that is the behaviour you actually depend on:

```bash
docker compose exec $REDIS_SVC redis-cli SET probe:key 1 NX EX 60   # -> OK
docker compose exec $REDIS_SVC redis-cli SET probe:key 1 NX EX 60   # -> (nil)  <- duplicate rejected
docker compose exec $REDIS_SVC redis-cli TTL probe:key              # -> seconds left
docker compose exec $REDIS_SVC redis-cli DEL probe:key
```

> `NX` = "only set if it does not already exist". The **return value is the
> answer**: `OK` means you claimed it (new work), `nil` means someone already
> did (duplicate). One atomic command — never `EXISTS` followed by `SET`, which
> leaves a race window where two workers both see "absent" and both process.

Useful extras:

```bash
docker compose exec $REDIS_SVC redis-cli INFO persistence | grep -E 'aof_enabled|rdb_last_bgsave_status'
docker compose exec $REDIS_SVC redis-cli DBSIZE       # number of keys
docker compose exec $REDIS_SVC redis-cli KEYS '*'     # dev only — O(n), never in prod
```

---

## Step 4 — LocalStack

Two separate checks. The health endpoint alone is not sufficient.

### 4a. Health endpoint

```bash
curl -s $AWS_ENDPOINT_URL/_localstack/health | jq '.services'

# Or just the services you care about:
curl -s $AWS_ENDPOINT_URL/_localstack/health | jq '.services | {sns, sqs}'
```

> This only proves LocalStack's front door is answering. Services load lazily,
> so `available` does not guarantee a real API call will succeed. Hence 4b.

### 4b. A real API call through the endpoint override

This is the check that matters — it proves the endpoint override **and**
credential handling work before any application code depends on them.

```bash
aws --endpoint-url $AWS_ENDPOINT_URL sqs list-queues
echo "exit code: $?"
```

Exit code `0` with empty output is a **pass** (there are simply no queues yet).

Empty output is unsatisfying, so create a throwaway resource — the AWS
equivalent of the throwaway table:

```bash
# Create, list, delete
QURL=$(aws --endpoint-url $AWS_ENDPOINT_URL sqs create-queue \
  --queue-name verify-throwaway --query QueueUrl --output text)
echo "$QURL"

aws --endpoint-url $AWS_ENDPOINT_URL sqs list-queues
aws --endpoint-url $AWS_ENDPOINT_URL sqs delete-queue --queue-url "$QURL"
```

Same for SNS:

```bash
TARN=$(aws --endpoint-url $AWS_ENDPOINT_URL sns create-topic \
  --name verify-throwaway-topic --query TopicArn --output text)
aws --endpoint-url $AWS_ENDPOINT_URL sns list-topics
aws --endpoint-url $AWS_ENDPOINT_URL sns delete-topic --topic-arn "$TARN"
```

### Optional: the `awslocal` shortcut

```bash
pip install awscli-local
awslocal sqs list-queues        # supplies --endpoint-url and dummy creds for you
```

Convenient, but learn the explicit form first so you know what it hides.

---

## Step 5 — Prove what actually persists

Most valuable step, and the one everyone skips. **Do not assume a volume means
your data survives.** Plant a marker in each service, restart, and look.

```bash
# --- plant markers ---
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB -c "CREATE TABLE survivor (id INT);"
docker compose exec $REDIS_SVC redis-cli SET survivor 1
aws --endpoint-url $AWS_ENDPOINT_URL sns create-topic --name survivor-topic

# --- restart WITHOUT -v (using -v would delete the volumes and prove nothing) ---
docker compose down
docker compose up -d
```

Wait for all services healthy, then check. **Predict each result before
running it** — a wrong prediction is the whole point of the exercise:

```bash
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB -c "\dt"   # survivor table?
docker compose exec $REDIS_SVC redis-cli GET survivor             # value 1?
aws --endpoint-url $AWS_ENDPOINT_URL sns list-topics                  # survivor-topic?
```

Cleanup:

```bash
docker compose exec $PG_SVC psql -U $PG_USER -d $PG_DB -c "DROP TABLE survivor;"
docker compose exec $REDIS_SVC redis-cli DEL survivor
```

### Results observed on OutboxFanout (Postgres 16 / Redis 7 AOF / LocalStack 3 Community)

| Service | Survives `down` + `up`? | Why |
| --- | --- | --- |
| Postgres | **Yes** | Named volume mounted at the *correct* `PGDATA` path |
| Redis | **Yes** | Named volume + `--appendonly yes` (AOF replayed at startup) |
| LocalStack | **No** | Resource persistence needs `PERSISTENCE=1`, a **paid** feature. The free version rebuilds empty every boot. |

**Consequence to design around:** if your emulated cloud resources vanish on
restart, creating them can never be a one-off manual command. It must be an
**idempotent bootstrap script** you can safely re-run any number of times —
which is how real infrastructure is provisioned anyway.

---

## Step 6 — Teardown

```bash
docker compose down       # stop + remove containers, KEEP volumes (data survives)
docker compose down -v    # ...and DELETE volumes (full reset)
```

- **`down`** = closing the shop for the night. Stock stays.
- **`down -v`** = also emptying the warehouse.

Use `-v` **before** a test that asserts "exactly one row exists" — leftover data
from an earlier run makes such a test meaningless. Never use `-v` *in the middle*
of an investigation or a demo; it destroys the evidence you were about to
inspect.

`down -v` is also the only way to re-run Postgres init scripts (see
troubleshooting below).

---

## Troubleshooting — real problems hit while writing this

### `permission denied ... unix:///var/run/docker.sock`

The socket is `srw-rw---- root:docker`. If you are neither root nor in the
`docker` group, you fall into "others", which has no access. The Docker CLI is
an ordinary HTTP client over that socket — nothing special about it.

```bash
sudo usermod -aG docker $USER     # -a means APPEND; without it you wipe your other groups
```

Then **log out and back in.** A new terminal tab is not enough: group
membership is part of a process's kernel credentials, fixed at login and
inherited via `fork()`. Nothing re-reads `/etc/group` afterwards.

```bash
id -Gn | grep -o docker           # verify in the NEW session
sg docker -c "docker ps"          # one-shot workaround without re-login
```

Note this grants effectively passwordless root (you can start a container that
mounts `/`). Fine on a personal laptop; know that you chose it.

### `error getting credentials — docker-credential-desktop: not found`

Left behind by uninstalling Docker Desktop. Remove the stale line from
`~/.docker/config.json`:

```json
"credsStore": "desktop",
```

Blocks every `docker pull` until fixed.

### `docker-compose: command not found`

You want `docker compose` (**space**), the v2+ Go plugin. The hyphenated
`docker-compose` is v1, Python, end-of-life since July 2023. Do **not**
`apt install docker-compose` — that installs the dead version.

### Data disappears after `docker compose down` + `up`

Your volume is mounted at a path the service doesn't actually use, so it sits
over an empty directory. No error is ever printed.

Real example: `postgres:latest` is now 18, which moved `PGDATA` from
`/var/lib/postgresql/data` to `/var/lib/postgresql/18/docker`. A volume mounted
at the old path silently holds nothing.

```bash
# Check where the image really writes
docker image inspect postgres:16 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep PGDATA
```

**Pin major versions. Never use `:latest`.**

### Changed `POSTGRES_PASSWORD` / init SQL, nothing happened

`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, and scripts in
`/docker-entrypoint-initdb.d` are honoured **only when the data directory is
empty** — i.e. first run only. Use `docker compose down -v` to force a re-init.

### Healthcheck never passes

- **Is the command present in the image?** `["CMD", "curl", ...]` fails forever
  if the image has no `curl`. Check: `docker run --rm --entrypoint sh <image> -c 'command -v curl'`
- **Missing `start_period`?** Slow-booting services (LocalStack ~30s) exhaust
  the retry budget during normal startup. During `start_period`, failures do
  **not** count toward `retries`.
- **`pg_isready` needs `-U` and `-d`.** Without them it connects as the OS user
  (root) to a database named `root`, spamming `FATAL: role "root" does not
  exist` while still reporting healthy.

### A container connects to `localhost` and fails

Inside a container, `localhost` is *that container*, not your laptop. Compose
puts services on a shared network where they reach each other by **service
name**:

| From | Address of Postgres |
| --- | --- |
| Your laptop | `localhost:5432` |
| Another container | `postgres:5432` |

Decide early whether your app runs on the host or in Compose, and be consistent.
Mixing the two is the most common source of "why can't it connect?".

---

## Quick reference

```bash
docker compose config              # validate + print the resolved file
docker compose config --quiet      # validate only, silent on success
docker compose up -d               # start detached
docker compose ps                  # status incl. health
docker compose logs -f <svc>       # follow logs
docker compose exec <svc> sh       # shell inside a running container
docker compose restart <svc>       # restart one service
docker compose stop <svc>          # stop one service (useful for fault injection)
docker compose down                # remove containers, keep volumes
docker compose down -v             # remove containers AND volumes
docker volume ls                   # list volumes
docker inspect --format '{{json .State.Health}}' <container> | jq
```

---

## References

- Docker Compose file reference — https://docs.docker.com/reference/compose-file/services/
- Docker post-install (Linux, `docker` group) — https://docs.docker.com/engine/install/linux-postinstall/
- Official Postgres image — https://hub.docker.com/_/postgres
- Official Redis image — https://hub.docker.com/_/redis
- Redis `SET` (NX, EX) — https://redis.io/docs/latest/commands/set/
- Redis persistence (RDB vs AOF) — https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- LocalStack installation — https://docs.localstack.cloud/aws/getting-started/installation/
- LocalStack + AWS CLI — https://docs.localstack.cloud/aws/integrations/aws-native-tools/aws-cli/
- LocalStack persistence (paid) — https://docs.localstack.cloud/aws/capabilities/state-management/persistence/
- AWS CLI environment variables — https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
