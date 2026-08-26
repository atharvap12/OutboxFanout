"""Single place where environment variables are read, typed, and validated.

Why this file exists when we already have a .env:
  - .env holds VALUES. This holds the RULES for reading them (name, type,
    default, required-or-not). Different jobs.
  - Inside a container nothing reads .env at all. Compose uses .env only to
    substitute ${VAR} into docker-compose.yml; the container then receives
    whatever is listed under `environment:`. Python just sees os.environ.
  - Every env var is a STRING. bool("0") is True, and time.sleep("2") raises
    TypeError. Coercion has to happen somewhere deliberate.
  - Reading happens at import time, so a missing or unparsable value kills the
    process at startup with a clear message instead of raising deep inside a
    poll loop twenty minutes later.

Stdlib only, on purpose: every service imports this, including ones that have
no use for SQLAlchemy or boto3.
"""

import os


class ConfigError(RuntimeError):
    """Raised at import time when configuration is missing or unusable."""


# --------------------------------------------------------------------------
# Readers. Each one does exactly one thing: fetch, coerce, or fail loudly.
# --------------------------------------------------------------------------

def _get(name: str, default: str) -> str:
    """Optional string with a default."""
    return os.environ.get(name, default)


def _require(name: str) -> str:
    """Mandatory string. Absence is a startup failure, not a runtime surprise."""
    value = os.environ.get(name)
    if not value:
        raise ConfigError(
            f"Required environment variable {name!r} is not set. "
            f"If running on the host, load it first:  set -a; source .env; set +a"
        )
    return value


def _get_int(name: str, default: int) -> int:
    """Integer with a default. Rejects garbage instead of failing later."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _get_bool(name: str, default: bool) -> bool:
    """Boolean with a default.

    Never use bool(os.environ[...]) for this. Env values are strings, and
    bool("0") / bool("false") are both True because the string is non-empty —
    so a flag you explicitly turned off would silently be on.
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    normalised = raw.strip().lower()
    if normalised in ("1", "true", "yes", "on"):
        return True
    if normalised in ("0", "false", "no", "off"):
        return False
    raise ConfigError(
        f"{name} must be a boolean-ish value "
        f"(1/0, true/false, yes/no, on/off), got {raw!r}"
    )


# --------------------------------------------------------------------------
# Postgres
#
# Defaults assume you are on the HOST (localhost). Compose overrides the host
# to the service name, because inside a container "localhost" means that
# container, not your laptop:
#     environment:
#       PG_HOST: postgres
# --------------------------------------------------------------------------

PG_HOST = _get("PG_HOST", "localhost")
PG_PORT = _get_int("PG_PORT", 5432)
PG_USER = _get("PG_USER", "myuser")
PG_DB = _get("PG_DB", "mydatabase")
PG_PASSWORD = _require("PG_PASSWORD")   # no sane default for a password

# "postgresql+psycopg" selects the psycopg 3 driver. Plain "postgresql://"
# would default to psycopg2, which we are not installing.
# DATABASE_URL can be set directly to override the assembled value.
DATABASE_URL = _get(
    "DATABASE_URL",
    f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
)

# Echo every SQL statement to the log. Genuinely useful in Phase 1 when you
# want to SEE that both INSERTs sit inside one BEGIN...COMMIT.
SQL_ECHO = _get_bool("SQL_ECHO", False)


# --------------------------------------------------------------------------
# Redis (Notifications consumer only)
# --------------------------------------------------------------------------

REDIS_HOST = _get("REDIS_HOST", "localhost")
REDIS_PORT = _get_int("REDIS_PORT", 6379)
REDIS_URL = _get("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")

# 48 hours, per the design doc. Note the tradeoff this encodes: a redelivery
# arriving later than this is treated as new. SQS retention defaults to 4 days
# (max 14), so it is possible — just unlikely. The Postgres consumers have no
# equivalent expiry and dedupe forever.
NOTIFY_DEDUP_TTL_SECONDS = _get_int("NOTIFY_DEDUP_TTL_SECONDS", 172_800)


# --------------------------------------------------------------------------
# AWS / LocalStack
#
# AWS_ENDPOINT_URL is the AWS SDKs' own standard variable name, so boto3 and
# the AWS CLI both honour it automatically. Using that exact name means
# `aws sqs list-queues` works with no --endpoint-url flag once it is exported.
# --------------------------------------------------------------------------

AWS_ENDPOINT_URL = _get("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = _get("AWS_DEFAULT_REGION", "us-east-1")

# LocalStack ignores the values, but boto3 signs every request with SigV4
# before sending it, and signing needs a key. Without these the request never
# leaves the machine.
AWS_ACCESS_KEY_ID = _get("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = _get("AWS_SECRET_ACCESS_KEY", "test")

# Resource names. ARNs and queue URLs are NOT hardcoded: LocalStack's free
# tier forgets every topic and queue on restart, so they get created by an
# idempotent bootstrap step and looked up by name at runtime.
SNS_TOPIC_NAME = _get("SNS_TOPIC_NAME", "order-events")
BILLING_QUEUE_NAME = _get("BILLING_QUEUE_NAME", "billing-queue")
SHIPPING_QUEUE_NAME = _get("SHIPPING_QUEUE_NAME", "shipping-queue")
NOTIFY_QUEUE_NAME = _get("NOTIFY_QUEUE_NAME", "notify-queue")


# --------------------------------------------------------------------------
# Relay behaviour
# --------------------------------------------------------------------------

RELAY_POLL_INTERVAL_SECONDS = _get_int("RELAY_POLL_INTERVAL_SECONDS", 2)
RELAY_BATCH_SIZE = _get_int("RELAY_BATCH_SIZE", 10)

# Fault injection for Scenario A ("the single most important proof in the
# whole project"): exit immediately after publishing to SNS but BEFORE marking
# the outbox row published. On restart the row looks unpublished, gets
# published a second time, and all three consumers must no-op on the
# duplicate. Default off — this deliberately breaks the relay.
CRASH_AFTER_PUBLISH = _get_bool("CRASH_AFTER_PUBLISH", False)


# --------------------------------------------------------------------------
# Order Service fault injection (Phase 1 STOP condition)
#
# Sabotages the SECOND insert on purpose, so we can prove the FIRST one also
# disappears. Think of a two-item order at a till: if the card is declined on
# the second item, you don't get to keep the first one for free — the whole
# sale is voided.
#
# It breaks the outbox row by nulling a NOT NULL column, so the database
# itself rejects it. That matters: we want proof that POSTGRES rolled the
# order back, not merely that Python skipped a line of code.
# --------------------------------------------------------------------------

BREAK_OUTBOX_INSERT = _get_bool("BREAK_OUTBOX_INSERT", False)


# --------------------------------------------------------------------------
# Consumer behaviour (SQS polling)
# --------------------------------------------------------------------------

# Long polling. 20s is the SQS maximum and the sane default: the receive call
# waits for a message instead of returning empty immediately, which means far
# fewer API calls and much lower latency than a sleep-and-retry loop.
SQS_WAIT_TIME_SECONDS = _get_int("SQS_WAIT_TIME_SECONDS", 20)
SQS_MAX_MESSAGES = _get_int("SQS_MAX_MESSAGES", 10)   # 10 is the SQS maximum

# How long a received message stays hidden from other consumers while we work
# on it. Too short and SQS redelivers while we are still processing (a useful
# way to force duplicates in Scenario B, but not what you want by default).
SQS_VISIBILITY_TIMEOUT_SECONDS = _get_int("SQS_VISIBILITY_TIMEOUT_SECONDS", 30)

# How long an unread message survives in a queue before SQS throws it away.
# 4 days is the AWS default (14 is the maximum).
#
# Worth noticing how this interacts with NOTIFY_DEDUP_TTL_SECONDS above: the
# Redis dedup key expires after 48h, but a message can sit in a queue for 4
# days. A redelivery arriving after the key expired would look like a brand new
# event, and the customer would get a second email. Unlikely, but real — and
# the Postgres consumers have no such window, because their dedup rows never
# expire.
SQS_MESSAGE_RETENTION_SECONDS = _get_int("SQS_MESSAGE_RETENTION_SECONDS", 345_600)


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

LOG_LEVEL = _get("LOG_LEVEL", "INFO").upper()

# Write logs to a file as well as the terminal.
#
# Why both? The terminal is a windscreen — great for what is happening right
# now, useless once it has scrolled past. The file is a flight recorder: still
# there tomorrow, greppable, and survives the container being destroyed.
# That matters a lot from Phase 6 onward, when the proof of a fault-injection
# scenario is a log line you need to go back and read.
LOG_TO_FILE = _get_bool("LOG_TO_FILE", True)

# One file per service: logs/order-service.log, logs/relay.log, ...
# Mounted from the host in docker-compose.yml so the files outlive containers.
LOG_DIR = _get("LOG_DIR", "logs")

# Log rotation. Without it a chatty poll loop fills your disk — slowly, and
# then all at once, usually at the worst moment.
#
# Think of it as a stack of notebooks. When the current one hits MAX_BYTES it
# is set aside as .log.1, a fresh one starts, and the oldest is thrown away
# once there are BACKUP_COUNT spares. Disk usage is capped at roughly
# MAX_BYTES x (BACKUP_COUNT + 1) — here, about 50 MB per service.
LOG_FILE_MAX_BYTES = _get_int("LOG_FILE_MAX_BYTES", 10 * 1024 * 1024)  # 10 MB
LOG_FILE_BACKUP_COUNT = _get_int("LOG_FILE_BACKUP_COUNT", 4)
