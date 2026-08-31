"""Environment variables, read and typed in one place.

Read at import so a missing or unparsable value fails at startup, not mid-loop.
Stdlib only: every service imports this, including ones with no ORM or AWS use.

Note containers never read .env — Compose uses it for ${VAR} substitution in
docker-compose.yml; Python only sees what `environment:` passes in.
"""

import os


class ConfigError(RuntimeError):
    """Configuration missing or unusable. Raised at import time."""


def _get(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(
            f"Required environment variable {name!r} is not set. "
            f"On the host: set -a; source .env; set +a"
        )
    return value


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _get_bool(name: str, default: bool) -> bool:
    """Parse a boolean. Never use bool(os.environ[...]): env values are
    strings and bool("0") is True, so an explicitly disabled flag would be on."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    normalised = raw.strip().lower()
    if normalised in ("1", "true", "yes", "on"):
        return True
    if normalised in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"{name} must be boolean-ish (1/0, true/false), got {raw!r}")


# --------------------------------------------------------------------------
# Postgres
# --------------------------------------------------------------------------

# Defaults target the host; Compose overrides PG_HOST to the service name.
PG_HOST = _get("PG_HOST", "localhost")
PG_PORT = _get_int("PG_PORT", 5432)
PG_USER = _get("PG_USER", "myuser")
PG_DB = _get("PG_DB", "mydatabase")
PG_PASSWORD = _require("PG_PASSWORD")

# "+psycopg" selects psycopg 3; bare "postgresql://" would default to psycopg2.
DATABASE_URL = _get(
    "DATABASE_URL",
    f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
)

SQL_ECHO = _get_bool("SQL_ECHO", False)


# --------------------------------------------------------------------------
# Redis (Notifications consumer)
# --------------------------------------------------------------------------

REDIS_HOST = _get("REDIS_HOST", "localhost")
REDIS_PORT = _get_int("REDIS_PORT", 6379)
REDIS_URL = _get("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")

# 48h. Encodes a tradeoff: a redelivery arriving later is treated as new.
# SQS retention defaults to 4 days, so it is possible but unlikely. The
# Postgres consumers have no equivalent expiry.
NOTIFY_DEDUP_TTL_SECONDS = _get_int("NOTIFY_DEDUP_TTL_SECONDS", 172_800)

# Phase 5 fault injection: exit between the Redis SET and the send, proving the
# key survives and the notification is genuinely lost. The mirror image of
# CRASH_AFTER_PUBLISH, which loses nothing and duplicates instead.
CRASH_AFTER_MARK = _get_bool("CRASH_AFTER_MARK", False)


# --------------------------------------------------------------------------
# AWS / LocalStack
# --------------------------------------------------------------------------

# AWS_ENDPOINT_URL is the SDKs' own standard name, so boto3 and the AWS CLI
# both honour it automatically. Unset it and the same code talks to real AWS.
AWS_ENDPOINT_URL = _get("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = _get("AWS_DEFAULT_REGION", "us-east-1")

# LocalStack ignores the values, but boto3 signs requests with SigV4 before
# sending, so absent credentials fail locally.
AWS_ACCESS_KEY_ID = _get("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = _get("AWS_SECRET_ACCESS_KEY", "test")

# Names, not ARNs/URLs: LocalStack forgets resources on restart, so
# identifiers change while names stay stable.
SNS_TOPIC_NAME = _get("SNS_TOPIC_NAME", "order-events")
BILLING_QUEUE_NAME = _get("BILLING_QUEUE_NAME", "billing-queue")
SHIPPING_QUEUE_NAME = _get("SHIPPING_QUEUE_NAME", "shipping-queue")
NOTIFY_QUEUE_NAME = _get("NOTIFY_QUEUE_NAME", "notify-queue")


# --------------------------------------------------------------------------
# Relay
# --------------------------------------------------------------------------

RELAY_POLL_INTERVAL_SECONDS = _get_int("RELAY_POLL_INTERVAL_SECONDS", 2)
RELAY_BATCH_SIZE = _get_int("RELAY_BATCH_SIZE", 10)

# Scenario A: exit after publishing to SNS but before marking the row
# published, so the row is republished on restart and consumers must no-op.
CRASH_AFTER_PUBLISH = _get_bool("CRASH_AFTER_PUBLISH", False)


# --------------------------------------------------------------------------
# Order Service fault injection
# --------------------------------------------------------------------------

# Phase 1 STOP condition. Nulls a NOT NULL column so Postgres rejects the
# outbox insert — proving it discards the orders insert too. A Python raise
# would only prove Python stopped early.
BREAK_OUTBOX_INSERT = _get_bool("BREAK_OUTBOX_INSERT", False)


# --------------------------------------------------------------------------
# SQS consumers
# --------------------------------------------------------------------------

SQS_WAIT_TIME_SECONDS = _get_int("SQS_WAIT_TIME_SECONDS", 20)   # 20 = SQS max
SQS_MAX_MESSAGES = _get_int("SQS_MAX_MESSAGES", 10)             # 10 = SQS max

# Set low (e.g. 1) to force natural redeliveries for the Scenario B duplicate
# delivery test.
SQS_VISIBILITY_TIMEOUT_SECONDS = _get_int("SQS_VISIBILITY_TIMEOUT_SECONDS", 30)

# How long an unconsumed message survives in a queue. 4 days is the SQS
# default (max 14). Note the interaction with NOTIFY_DEDUP_TTL_SECONDS above:
# retention longer than the Redis TTL means a very late redelivery could be
# treated as new. The Postgres consumers have no expiry and dedupe forever.
SQS_MESSAGE_RETENTION_SECONDS = _get_int("SQS_MESSAGE_RETENTION_SECONDS", 345_600)


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

LOG_LEVEL = _get("LOG_LEVEL", "INFO").upper()
LOG_TO_FILE = _get_bool("LOG_TO_FILE", True)
LOG_DIR = _get("LOG_DIR", "logs")

# Rotation caps disk at ~MAX_BYTES x (BACKUP_COUNT + 1) per service.
LOG_FILE_MAX_BYTES = _get_int("LOG_FILE_MAX_BYTES", 10 * 1024 * 1024)
LOG_FILE_BACKUP_COUNT = _get_int("LOG_FILE_BACKUP_COUNT", 4)
