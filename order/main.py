"""Order Service entrypoint.

Run locally:
    set -a; source .env; set +a
    uvicorn order.main:app --reload

Then open http://localhost:8000/docs for an interactive form to poke it with.
"""

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from shared import config
from shared.db import Base, engine
from shared.log import correlation_scope, get_logger, setup

# Importing models registers Order and OutboxEvent with Base.metadata.
# Without this import the classes are never defined, so create_all below
# would find nothing to create. It looks like an unused import; it is not.
from order import models  # noqa: F401
from order.routes import router

log = setup("order-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Runs once on startup, and once on shutdown.

    Same yield-splits-a-function idea as session_scope(): everything before
    the yield happens at boot, everything after happens on shutdown.
    """
    log.info("order service starting")
    log.info("database: %s", config.DATABASE_URL.split("@")[-1])

    # Create any table that does not exist yet.
    #
    # create_all is idempotent: it looks first and skips what is already
    # there. But be clear about its limit — it CREATES missing tables, it
    # never ALTERS existing ones. Add a column to models.py and this will
    # silently do nothing, because the table already exists.
    #
    # For this project that is fine: `docker compose down -v` wipes the
    # volume and you start clean. A real system uses migrations (Alembic),
    # which record each change as a versioned step. create_all is a first
    # draft; migrations are version control for your schema.
    Base.metadata.create_all(bind=engine)
    log.info("tables ready: %s", ", ".join(sorted(Base.metadata.tables)))

    if config.BREAK_OUTBOX_INSERT:
        log.warning("=" * 62)
        log.warning("BREAK_OUTBOX_INSERT=1 — every POST /orders will FAIL.")
        log.warning("This is the Phase 1 atomicity proof. Unset it to go back.")
        log.warning("=" * 62)

    yield

    log.info("order service shutting down")


app = FastAPI(
    title="OutboxFanout — Order Service",
    description=(
        "Accepts orders and writes them atomically alongside an outbox event. "
        "Never talks to SNS: that is the relay's job, and the whole point."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Request correlation middleware.
#
# "Middleware" means code that wraps EVERY request: it runs before your
# endpoint, then again after it. Like a receptionist who logs you in on the
# way past, sends you to the right office, and logs you out on the way back.
#
# This one exists so the log is readable when more than one request is in
# flight at once. It does two things:
#
#   1. Opens a correlation scope, so every line produced while handling this
#      request — ours, SQLAlchemy's, anything — carries the same short id.
#   2. Brackets the request with ┌─ and └─ lines, so a single request reads
#      as one visual block when requests happen to arrive one at a time.
#
# The brackets are the nice-to-have; the id is the part that actually works
# under load, because interleaved lines cannot be separated by whitespace.
# ---------------------------------------------------------------------------

req_log = get_logger("request")

# Paths we do NOT bracket. Healthchecks fire every few seconds and docs pages
# are noisy; left in, they would bury the requests you actually care about.
# The correlation scope still applies — we just skip the two extra lines.
_QUIET_PATHS = {"/health", "/docs", "/openapi.json", "/favicon.ico"}


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    """Wrap each request in a correlation scope and bracket it in the logs.

    `call_next` is "run the rest of the application and give me the response".
    Everything before that line happens on the way in; everything after
    happens on the way out.

    An inbound X-Request-ID header is honoured rather than overwritten. That
    is how one id follows a request across service boundaries: a caller
    generates it, passes it along, and every service logs under the same tag.
    We echo it back on the response either way, so a client hitting a problem
    can tell you the exact id to grep for.
    """
    incoming = request.headers.get("X-Request-ID")
    quiet = request.url.path in _QUIET_PATHS

    with correlation_scope(incoming) as cid:
        if not quiet:
            req_log.info("┌─ %s %s", request.method, request.url.path)

        # perf_counter, not time.time(). time.time() is wall-clock and can
        # jump backwards if the system clock is adjusted, which would give you
        # a negative duration. perf_counter only ever moves forward and exists
        # precisely for measuring elapsed time.
        started = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            # Log the closing line BEFORE re-raising.
            #
            # Without this, an unhandled crash produces a ┌─ with no matching
            # └─, and the one request you most need to understand is the one
            # that looks truncated. .exception() also records the traceback.
            elapsed = (time.perf_counter() - started) * 1000
            req_log.exception("└─ UNHANDLED after %.1fms", elapsed)
            raise

        elapsed = (time.perf_counter() - started) * 1000
        if not quiet:
            req_log.info("└─ %s in %.1fms", response.status_code, elapsed)

        # Hand the id back to the caller so they can quote it in a bug report.
        response.headers["X-Request-ID"] = cid
        return response


app.include_router(router)
