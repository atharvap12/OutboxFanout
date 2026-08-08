"""Order Service entrypoint.

Local run:
    set -a; source .env; set +a
    uvicorn order.main:app --reload
"""

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from shared import config
from shared.db import Base, engine
from shared.log import correlation_scope, get_logger, setup

# Registers Order and OutboxEvent with Base.metadata. Looks unused; without it
# create_all() below would find no tables to create.
from order import models  # noqa: F401
from order.routes import router

log = setup("order-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup before the yield, shutdown after."""
    log.info("order service starting")
    log.info("database: %s", config.DATABASE_URL.split("@")[-1])

    # Creates missing tables only — it never ALTERS existing ones, so a schema
    # change needs `docker compose down -v`. Alembic is the real answer.
    Base.metadata.create_all(bind=engine)
    log.info("tables ready: %s", ", ".join(sorted(Base.metadata.tables)))

    if config.BREAK_OUTBOX_INSERT:
        log.warning("BREAK_OUTBOX_INSERT=1 — every POST /orders will fail (atomicity proof)")

    yield

    log.info("order service shutting down")


app = FastAPI(
    title="OutboxFanout — Order Service",
    description=(
        "Accepts orders and writes them atomically alongside an outbox event. "
        "Never talks to SNS; that is the relay's job."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

req_log = get_logger("request")

# Paths whose logs would drown out real traffic (healthchecks, docs).
_QUIET_PATHS = {"/health", "/docs", "/openapi.json", "/favicon.ico"}


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    """Wrap each request in a correlation scope and bracket it in the logs.

    Every line emitted while handling the request — ours, SQLAlchemy's — is
    tagged with the same id, so concurrent requests stay separable even when
    their lines interleave.

    An inbound X-Request-ID is honoured so a caller's id flows through; the
    id is echoed back on the response either way.
    """
    incoming = request.headers.get("X-Request-ID")
    quiet = request.url.path in _QUIET_PATHS

    with correlation_scope(incoming) as cid:
        if not quiet:
            req_log.info("┌─ %s %s", request.method, request.url.path)

        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            # Log the closing line before the exception propagates, or the
            # request that matters most is the one with no visible end.
            elapsed = (time.perf_counter() - started) * 1000
            req_log.exception("└─ UNHANDLED after %.1fms", elapsed)
            raise

        elapsed = (time.perf_counter() - started) * 1000
        if not quiet:
            req_log.info("└─ %s in %.1fms", response.status_code, elapsed)

        response.headers["X-Request-ID"] = cid
        return response


app.include_router(router)
