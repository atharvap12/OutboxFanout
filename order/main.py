"""Order Service entrypoint.

Run locally:
    set -a; source .env; set +a
    uvicorn order.main:app --reload

Then open http://localhost:8000/docs for an interactive form to poke it with.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared import config
from shared.db import Base, engine
from shared.log import setup

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

app.include_router(router)
