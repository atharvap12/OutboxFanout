"""Notifications consumer entrypoint. Wires the generic loop to Redis dedup.

Run it locally without Docker:

    set -a; source .env; set +a
    python -m notifications.main

Compare this file to billing/main.py side by side — the shape is identical, and
the two lines that differ are the whole of Phase 5's difference:

    billing/main.py         Base.metadata.create_all(bind=engine)
    notifications/main.py   client().ping()

One consumer needs a SCHEMA. The other needs a REACHABLE CACHE.
"""

from shared import config, consumer
from shared.log import setup
from shared.redis_client import client

from notifications.service import handle

log = setup("notifications")


def main() -> None:
    log.info("notifications consumer starting")

    # ------------------------------------------------------------------
    # NO create_all() HERE, AND NO MODEL IMPORT EITHER.
    #
    # Billing and Shipping each import their models module (so the table gets
    # registered) and then call create_all(). This consumer owns no tables at
    # all, so there is nothing to create and nothing to migrate.
    #
    # That is a genuine, underrated advantage of the Redis approach and worth
    # naming: NO SCHEMA MEANS NO MIGRATION. Recall the sharp edge in
    # billing/main.py — create_all() creates missing tables but never ALTERs
    # existing ones, so changing Billing's constraint silently does nothing and
    # needs `docker compose down -v`. There is no equivalent trap here.
    #
    # The flip side, of course, is that there is also no schema to STOP you:
    # nothing forces a new code path to check the key. Phase 4's constraint
    # cannot be forgotten; this convention can. That is the trade the design
    # doc's comparison table calls "purely convention".
    # ------------------------------------------------------------------

    # PING AT BOOT, ON PURPOSE.
    #
    # A wrong REDIS_URL (say REDIS_HOST=localhost inside a container, the
    # classic mistake from Phase 0) would otherwise not surface until the first
    # message arrived — possibly an hour later, in a stack trace buried in the
    # consumer loop, looking like a message-handling bug rather than a config
    # one. Failing here makes it a startup error with an obvious cause.
    #
    # Note this is NOT a healthcheck and does not make the consumer fragile: if
    # Redis is briefly down the container exits and Docker leaves it exited (we
    # deliberately set no `restart:` policy anywhere, so failures stay visible).
    client().ping()
    log.info(
        "redis reachable at %s (dedup TTL %ss = %.1fh)",
        config.REDIS_URL,
        config.NOTIFY_DEDUP_TTL_SECONDS,
        config.NOTIFY_DEDUP_TTL_SECONDS / 3600,
    )

    # ------------------------------------------------------------------
    # THE SAME LOOP BILLING AND SHIPPING RUN, IMPORTED UNCHANGED.
    #
    # This is the real test of whether Phase 4 drew the loop/handler boundary in
    # the right place — and it passed. A consumer with a completely different
    # dedup store, a different durability model, no database at all, and an
    # irreversible side effect needed ZERO changes to shared/consumer.py.
    #
    # The reason it worked: that module was written to know about queues,
    # timers, redeliveries and shutdown, and to know NOTHING about what a
    # message means. Everything Phase 5 changes is "what a message means".
    # ------------------------------------------------------------------
    consumer.run(config.NOTIFY_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
