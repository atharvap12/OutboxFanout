"""Notifications consumer entrypoint.

Local run:
    set -a; source .env; set +a
    python -m notifications.main
"""

from shared import config, consumer
from shared.log import setup
from shared.redis_client import client

from notifications.service import handle

log = setup("notifications")


def main() -> None:
    log.info("notifications consumer starting")

    # No create_all(): this consumer owns no tables. Its dedup store is Redis,
    # which needs no schema — a difference worth noticing, since "no migration
    # to run" is one of the real advantages of the Redis approach.
    #
    # Ping at boot so a misconfigured REDIS_URL fails here, loudly, rather than
    # on the first message that arrives an hour later.
    client().ping()
    log.info("redis reachable at %s (dedup TTL %ss)",
             config.REDIS_URL, config.NOTIFY_DEDUP_TTL_SECONDS)

    # The same loop Billing and Shipping use, unchanged. That it needed no
    # modification for a consumer with a completely different dedup store is
    # the evidence that the loop/handler split was drawn in the right place.
    consumer.run(config.NOTIFY_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
