"""Notifications consumer entrypoint.

Local run:
    set -a; source .env; set +a
    python -m notifications.main
"""

import time

from shared import config, consumer
from shared.log import setup
from shared.redis_client import client

from notifications.service import handle

log = setup("notifications")

# Boot-time Redis probe: bounded, and NON-FATAL when it runs out of attempts.
_PING_ATTEMPTS = 5
_PING_BACKOFF_SECONDS = 2


def _probe_redis() -> bool:
    """Log Redis reachability at startup. Never fatal.

    Two competing goals. A wrong REDIS_URL should be obvious immediately, not
    on the first message an hour later — so we probe. But a process that
    refuses to start because a dependency blinked is worse than one that waits,
    which is the rule relay/main.py already follows.

    Learned the hard way in Phase 6: the first version called ping() once and
    let the exception kill the process. With no `restart:` policy anywhere
    (deliberate, so failures stay visible), one DNS hiccup while Redis was
    being recreated stopped this consumer permanently — `depends_on:
    service_healthy` does not protect you from a container restarting
    underneath you.

    So: retry a few times, then hand over to the consumer loop regardless. The
    loop treats a handler failure as "not processed", leaves the message on the
    queue and retries — so no work is lost either way, and the operator gets a
    loud log line instead of a dead container.
    """
    for attempt in range(1, _PING_ATTEMPTS + 1):
        try:
            client().ping()
            log.info(
                "redis reachable at %s (dedup TTL %ss)",
                config.REDIS_URL, config.NOTIFY_DEDUP_TTL_SECONDS,
            )
            return True
        except Exception as exc:
            log.warning(
                "redis not reachable at %s (attempt %d/%d): %s",
                config.REDIS_URL, attempt, _PING_ATTEMPTS, exc,
            )
            if attempt < _PING_ATTEMPTS:
                time.sleep(_PING_BACKOFF_SECONDS)

    log.error(
        "redis still unreachable at %s after %d attempts — starting anyway; "
        "messages will be retried until it returns",
        config.REDIS_URL, _PING_ATTEMPTS,
    )
    return False


def main() -> None:
    log.info("notifications consumer starting")

    # No create_all(): this consumer owns no tables. Its dedup store is Redis,
    # which needs no schema — a difference worth noticing, since "no migration
    # to run" is one of the real advantages of the Redis approach.
    _probe_redis()

    # The same loop Billing and Shipping use, unchanged. That it needed no
    # modification for a consumer with a completely different dedup store is
    # the evidence that the loop/handler split was drawn in the right place.
    consumer.run(config.NOTIFY_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
