"""Outbox Relay entrypoint — the Polling Publisher.

A separate process from the Order Service on purpose: a slow or crashing relay
must never block order creation, and it has to be independently restartable
(Scenario A is precisely that).

Local run:
    set -a; source .env; set +a
    python -m relay.main
"""

import signal
import threading
import types

from shared import config
from shared.log import correlation_scope, get_logger, setup

from relay import publisher, service

log = setup("relay")

# Set by the signal handler, read by the loop. An Event rather than a bool
# because the poll sleep waits on it — see the loop below.
_shutdown = threading.Event()


def _handle_signal(signum: int, _frame: types.FrameType | None) -> None:
    """Ask the loop to stop after the row it is currently working on.

    Deliberately does not exit here. A signal can arrive mid-publish, and
    tearing the process down at that instant is the very failure Scenario A
    simulates on purpose — it should not also happen every time you press
    Ctrl-C. Finishing the current row means the outbox row and SNS agree.
    """
    log.info("received %s — finishing the current row, then stopping",
             signal.Signals(signum).name)
    _shutdown.set()


def main() -> None:
    # SIGTERM is what `docker compose stop` sends (SIGKILL follows if we are
    # still alive after the grace period); SIGINT is Ctrl-C.
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "relay starting: poll every %ss, batch %d, topic %r at %s",
        config.RELAY_POLL_INTERVAL_SECONDS,
        config.RELAY_BATCH_SIZE,
        config.SNS_TOPIC_NAME,
        config.AWS_ENDPOINT_URL,
    )
    if config.CRASH_AFTER_PUBLISH:
        log.warning(
            "CRASH_AFTER_PUBLISH=1 — will exit(%d) after the first publish, "
            "before marking the row (Scenario A)",
            service.CRASH_EXIT_CODE,
        )

    # Resolve (and create) the topic up front so a wrong endpoint or a dead
    # LocalStack surfaces at startup rather than on the first real order.
    # Non-fatal: the loop retries, because a relay that refuses to start when a
    # dependency is briefly down is worse than one that waits for it.
    try:
        publisher.topic_arn()
    except Exception:
        log.exception("could not reach SNS at %s — retrying in the loop",
                      config.AWS_ENDPOINT_URL)

    # Never create tables here. The Order Service owns that schema; two
    # processes racing to create_all() is a bug waiting to happen. If the
    # tables do not exist yet, the query below fails, gets logged, and retries.
    while not _shutdown.is_set():
        # One correlation id per poll cycle, so every line a cycle emits —
        # ours and SQLAlchemy's — can be grepped back out as a unit.
        with correlation_scope():
            try:
                published, seen = service.relay_batch()
                if seen:
                    log.info("poll: published %d of %d pending row(s)", published, seen)
            except Exception:
                # The loop must outlive any single failure. Postgres down,
                # tables missing, SNS unreachable — all are transient from here,
                # and the unpublished rows are still safe in the outbox.
                log.exception("poll cycle failed — continuing")

        # _shutdown.wait(), not time.sleep(): the handler sets the event, which
        # wakes this immediately. With sleep() a SIGTERM arriving just after a
        # poll would sit unacted-on for the rest of the interval, and Docker's
        # grace period is finite.
        _shutdown.wait(config.RELAY_POLL_INTERVAL_SECONDS)

    log.info("relay stopped cleanly")


if __name__ == "__main__":
    main()
