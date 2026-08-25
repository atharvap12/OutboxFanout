"""Outbox Relay entrypoint — the clerk's working day.

This file runs when the container starts; when it finishes, the container
stops. So this one file IS the relay process.

    set -a; source .env; set +a
    python -m relay.main

`-m` runs this as a module inside a PACKAGE (a folder with an __init__.py).
It matters for imports: run it as a plain path and Python treats relay/ as the
starting point, so `from relay import publisher` fails — there is no relay/
inside relay/. With -m the starting point stays /app, where relay/, order/ and
shared/ all live.
"""

import signal
import threading
import types

from shared import config
from shared.log import correlation_scope, get_logger, setup

from relay import publisher, service

log = setup("relay")

# An Event is a flag with three operations:
#     .set()          turn it on
#     .is_set()       read it
#     .wait(seconds)  SLEEP until it turns on, or the time runs out
# That third one is the whole reason it is not a plain True/False — a boolean
# cannot wake a sleeping process. See the nap at the bottom of the loop.
_shutdown = threading.Event()


def _handle_signal(signum: int, _frame: types.FrameType | None) -> None:
    """Ask the loop to stop after the letter it is currently carrying.

    A SIGNAL is a tiny message the OS delivers to a process — no data, just a
    number meaning "something happened". Like tapping the clerk on the shoulder.

        SIGINT  (2)   you pressed Ctrl-C
        SIGTERM (15)  `docker compose stop` — "please wrap up"
        SIGKILL (9)   you ignored SIGTERM too long

    The asymmetry that matters: SIGTERM IS A REQUEST, SIGKILL IS NOT. SIGTERM
    can be caught (that is what we are doing). SIGKILL cannot be caught by
    anything, ever — a deliberate OS guarantee so any program can be stopped.
    `docker compose stop` sends SIGTERM, waits for `stop_grace_period`, then
    SIGKILLs.

    A handler does not run alongside your program: Python PAUSES the current
    line, runs this, then resumes. An interruption, not a second thread.

    WHY THIS DOESN'T EXIT: it only flips a flag. A signal can arrive right
    after publish() but before the tick-off — exiting there is exactly
    Scenario A, which we want when we ARM it, not every time someone presses
    Ctrl-C. Finishing the current row leaves the outbox and SNS agreeing.

    `_frame` describes where the program was interrupted. We don't need it; the
    leading underscore means "required to accept, deliberately ignored".
    """
    log.info("received %s — finishing the current row, then stopping",
             signal.Signals(signum).name)
    _shutdown.set()


def main() -> None:
    # Register BEFORE any work. Without this, SIGTERM's DEFAULT behaviour is
    # instant death — which could land between publish and tick-off, so every
    # ordinary `docker compose stop` would occasionally produce a duplicate
    # nobody asked for.
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "relay starting: poll every %ss, batch %d, topic %r at %s",
        config.RELAY_POLL_INTERVAL_SECONDS, config.RELAY_BATCH_SIZE,
        config.SNS_TOPIC_NAME, config.AWS_ENDPOINT_URL,
    )
    if config.CRASH_AFTER_PUBLISH:
        log.warning(
            "CRASH_AFTER_PUBLISH=1 — will exit(%d) after the first publish, "
            "BEFORE ticking the row off (Scenario A, armed on purpose)",
            service.CRASH_EXIT_CODE,
        )

    # Reach SNS once at startup so a wrong endpoint or dead LocalStack is
    # visible NOW, not three hours later on the first order. But do not die:
    # if LocalStack is five seconds from ready, a relay that refuses to boot is
    # worse than one that waits — especially with no `restart:` policy to bring
    # it back. That tension has a name (FAIL-FAST vs RESILIENCE); we resolve it
    # as "complain loudly, keep going".
    try:
        publisher.topic_arn()
    except Exception:
        log.exception("could not reach SNS at %s — retrying in the loop",
                      config.AWS_ENDPOINT_URL)

    # We never call create_all() here. The Order Service owns those tables, and
    # two processes racing to create them is a bug waiting to happen. Missing
    # tables just fail, get logged, and are retried in 2 seconds.
    while not _shutdown.is_set():
        # One correlation id per cycle, so a whole poll can be grepped out of a
        # busy log. Nothing passes it into relay_batch() — it is ambient:
        # correlation_scope stores it in a ContextVar and the logging filter in
        # shared/log.py reads it as each line is written. That is why
        # SQLAlchemy's lines get tagged too; you cannot pass it an argument.
        with correlation_scope():
            try:
                published, seen = service.relay_batch()

                # Only log when there was work. 86,400 seconds a day / 2 =
                # 43,200 polls. One line per empty poll buries the five that
                # matter. Silence is a feature.
                if seen:
                    log.info("poll: published %d of %d pending row(s)", published, seen)

            except Exception:
                # THE LOOP MUST OUTLIVE ANY SINGLE FAILURE. An uncaught
                # exception kills the process, and Postgres restarting or
                # LocalStack blinking are temporary — the unsent rows are safe
                # in the database throughout. log.exception() (not .error())
                # records the full traceback.
                log.exception("poll cycle failed — continuing")

        # .wait(), not time.sleep(). With a 30s interval and Docker's 10s grace:
        #     t=0s   poll ends, time.sleep(30)
        #     t=1s   SIGTERM; handler sets the flag... but sleep keeps sleeping
        #     t=11s  Docker gives up -> SIGKILL, dead mid-nap
        # The flag was set at t=1s; the loop never got to LOOK at it. And
        # SIGKILL does not pick a convenient moment — land it between publish
        # and commit and that is Scenario A BY ACCIDENT, on every deploy.
        # .wait() is woken by .set() immediately. Measured: 1.2s to stop.
        _shutdown.wait(config.RELAY_POLL_INTERVAL_SECONDS)

    log.info("relay stopped cleanly")


# __name__ is "__main__" when a file is RUN, or the module name when it is
# IMPORTED. So: start the loop only if someone ran this file. Without it, a
# test doing `import relay.main` would launch an infinite loop and hang.
if __name__ == "__main__":
    main()
