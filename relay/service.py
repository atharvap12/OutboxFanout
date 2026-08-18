"""The relay's unit of work: claim an unpublished outbox row, publish it, mark it.

Ordering is publish-then-mark, deliberately. The alternative, mark-then-publish,
is at-most-once: a crash in the gap loses the event silently and forever,
because the row now claims to have been sent and no retry will ever fire.
Publish-then-mark is at-least-once: the same crash republishes on restart.

Exactly-once is not on the menu — there is no transaction spanning Postgres and
SNS. The whole design is choosing which side of that to land on, and a duplicate
is the recoverable side: it arrives, it is visible, and one idempotency check at
the consumer contains it.
"""

import logging
import os
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from shared import config
from shared.db import session_scope
from shared.log import get_logger

from order.models import OutboxEvent
from relay import publisher

log = get_logger(__name__)

# Distinctive exit code so the Scenario A script can assert the process died
# where we intended rather than for some unrelated reason.
CRASH_EXIT_CODE = 17


def pending_event_ids(session: Session, limit: int) -> list:
    """Ids of unpublished rows, oldest first.

    Ids rather than whole rows: the objects would be detached once this short
    session closes, and relay_one() has to re-read each row under a lock anyway.

    ORDER BY created_at does NOT buy ordered delivery — SNS standard topics and
    SQS standard queues both explicitly make no ordering guarantee, so
    consumers can see events in any order regardless of what we do here. What it
    buys is fairness: without it, a row that keeps failing could be skipped
    indefinitely while newer rows are served, and the backlog's oldest entry
    would starve.
    """
    stmt = (
        select(OutboxEvent.id)
        # `== False`, not `.is_(False)`: this renders `published = false`,
        # textually matching the partial index predicate. Postgres's proof that
        # a query is covered by a partial index is deliberately limited, so the
        # closer the expressions match the better. Confirm with EXPLAIN rather
        # than assuming — see VERIFY-PHASE-2.md.
        .where(OutboxEvent.published == False)  # noqa: E712
        .order_by(OutboxEvent.created_at)
        .limit(limit)
    )
    return list(session.scalars(stmt))


def _crash_now(event_id) -> None:
    """Scenario A: die between the publish and the mark.

    os._exit(), not sys.exit() or a raise: those unwind the stack, so
    session_scope's `except` would roll back and its `finally` would close the
    connection — a tidy, deliberate shutdown. That is not a crash, and a
    simulation that runs cleanup proves nothing about a process that never got
    the chance to. os._exit() skips finally blocks, atexit handlers and buffer
    flushing, which is what SIGKILL or a power cut actually looks like.

    logging.shutdown() first because that same skipped flushing would otherwise
    swallow the log lines explaining what happened. Flushing the record of the
    crash is instrumentation, not application cleanup — the transaction is
    still abandoned exactly as violently as intended.
    """
    log.critical(
        "CRASH_AFTER_PUBLISH — event %s is on SNS but the outbox row is still "
        "unpublished. Restart the relay: it will publish it again, and every "
        "consumer must no-op on the duplicate.",
        event_id,
    )
    logging.shutdown()
    os._exit(CRASH_EXIT_CODE)


def relay_one(event_id) -> bool:
    """Publish a single outbox row. True if we published it, False if someone
    else got there first.

    The SNS call happens INSIDE the transaction that holds the row lock, which
    is the one uncomfortable decision in this file. The cost is real: a
    transaction stays open across a network call to a third party, and with the
    boto3 config in shared/aws.py the worst case is roughly 35s of retries.

    It is worth it because the lock is what makes SKIP LOCKED useful. If the
    publish happened outside the lock, two relays could both read the row, both
    publish, and both mark it — a duplicate manufactured by our own design
    rather than by a crash. Holding the lock across the publish means the second
    relay skips the row entirely and moves to the next one.

    The costs are also bounded: one row per transaction, not the batch, and it
    is a single row-level lock — nothing else in the system wants that row.
    The alternative that removes the network call from the transaction is a
    two-phase claim (a `claimed_at` column set in its own quick transaction,
    published outside it, then marked), but that needs a reaper for rows whose
    claimer died holding them. Deliberately not built: this project runs one
    relay, and the machinery would outweigh the lesson.
    """
    with session_scope() as session:
        stmt = (
            select(OutboxEvent)
            # `published == False` is re-checked under the lock, not just in
            # pending_event_ids(). Between that lock-free read and this lock,
            # another relay may have published the row. Re-reading the
            # condition inside the critical section is what closes that window.
            .where(OutboxEvent.id == event_id, OutboxEvent.published == False)  # noqa: E712
            # SKIP LOCKED, not plain FOR UPDATE: a second relay steps over a row
            # already being worked on instead of blocking until it is free.
            # Plain FOR UPDATE would serialise every relay onto the same row and
            # make a second instance pure overhead.
            .with_for_update(skip_locked=True)
        )
        event = session.scalars(stmt).one_or_none()

        if event is None:
            # Either another relay holds the lock (SKIP LOCKED returned
            # nothing) or it already published the row. Both mean "not ours".
            log.debug("outbox row %s already claimed or published; skipping", event_id)
            return False

        message_id = publisher.publish(event)

        if config.CRASH_AFTER_PUBLISH:
            _crash_now(event.id)

        # Only reached if the publish returned without raising. Committed by
        # session_scope on the way out of this block.
        event.published = True
        event.published_at = datetime.now(timezone.utc)

        log.info(
            "published outbox row %s (%s, order %s) -> SNS MessageId %s",
            event.id,
            event.event_type,
            event.order_id,
            message_id,
        )
        return True


def relay_batch() -> tuple[int, int]:
    """One poll cycle. Returns (published, rows seen).

    Two transactions rather than one: a short lock-free read to find work, then
    a separate short transaction per row. A single batch-wide transaction would
    hold every row's lock for the duration of every publish, and — worse — mark
    all of them in one atomic UPDATE, so a crash before that commit republishes
    the entire batch. Per-row keeps the blast radius of a crash at exactly one
    duplicate.
    """
    with session_scope() as session:
        event_ids = pending_event_ids(session, config.RELAY_BATCH_SIZE)

    if not event_ids:
        return 0, 0

    published = 0
    for event_id in event_ids:
        try:
            if relay_one(event_id):
                published += 1
        except Exception:
            # Abandon the rest of the batch instead of continuing. If SNS is
            # unreachable the remaining rows fail identically, and each failure
            # burns the full boto3 retry budget first — so pressing on turns a
            # 2s poll into a minutes-long stall. The rows stay unpublished,
            # which is the whole point of the outbox; the next poll retries.
            log.exception("publish failed for outbox row %s — abandoning batch", event_id)
            break

    return published, len(event_ids)
