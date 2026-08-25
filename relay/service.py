"""The relay's actual work: claim a letter, post it, tick it off.

THE ORDER OF THE LAST TWO STEPS IS THE WHOLE DESIGN.

There are only two orderings and neither is safe — a transaction cannot span
Postgres AND SNS, so you must choose which way it breaks.

    TICK OFF, THEN POST  ("at-most-once")
        Crash in the gap and the letter is never sent, but the ledger says it
        was. Nothing looks unsent, so no retry ever fires. Gone, silently.

    POST, THEN TICK OFF  ("at-least-once")   <-- WHAT WE DO
        Crash in the gap and the letter goes twice: once before, once after
        the restart, because the row still looks unsent.

We choose duplicates, and not merely because our consumers are idempotent
(true but circular). The two failures are not equally bad:

    A LOST EVENT IS SILENT AND UNRECOVERABLE. Nothing records it should have
    existed. Noticing requires a SECOND source of truth built on purpose — a
    reconciliation job, an angry customer, accounts that don't balance.

    A DUPLICATE IS LOUD AND LOCALLY FIXABLE. It arrives, you can see it, and
    one check at the receiving end contains it using an id you already have.

So the outbox pattern does not remove the dual-write problem. It CONVERTS AN
UNRECOVERABLE FAILURE INTO A RECOVERABLE ONE — which only pays off because
Phases 4-6 make duplicates harmless.
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

# Unusual on purpose: 0 means success and 1 is the generic failure everything
# uses, so 17 lets a test assert "died exactly where we aimed it".
CRASH_EXIT_CODE = 17


def pending_event_ids(session: Session, limit: int) -> list:
    """Ids of unsent letters, oldest first.

    SQLAlchemy translates Python into SQL. This becomes:

        SELECT id FROM outbox WHERE published = false
        ORDER BY created_at LIMIT 10;
    """
    stmt = (
        # (a) IDS ONLY. A SQLAlchemy object is tied to the session that loaded
        #     it; once that closes the object is "detached" and touching it can
        #     raise. This session closes in a moment. We also re-read each row
        #     under a lock below, so full rows would be thrown away anyway.
        select(OutboxEvent.id)

        # (b) `== False`, NOT `.is_(False)`. Looks like bad style, and a linter
        #     flags it (E712) — `# noqa` says we mean it. We are generating SQL,
        #     and the two spellings differ:
        #
        #         .published == False    ->  published = false
        #         .published.is_(False)  ->  published IS false
        #
        #     Identical to a human, not to Postgres. Our index is PARTIAL — it
        #     holds only rows where published = false. Before using it Postgres
        #     must be sure the query wants only rows inside it, or it would
        #     silently miss rows. It checks by comparing the conditions as
        #     PATTERNS, not by reasoning about meaning:
        #
        #         index:      published = false
        #         you wrote:  published = false    -> match, use the index
        #         you wrote:  published IS false   -> different, don't risk it
        #
        #     A pattern-matcher, not a logician. Measured here on 50k rows:
        #     0.134 ms (index scan) vs 18.054 ms (seq scan + sort). 135x, from
        #     a spelling difference.
        #
        #     LESSON: an index that EXISTS is not an index that is USED. Check
        #     with EXPLAIN (ANALYZE), always for a partial index.
        #     https://www.postgresql.org/docs/current/indexes-partial.html
        .where(OutboxEvent.published == False)  # noqa: E712

        # (c) OLDEST FIRST — but not for ordering. SNS standard topics and SQS
        #     standard queues both promise NO ordering, so consumers see events
        #     in any sequence regardless. This is about STARVATION: without it,
        #     "any 10 rows" lets Postgres keep returning the cheapest ones while
        #     an older failing row is never looked at again.
        .order_by(OutboxEvent.created_at)

        # (d) A BITE, NOT THE PLATE. After an overnight outage there could be
        #     50,000 rows; loading all of them spikes memory and holds one huge
        #     transaction open.
        .limit(limit)
    )
    return list(session.scalars(stmt))


def _crash_now(event_id) -> None:
    """Die in the gap between posting and ticking off (Scenario A).

    THREE WAYS TO END A PYTHON PROGRAM, AND THEY DIFFER:

        raise / sys.exit()   unwind the stack — every `finally` still runs
        os._exit()           stop the process NOW; no finally, no flushing

    session_scope has `finally: session.close()`. With raise or sys.exit those
    RUN, so the transaction is politely rolled back — a SHUTDOWN, not a CRASH.
    It is the difference between resigning with notice and being hit by a bus:
    the end state can look similar, but only one tells you how the company
    copes. os._exit() is the bus — what SIGKILL or a power cut actually is.

    Same standard as Phase 1's BREAK_OUTBOX_INSERT, where we let POSTGRES
    reject the row instead of raising in Python:
    MAKE THE REAL MECHANISM FAIL, NOT YOUR OWN CONTROL FLOW.

    logging.shutdown() first because log output is buffered and os._exit()
    would discard it, losing the line that explains the crash. Flushing the
    RECORD is instrumentation — a black box surviving the plane. The
    transaction is still abandoned just as violently.
    """
    log.critical(
        "CRASH_AFTER_PUBLISH — event %s is already on SNS but its outbox row is "
        "still unpublished. Restart the relay: it will send the event again, and "
        "every consumer must ignore the duplicate.",
        event_id,
    )
    logging.shutdown()
    os._exit(CRASH_EXIT_CODE)


def relay_one(event_id) -> bool:
    """Publish one row. True if we sent it, False if someone else got there first.

    WHAT A LOCK IS. Two clerks share one out-tray. Both see letter #7, both
    pick it up, both post it — the customer gets two copies. That is a RACE
    CONDITION. A lock is the rule "put your hand on it first; if a hand is
    already there, you can't". Postgres spells it `SELECT ... FOR UPDATE`.

    WHY `SKIP LOCKED`. Plain FOR UPDATE makes clerk B *wait* for A — safe but
    useless, since B idles while A works and both read the same oldest-first
    list, leaving B permanently one step behind. SKIP LOCKED means "if someone
    has it, hand me nothing and I'll find other work". Measured: 30 rows, two
    relays, split 11/20, nothing sent twice.

    "Skipped" is NOT "dropped" — the row stays unpublished. Either the holder
    finishes in milliseconds, or it dies and the lock dies with its connection,
    and the row is picked up next poll.
    https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE

    WHY THE SNS CALL IS INSIDE THE TRANSACTION — the one uncomfortable choice.
    A LOCK LASTS EXACTLY AS LONG AS ITS TRANSACTION; there is no "release this
    row" command, only COMMIT or ROLLBACK. So "hold the lock while publishing"
    and "keep the transaction open while publishing" are the same single thing.
    The cost is real: a transaction open across a third-party network call,
    worst case ~35s with our retry config. We accept it because THE LOCK IS THE
    ONLY THING PREVENTING A DUPLICATE — publish outside it and two relays both
    claim, both send, both tick off. Bounded to one row-level lock at a time.

    THE ALTERNATIVE when this stops being enough: a TWO-PHASE CLAIM. Add
    `claimed_at`, set it in a quick transaction, publish with nothing open,
    tick off in a second. Others skip on `claimed_at IS NOT NULL` rather than a
    lock. The catch: a lock cleans itself up when a process dies, a COLUMN DOES
    NOT — so a relay dying mid-publish claims that row forever, and you need a
    reaper job un-claiming rows older than N minutes. More machinery than this
    project's lesson is worth.
    """
    with session_scope() as session:
        stmt = (
            select(OutboxEvent)
            # THE RE-CHECK — the line most people leave out.
            #
            # We already filtered on published == False in pending_event_ids.
            # But THAT READ TOOK NO LOCK:
            #
            #   .000  Relay A reads the list: #7 unsent      (no lock)
            #   .001  Relay B reads the list: #7 unsent      (no lock)
            #   .002  Relay B locks #7, posts, ticks off, COMMITS
            #         -- committing RELEASES the lock
            #   .100  Relay A asks to lock #7 ... and succeeds. It's free now.
            #
            # Matching on id alone, A posts it a second time. Asking for "#7 IF
            # STILL UNSENT" means A gets nothing back and moves on.
            #
            # The gap between CHECKING and ACTING has a name: TOCTOU (Time Of
            # Check to Time Of Use). The fix is always the same shape — DO THE
            # CHECK INSIDE THE THING THAT CLAIMS. You meet this idea three
            # times in this project:
            #
            #   Phase 2  SELECT ... WHERE published = false FOR UPDATE
            #   Phase 4  INSERT ... ON CONFLICT DO NOTHING
            #   Phase 5  SET key value NX EX ttl
            #
            # Three technologies, one idea: check and claim must be a single
            # indivisible action.
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.published == False,  # noqa: E712
            )
            .with_for_update(skip_locked=True)
        )

        # one_or_none(), not first(). The three options differ on surprises:
        #     .one()          0 rows -> raises   2+ -> raises
        #     .one_or_none()  0 rows -> None     2+ -> raises
        #     .first()        0 rows -> None     2+ -> silently picks one
        # Zero rows is normal (someone beat us). Two would mean a broken
        # primary key — shout, don't paper over, which is what first() does.
        event = session.scalars(stmt).one_or_none()

        if event is None:
            # Another relay holds it, or already published it. Not our letter.
            log.debug("outbox row %s already claimed or sent; skipping", event_id)
            return False

        # POST IT. If this raises we never reach the lines below: session_scope
        # rolls back, the row stays unsent, the next poll retries. That is the
        # at-least-once guarantee working.
        message_id = publisher.publish(event)

        # THE GAP. Above has happened in the outside world and cannot be
        # undone; below is still only in our database. Scenario A wedges a
        # crash exactly here.
        if config.CRASH_AFTER_PUBLISH:
            _crash_now(event.id)

        # Tick it off. These become an UPDATE, made permanent when
        # session_scope commits on the way out of this block.
        event.published = True
        event.published_at = datetime.now(timezone.utc)

        log.info(
            "published outbox row %s (%s, order %s) -> SNS MessageId %s",
            event.id, event.event_type, event.order_id, message_id,
        )
        return True


def relay_batch() -> tuple[int, int]:
    """One poll cycle. Returns (how many sent, how many found).

    TWO TRANSACTIONS, NOT ONE — this decides how much damage one crash does.

    A batch-wide transaction ticks all ten rows off in ONE statement, which is
    atomic: you can never get 4 of 10. That sounds reassuring but is exactly
    why the damage is large — publish ten, crash before that single commit, and
    ALL TEN are sent again. Per-row caps it at exactly one duplicate.

    Cost: ten commits instead of one, and each forces a write to physical disk
    (an "fsync") before Postgres acknowledges. Irrelevant here; the first thing
    you'd change at scale.
    """
    # Find the work: short, lock-free, session closed immediately — we do not
    # want anything held open during the slow part.
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
            # Give up on the batch. If SNS is unreachable the other nine fail
            # identically, each burning the full boto3 retry budget (~35s)
            # first — ten of those turn a 2s poll into a six-minute stall
            # during which we are also deaf to shutdown signals. The rows stay
            # unsent, which is the point of an outbox.
            #
            # KNOWN FLAW, LEFT IN ON PURPOSE. That holds when the WHOLE WORLD
            # is broken; it fails badly when ONE ROW is. A payload SNS always
            # rejects (say, over the 256 KB limit) is the oldest row, so
            # ORDER BY created_at puts it FIRST IN EVERY BATCH FOREVER, and
            # `break` abandons everything behind it. The entire outbox stops
            # draining permanently while the relay looks healthy.
            #
            # This is HEAD-OF-LINE BLOCKING (the row is a POISON MESSAGE). The
            # bug is treating all failures alike; the fix is classifying them:
            #   transient (timeout, throttling) -> break, the batch would fail
            #   permanent (too large, malformed) -> park the row, continue
            # Parking = an `attempts` counter and `failed_at`; after N tries
            # stop selecting it and alarm. That is a dead-letter queue applied
            # to the outbox table — Phase 6's natural home.
            log.exception(
                "publish failed for outbox row %s — abandoning batch; next poll retries",
                event_id,
            )
            break

    return published, len(event_ids)
