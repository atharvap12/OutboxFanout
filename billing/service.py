"""Bill an order exactly once, letting the database be the judge.

THE SHAPE OF THE PROBLEM

The same event may arrive two, three, ten times — the relay republishes after
a crash, SQS redelivers after a visibility timeout, boto3 retries a lost
acknowledgement. We are told to assume at-least-once and never exactly-once.

So the question is never "how do I stop duplicates arriving?" (you cannot). It
is "how do I make the SECOND arrival do nothing?"

THE ANSWER, IN ONE STATEMENT

    INSERT ... ON CONFLICT (order_id) DO NOTHING

Try to write the row. If a row with that order_id already exists, Postgres
quietly declines and moves on — no exception, no crash, no duplicate.

    A CLOAKROOM WITH NUMBERED PEGS. You arrive with a coat and the number 47.
    If peg 47 is empty, your coat hangs there. If peg 47 already has a coat,
    the attendant just... doesn't hang a second one. He does not argue with
    you, and he does not throw your coat away either. Nothing bad happens; the
    world simply already contains exactly one coat on peg 47.

THE PART EVERYONE GETS WRONG

Because DO NOTHING raises no exception, the two outcomes — "I inserted it" and
"it was already there" — LOOK IDENTICAL from Python. Something has to tell them
apart, and getting that wrong is the subject of the long comment below.

Reference: PostgreSQL — INSERT ... ON CONFLICT —
https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.db import session_scope
from shared.log import get_logger

from billing.models import BillingRecord

log = get_logger(__name__)


def handle(event: dict) -> None:
    """Process one OrderCreated event, or recognise it as one we already did.

    Returning normally means "dealt with" — including the duplicate case — and
    the consumer loop then deletes the SQS message. Raising means "not dealt
    with", and the loop leaves the message for redelivery.
    """
    payload = event["payload"]
    order_id = uuid.UUID(payload["order_id"])

    # session_scope() = one transaction. Everything inside either all happens
    # or none of it does. Here that is a single INSERT, so it looks like
    # overkill — but it is what guarantees the consumer loop's contract: if
    # anything throws, NOTHING was written, so a redelivery starts from a clean
    # slate rather than from some half-finished state.
    with session_scope() as session:
        stmt = (
            # pg_insert, not the generic sqlalchemy insert: ON CONFLICT is a
            # PostgreSQL extension, not standard SQL, so it lives in the
            # postgresql dialect module.
            pg_insert(BillingRecord)
            .values(
                id=uuid.uuid4(),
                order_id=order_id,
                event_id=uuid.UUID(event["event_id"]),
                customer_id=payload["customer_id"],
                # str -> Decimal, exactly. The relay deliberately shipped this
                # as a JSON string rather than a JSON number so that this parse
                # is lossless; JSON's only number type is a float.
                amount=Decimal(payload["amount"]),
                processed_at=datetime.now(timezone.utc),
            )

            # ----------------------------------------------------------
            # THE CHECK AND THE CLAIM ARE ONE STATEMENT.
            #
            # This is the third time this project has used the same idea, and
            # it is worth naming, because it is the single most transferable
            # thing in the whole build:
            #
            #     relay      SELECT ... FOR UPDATE SKIP LOCKED
            #     here       INSERT ... ON CONFLICT DO NOTHING
            #     Phase 5    SET key value NX
            #
            #     DO THE CHECK INSIDE THE THING THAT CLAIMS, NEVER BEFORE IT.
            #
            # Any version with a separate "look first" step has a window
            # between looking and acting where a second worker can look too.
            # The database (or Redis) is the only party that can close that
            # window, because it is the only party that sees both requests.
            # ----------------------------------------------------------
            .on_conflict_do_nothing(index_elements=["order_id"])

            # ----------------------------------------------------------
            # ⚠️ HOW WE LEARN WHICH BRANCH HAPPENED — AND THE TRAP HERE.
            #
            # RETURNING asks Postgres to hand back the rows it actually wrote:
            #
            #     inserted   -> one row comes back    -> this event is NEW
            #     conflicted -> no rows come back     -> DUPLICATE
            #
            # Unambiguous, and it is the database itself reporting, not a
            # driver's bookkeeping.
            #
            # WHY NOT `result.rowcount`, WHICH THE DESIGN DOC SUGGESTS?
            # Because on this exact stack — SQLAlchemy 2.0 + psycopg 3, an ORM
            # entity, an INSERT — rowcount comes back as **-1**. Not 0, not 1:
            # -1, which means "I do not have that information for you". So
            # `rowcount == 1` is NEVER true, and every event, including the
            # genuinely fresh ones, is reported as a duplicate. Measured; see
            # VERIFY-PHASE-4.md for the probe that shows it.
            #
            # WHAT MAKES THAT BUG NASTY IS WHERE IT FAILS. The database is
            # still perfectly correct — the UNIQUE constraint does its job and
            # exactly one row exists either way. What is wrong is only the
            # REPORT. So the obvious test, `SELECT count(*) = 1`, PASSES on
            # completely broken duplicate-detection code, and the only symptom
            # is a log line that lies.
            #
            # It is harmless here purely because the dedup check and the side
            # effect are the same write, so a wrong flag corrupts nothing but
            # the log. In Phase 5 they are SEPARATE — the flag decides whether
            # the email gets sent — and the identical bug would silently skip
            # every notification.
            #
            # SQLAlchemy is explicit that rowcount is only meaningful for
            # UPDATE and DELETE:
            # https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.CursorResult.rowcount
            # ----------------------------------------------------------
            .returning(BillingRecord.id)
        )

        # scalar_one_or_none(): exactly one value, or None. Perfectly matched
        # to a question whose only two answers are "here is the id I wrote"
        # and "I wrote nothing".
        fresh = session.execute(stmt).scalar_one_or_none() is not None

    # Logging OUTSIDE the `with` block, so these lines only ever appear after
    # the transaction has actually committed. Logging "BILLED" inside the block
    # would print it even in the run where the commit then fails — a log that
    # claims work that was rolled back is worse than no log at all.
    #
    # The design doc asks for visibility over cleverness here: the whole
    # deliverable of Phase 4 is being able to SEE a duplicate being caught.
    if fresh:
        log.info(
            "💳 BILLED order %s — %s %s (event %s)",
            order_id, payload["customer_id"], payload["amount"], event["event_id"],
        )
    else:
        log.info(
            "🔁 DUPLICATE ignored for order %s — already billed (event %s)",
            order_id, event["event_id"],
        )
