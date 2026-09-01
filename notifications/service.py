"""Notify a customer at most once per order, using Redis as the arbiter.

=============================================================================
PART 1 — THE CLAIM: WHY ONE COMMAND AND NOT TWO
=============================================================================

    A CAFÉ WITH ONE TOILET AND ONE KEY ON A HOOK.

    The right way: you walk up and TAKE THE KEY. One motion. If your hand comes
    back with the key, the toilet is yours. If it comes back empty, someone else
    has it and you go and sit down.

    The wrong way, which looks identical when the café is quiet: you GLANCE at
    the hook, see a key hanging there, and then reach for it. Two people glance
    at the same instant, both see a key, both reach — and now two people believe
    they have exclusive access to one toilet.

The wrong way is `EXISTS` followed by `SET`:

    if not redis.exists(key):      # <-- glance
        redis.set(key, 1)          # <-- reach
        send_email()

Between the glance and the reach there is a window. It is microseconds wide,
which is precisely what makes it dangerous: it will never fail while you are
testing it by hand, and it will fail under load, in production, as "some
customers got two emails and I cannot reproduce it."

The right way is one command:

    SET key value NX EX ttl

    NX  =  "only if it does Not eXist"
    EX  =  "and expire it after this many seconds"

Redis executes commands ONE AT A TIME (its command execution is single
threaded), so nothing can interleave between the check and the write — because
the check happens *inside* the write. There is no window to lose a race in.

MEASURED, not assumed. 50 threads racing for the same key:

    SET ... NX          ->  1 winner
    EXISTS then SET     ->  3 winners      <-- three emails

The reproduction is in VERIFY-PHASE-5.md, step 4.

THIS IS THE THIRD TIME THIS PROJECT HAS SOLVED THE SAME PROBLEM:

    relay                SELECT ... FOR UPDATE SKIP LOCKED
    billing / shipping   INSERT ... ON CONFLICT DO NOTHING
    notifications        SET key value NX EX ttl

Three different technologies, one idea:
    DO THE CHECK INSIDE THE THING THAT CLAIMS, NEVER BEFORE IT.
If you can name that idea, you can find this class of bug anywhere.

Reference: Redis SET, including NX and EX — https://redis.io/docs/latest/commands/set/


=============================================================================
PART 2 — THE ORDERING: WHICH FAILURE DO YOU WANT?
=============================================================================

Two acts — mark the key, send the email — and no transaction spans Redis and an
SMTP server. So there is an order, and it must be chosen deliberately.

    A LETTER YOU CANNOT UNPOST.

    Billing writes in a ledger, and the ledger entry IS the payment. Nothing can
    drift out of sync, because there is only one thing.

    Sending an email is dropping a letter into a postbox. Once it has gone, no
    amount of database work brings it back. And ticking your notebook to say "I
    posted it" is a SEPARATE physical act from dropping the letter.

    So: do you tick the notebook first, or drop the letter first?

    DROP THE LETTER, THEN TICK          A power cut in between leaves a posted
    (send, then mark)                   letter and no record of it. Tomorrow you
                                        read the notebook, see nothing, and POST
                                        A SECOND LETTER. The record-keeping has
                                        failed in the exact direction it exists
                                        to prevent.

    TICK, THEN DROP THE LETTER          A power cut in between leaves a notebook
    (mark, then send)   <-- WHAT WE DO  claiming a letter that was never posted.
                                        ONE NOTIFICATION IS LOST. But the
                                        notebook is never wrong in the
                                        permissive direction — it never lets a
                                        second one out.

We tick first. The design doc says the same thing in its own words: writing the
key after processing "creates a false negative on the next duplicate check".

⚠️ NOTE THIS IS THE OPPOSITE CHOICE TO THE RELAY, AND THAT IS NOT AN
INCONSISTENCY.

    relay          publish, THEN mark   ->  prefers DUPLICATES over loss
    notifications  mark, THEN send      ->  prefers LOSS over duplicates

Both are correct, and the rule that produces both is one sentence:

    CHOOSE THE FAILURE YOUR NEXT HOP CAN ACTUALLY HANDLE.

The relay's next hop is three consumers built specifically to absorb duplicates
— so duplicates there are free, and loss would be catastrophic and silent. This
consumer's next hop is a human being's inbox. Nothing downstream of an email can
undo it; there is no `ON CONFLICT DO NOTHING` for a customer who has already
read it.

The loss window is narrowed further below (see the `except` block) so that only
a HARD kill can actually drop a notification — but it cannot be closed, and
pretending otherwise would be the real mistake.
"""

import os

from shared import config
from shared.log import get_logger
from shared.redis_client import client

log = get_logger(__name__)

# Distinct from the relay's 17, so a test can assert WHICH deliberate crash
# happened rather than just "something exited non-zero".
CRASH_EXIT_CODE = 19


def _key(order_id: str) -> str:
    """The dedup key for one order.

    NAMESPACED, and that matters more than it looks. Redis has ONE FLAT
    KEYSPACE — no tables, no schemas, no separation whatsoever. A bare
    `{order_id}` would collide with anything else that ever stores state here,
    and the collision would be silent.

    Compare Billing, where the equivalent mistake is impossible: a UNIQUE
    constraint belongs to a named table, so Postgres would never confuse it with
    another consumer's. WITH REDIS, THE NAMESPACE IS YOUR JOB.

    Keyed on order_id, not event_id, for the same reason as Phase 4: the business
    rule is "notify this order once", and the relay republishing one outbox row
    three times means three deliveries carrying the SAME order_id.
    """
    return f"notify:processed:{order_id}"


def _send_notification(order_id: str, payload: dict) -> None:
    """The side effect. A log line stands in for an email / SMS / push.

    Keep in mind what it REPRESENTS, because that is what drives every decision
    in this file: something that

        (a) is NOT a database write, so it cannot join a transaction, and
        (b) CANNOT BE UNDONE once it has happened.

    Swap in a real SES or Twilio call and nothing above or below needs to change
    — which is the point. The design doc's guidance is that Redis-style
    idempotency is the right fit exactly "when the side effect is NOT a DB write
    (sending an email/SMS, calling a third-party API)".
    """
    log.info(
        "📧 EMAIL SENT to %s — order %s confirmed (%s, %s)",
        payload["customer_id"], order_id, payload["item"], payload["amount"],
    )


def handle(event: dict) -> None:
    """Process one OrderCreated event, or recognise it as already notified.

    Contract with shared/consumer.py (unchanged from Phase 4):
        returns  ->  "dealt with", loop deletes the SQS message
        raises   ->  "not dealt with", loop leaves it for redelivery
    """
    payload = event["payload"]
    order_id = payload["order_id"]
    key = _key(order_id)

    # ---------------------------------------------------------------------
    # STEP 1 — TAKE THE KEY OFF THE HOOK.
    #
    # One command. See PART 1 of the module docstring for why this must not be
    # an EXISTS followed by a SET.
    #
    # WHAT COMES BACK IS THE ANSWER ITSELF:
    #
    #     True  -> we set it, nobody had it, THIS EVENT IS NEW
    #     None  -> it already existed, so this is a DUPLICATE
    #
    # (redis-py returns None, not False, when NX declines. `if not claimed`
    # handles both, and is why this is not written as `if claimed is False`.)
    #
    # WORTH COMPARING TO PHASE 4. There, `ON CONFLICT DO NOTHING` succeeds
    # silently either way and tells you nothing, so Billing had to add a
    # RETURNING clause to find out which branch happened — and using `rowcount`
    # instead silently reported every event as a duplicate. Redis has no such
    # trap: the return value of the claim IS the fresh/duplicate verdict.
    #
    # WHAT THE TTL BUYS AND COSTS. Postgres dedupes forever; this key expires
    # after 48h, so a redelivery arriving later is treated as brand new and the
    # customer gets a second email. SQS retention defaults to FOUR DAYS, which
    # is longer than the TTL — so that window is real, not theoretical. In
    # exchange, the dedup store cleans itself up instead of growing forever.
    # (shared/config.py documents this interaction; it is a genuine unfixed
    # sharp edge in this project, recorded rather than quietly tuned away.)
    # ---------------------------------------------------------------------
    claimed = client().set(
        key,
        # Store WHICH event claimed it, rather than a bare "1" as the design doc
        # suggests. Costs nothing and makes the duplicate log line able to say
        # who got there first — so you can tell "one event redelivered three
        # times" from "three different events about one order" without guessing.
        event["event_id"],
        nx=True,
        ex=config.NOTIFY_DEDUP_TTL_SECONDS,
    )

    if not claimed:
        # Somebody already holds the claim. Read the holder purely for the log.
        first = client().get(key)
        log.info(
            "🔁 DUPLICATE ignored for order %s — already notified by event %s "
            "(this delivery: %s)",
            order_id, first, event["event_id"],
        )
        # Returning normally means the consumer loop DELETES the SQS message.
        # Deliberately doing nothing is still "dealt with" — leaving it would
        # have SQS redeliver this same duplicate forever.
        return

    # ---------------------------------------------------------------------
    # THE GAP. Everything interesting about Phase 5 lives on this line.
    #
    # The key now says "notified". The email has not been sent. A crash here
    # loses the notification permanently, and CRASH_AFTER_MARK exists to prove
    # that rather than let it stay an interesting hypothesis.
    #
    # os._exit() rather than raise/sys.exit(), for the reason spelled out in
    # relay/service.py: those unwind the stack and run cleanup handlers, which
    # is a tidy SHUTDOWN and proves nothing. os._exit() skips finally blocks,
    # atexit and buffer flushing — it is what SIGKILL or a yanked power cable
    # actually looks like. (logging.shutdown() runs first only so the
    # explanatory line survives; flushing the RECORD of a crash is
    # instrumentation, the crash itself is still just as violent.)
    # ---------------------------------------------------------------------
    if config.CRASH_AFTER_MARK:
        log.error("CRASH_AFTER_MARK — exiting between the Redis SET and the send")
        import logging
        logging.shutdown()
        os._exit(CRASH_EXIT_CODE)

    # ---------------------------------------------------------------------
    # STEP 2 — POST THE LETTER.
    # ---------------------------------------------------------------------
    try:
        _send_notification(order_id, payload)
    except Exception:
        # RELEASE THE CLAIM.
        #
        # Without this line, a single transient failure — SMTP down for five
        # seconds — suppresses this customer's notification FOR 48 HOURS, because
        # the key sits there claiming a send that never happened. The redelivery
        # arrives, sees the key, and politely says "already notified".
        #
        # Deleting the key hands the toilet key back to the hook, so the
        # redelivery can genuinely retry. This narrows the loss window from
        # "any send failure at all" down to "hard-killed between the SET and
        # this handler" — which is exactly what CRASH_AFTER_MARK simulates.
        #
        # IT DOES NOT CLOSE THE WINDOW, AND CANNOT. If the send actually
        # succeeded and only the acknowledgement was lost, we delete the key and
        # the redelivery sends a SECOND email. That is the same unresolvable
        # "did it work?" ambiguity the relay has with SNS: you can never
        # distinguish "it failed" from "it succeeded but the reply went missing".
        # All you get to choose is which way to guess.
        client().delete(key)
        log.exception("send failed for order %s — claim released for retry", order_id)
        # Re-raise so the consumer loop leaves the SQS message for redelivery.
        # Swallowing it here would delete the message and lose the notification
        # for good — with the claim released, which is the worst of both.
        raise
