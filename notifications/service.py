"""Notify a customer at most once per order, using Redis as the arbiter.

WHY THIS IS NOT JUST PHASE 4 WITH A DIFFERENT DATABASE

Billing's dedup marker IS its side effect: one row, one constraint, one
transaction. Nothing can happen in between, because there is no "between".

Here the marker (a Redis key) and the side effect (sending an email) are in two
systems that no transaction spans — the same shape as the relay's Postgres/SNS
problem. So the two steps have an order, and it has to be chosen:

    SEND, THEN MARK      a crash in the gap leaves no record that we sent it,
                         so the next delivery sends a SECOND email. The dedup
                         check is silently wrong in the direction it exists to
                         prevent.

    MARK, THEN SEND      <-- what we do. A crash in the gap means the key says
                         "sent" when nothing was. One notification is lost, but
                         the check is never wrong in the permissive direction.

Note this is the OPPOSITE polarity to the relay, which chose duplicates over
loss. The reason is what sits downstream: the relay's duplicates land on
consumers built to absorb them, whereas a duplicate email lands in a human's
inbox and nothing downstream can undo it. Choose the failure your next hop can
actually handle.

The loss window is narrowed below by releasing the key when the send fails
loudly — so only a hard kill (SIGKILL, power cut) can actually drop one.

Reference: Redis SET, NX and EX options — https://redis.io/docs/latest/commands/set/
"""

import os

from shared import config
from shared.log import get_logger
from shared.redis_client import client

log = get_logger(__name__)

CRASH_EXIT_CODE = 19  # distinct from the relay's 17, so a test can tell them apart


def _key(order_id: str) -> str:
    # Namespaced: Redis has one keyspace, and a bare order id would collide the
    # moment anything else stores state here.
    return f"notify:processed:{order_id}"


def _send_notification(order_id: str, payload: dict) -> None:
    """The side effect. A log line stands in for an email/SMS/push.

    What matters for the pattern is that it is NOT a database write: it cannot
    join a transaction, and once it has happened it cannot be rolled back.
    """
    log.info(
        "📧 EMAIL SENT to %s — order %s confirmed (%s, %s)",
        payload["customer_id"], order_id, payload["item"], payload["amount"],
    )


def handle(event: dict) -> None:
    """Process one OrderCreated event, or recognise it as already notified."""
    payload = event["payload"]
    order_id = payload["order_id"]
    key = _key(order_id)

    # ------------------------------------------------------------------
    # CLAIM. One command, not EXISTS-then-SET.
    #
    #   NX  set only if the key does not exist
    #   EX  expire after TTL seconds
    #
    # Redis executes commands one at a time, so the check and the write cannot
    # be interleaved by another consumer. A separate EXISTS followed by a
    # separate SET has a window between them where two consumers both see
    # "absent" and both send. Same idea as ON CONFLICT in Phase 4 and
    # SELECT ... FOR UPDATE in the relay: the check belongs inside the claim.
    #
    # Returns True if we set it, None if it already existed — so the return
    # value IS the fresh/duplicate answer. (Phase 4's equivalent needed
    # RETURNING because ON CONFLICT DO NOTHING reports nothing useful.)
    # ------------------------------------------------------------------
    claimed = client().set(
        key, event["event_id"], nx=True, ex=config.NOTIFY_DEDUP_TTL_SECONDS
    )

    if not claimed:
        # Someone already holds the claim. Read who, purely so the log can show
        # whether this was the same event redelivered or a different one.
        first = client().get(key)
        log.info(
            "🔁 DUPLICATE ignored for order %s — already notified by event %s "
            "(this delivery: %s)",
            order_id, first, event["event_id"],
        )
        return

    # Scenario A's Phase 5 twin: die in the gap between marking and sending, to
    # prove the key survives and the notification is genuinely lost.
    if config.CRASH_AFTER_MARK:
        log.error("CRASH_AFTER_MARK — exiting between the Redis SET and the send")
        import logging
        logging.shutdown()
        os._exit(CRASH_EXIT_CODE)

    try:
        _send_notification(order_id, payload)
    except Exception:
        # RELEASE THE CLAIM so a redelivery can retry. Without this, one
        # transient failure suppresses the notification permanently — the key
        # would sit there for 48h claiming a send that never happened.
        #
        # This shrinks the loss window from "any send failure" to "killed
        # between the SET and this handler". It does not close it: if the send
        # actually succeeded and only the acknowledgement failed, deleting the
        # key means the redelivery sends a second email. That ambiguity is not
        # solvable here — it is the same "did it work?" problem the relay has
        # with SNS.
        client().delete(key)
        log.exception("send failed for order %s — claim released for retry", order_id)
        raise
