"""Everything about talking to SNS lives here.

Keeping it separate means service.py reads as pure outbox logic. If this
project ever moved to Kafka, this is the only file that changes.

WHAT SNS IS: a broadcaster. You create a "topic" (a named channel), parties
"subscribe", and one publish gives every subscriber its own copy. The
alternative is the relay looping over three destinations itself; with SNS,
adding a fourth consumer is a subscription change and zero code changes here.

Our topic currently has no subscribers, which is fine — publishing to a topic
nobody listens to succeeds and the message is dropped. Phase 3 adds listeners.
"""

import json
from typing import Any

from botocore.exceptions import ClientError

from shared import aws, config
from shared.log import get_logger

from order.models import OutboxEvent

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Finding the topic
#
# An ARN (Amazon Resource Name) is AWS's full postal address for a thing:
#
#     arn:aws:sns:us-east-1:000000000000:order-events
#                  region     account       name
#
# You publish to an ARN, not a name — but an ARN contains your account and
# region, so it differs per environment. config.py therefore stores the stable
# NAME and we look the address up at runtime. A hardcoded ARN would break the
# day you moved to real AWS.
# ---------------------------------------------------------------------------

# `str | None` = "either text, or nothing". None means "not looked up yet".
_topic_arn: str | None = None


def topic_arn() -> str:
    """Get the topic's address, creating the topic if it doesn't exist.

    CACHING: asking AWS costs a network round trip; doing it every 2 seconds
    forever is waste, so we remember the answer.

    `global` is required. Normally, assigning inside a function creates a new
    variable that vanishes on return — a sticky note you throw away. `global`
    says "I mean the shared one at the top of the file". Without it the cache
    would fill and be discarded every single time.

    BOOTSTRAPPING: create_topic is *idempotent* — doing it twice has the same
    effect as once. Existing name returns its address; new name creates one.
    That is what lets us call it on every start instead of maintaining a
    separate setup step someone has to remember, which matters because
    LocalStack's free tier forgets every topic on restart.
    """
    global _topic_arn
    if _topic_arn is None:
        _topic_arn = aws.topic_arn(config.SNS_TOPIC_NAME)
        log.info("SNS topic %r resolved to %s", config.SNS_TOPIC_NAME, _topic_arn)
    return _topic_arn


def forget_topic_arn() -> None:
    """Drop the remembered address so the next call looks it up again.

    A cache is a bet that the world hasn't changed, and here that bet can lose:
    restart LocalStack and the topic is gone while we still hold its old
    address — like writing to a friend who moved. Without an escape hatch the
    relay would fail forever and need a human; with one it heals itself.

    (This is why the cache is a plain variable rather than @lru_cache, which
    shared/aws.py uses for the clients. lru_cache is tidier but gives you no
    clean way to say "forget".)
    """
    global _topic_arn
    _topic_arn = None


def build_message(event: OutboxEvent) -> dict[str, Any]:
    """Wrap the order snapshot in an envelope.

    An ENVELOPE is metadata on the OUTSIDE of the letter, so a consumer can
    decide whether it cares without opening and parsing the contents.

    THE KEY FIELD IS `event_id` — the outbox ROW's own id, deliberately NOT the
    id SNS will assign. SNS invents a fresh MessageId on every publish, so
    after a Scenario A crash the two copies of one real event carry two
    different MessageIds and look unrelated. A consumer deduping on MessageId
    would remember two and bill the customer twice. Our event_id never changes,
    however many times the row is republished.

        A DEDUP KEY MUST COME FROM YOUR DOMAIN, NOT FROM THE TRANSPORT.

    (Phases 4-5 will actually key on order_id, because the rule is "bill each
    ORDER once". Both are ours; either satisfies the principle.)

    `payload` passes through untouched: the Order Service already defined what
    an OrderCreated contains, and two copies of a definition always drift. The
    courier does not open the letter and rewrite it.
    """
    return {
        # str() because JSON has no UUID type — see occurred_at below.
        "event_id": str(event.id),
        "event_type": event.event_type,
        "order_id": str(event.order_id),

        # When it HAPPENED, not when we sent it. If the relay was down three
        # hours this still says when the customer clicked buy; the difference
        # is relay lag. Given only send-time, a consumer cannot tell a fresh
        # event from a backlog replay.
        #
        # .isoformat() because JSON has no date type — nor UUID, nor Decimal.
        # JSON knows text, numbers, true/false, null, lists, dicts. Anything
        # else must become text or json.dumps() below refuses to run.
        "occurred_at": event.created_at.isoformat(),

        # Already a dict (stored as JSONB). Note `amount` inside is still the
        # STRING "499.99" for the floating-point reason in order/service.py.
        "payload": event.payload,
    }


def publish(event: OutboxEvent) -> str:
    """Send one outbox row to SNS. Returns the id SNS assigned.

    RAISES rather than returning False on failure. A True/False can be ignored
    by accident — one forgotten `if` and you have marked an unsent event as
    sent, losing it permanently. An exception cannot be ignored: it travels up,
    the transaction rolls back, the row stays unpublished, the next poll
    retries.

        MAKE THE DANGEROUS MISTAKE IMPOSSIBLE, NOT MERELY DOCUMENTED.
    """
    try:
        response = aws.sns().publish(
            TopicArn=topic_arn(),
            # SNS carries text, not Python objects.
            Message=json.dumps(build_message(event)),

            # Yes, event_type is already in the body — duplicated deliberately.
            # A subscriber can attach a filter policy ("only send me
            # OrderCreated"), but SNS filters on ATTRIBUTES only; the body is
            # an opaque blob it never opens. Body = sealed letter, attributes =
            # what's written on the envelope; the sorting office routes on the
            # outside. Phase 3 may want this, and it costs one dict now.
            # https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": event.event_type,
                },
            },
        )
    except ClientError as exc:
        # ClientError = "AWS answered, and the answer was an error". We catch
        # only to check one case, then re-raise so the caller still fails.
        #
        # The case: LocalStack restarted and forgot our topic, so our cached
        # address points at nothing. Clearing it makes the next attempt look it
        # up — and since create_topic is idempotent, that also RE-CREATES the
        # topic. Self-repair, nobody watching.
        #
        # .get(...).get(...) is defensive: it returns a fallback instead of
        # exploding on a missing key. We are already in an error path, and a
        # crash inside the error handler would hide the original problem.
        if "NotFound" in exc.response.get("Error", {}).get("Code", ""):
            log.warning("topic address went stale (LocalStack restart?) — will re-resolve")
            forget_topic_arn()
        raise

    return response["MessageId"]
