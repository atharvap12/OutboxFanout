"""Turning an outbox row into an SNS message.

Everything SNS-specific lives here, so service.py reads as pure outbox logic.
"""

import json
from typing import Any

from botocore.exceptions import ClientError

from shared import aws, config
from shared.log import get_logger

from order.models import OutboxEvent

log = get_logger(__name__)


# Resolved once, then reused: create_topic is a network round trip and the ARN
# is stable while LocalStack is up. Cached at module level rather than with
# lru_cache so it can be invalidated — LocalStack forgets every topic on
# restart, which turns a cached ARN into a stale one that 404s forever.
_topic_arn: str | None = None


def topic_arn() -> str:
    """ARN of the order-events topic, creating it if absent.

    create_topic is idempotent, so this doubles as the bootstrap step. That
    matters because LocalStack has no free persistence: the topic disappears on
    every restart and something has to recreate it without human intervention.
    """
    global _topic_arn
    if _topic_arn is None:
        _topic_arn = aws.topic_arn(config.SNS_TOPIC_NAME)
        log.info("SNS topic %r resolved to %s", config.SNS_TOPIC_NAME, _topic_arn)
    return _topic_arn


def forget_topic_arn() -> None:
    """Drop the cached ARN so the next publish re-resolves (and recreates) it."""
    global _topic_arn
    _topic_arn = None


def build_message(event: OutboxEvent) -> dict[str, Any]:
    """The envelope consumers will receive.

    `event_id` is the outbox row's id, and it is the value consumers should
    dedupe on if they ever dedupe per-event rather than per-order. Note it is
    NOT the SNS MessageId: SNS mints a fresh MessageId on every publish, so
    republishing the same row after a crash produces two different MessageIds
    for one logical event. The outbox row id does not change, which is exactly
    what makes it a usable idempotency key.

    `payload` is passed through untouched — the Order Service already decided
    what an OrderCreated event contains, and the relay is deliberately dumb
    about it. A relay that reshapes payloads becomes a second place where the
    event schema lives.
    """
    return {
        "event_id": str(event.id),
        "event_type": event.event_type,
        "order_id": str(event.order_id),
        # When the event happened, not when it was published. The gap between
        # the two is relay lag, and a consumer that cares about ordering or
        # staleness needs the former.
        "occurred_at": event.created_at.isoformat(),
        "payload": event.payload,
    }


def publish(event: OutboxEvent) -> str:
    """Publish one outbox row to SNS. Returns the SNS MessageId.

    Raises on failure rather than returning a flag: the caller must not mark
    the row published, and an exception is the only way to make forgetting that
    impossible.
    """
    try:
        response = aws.sns().publish(
            TopicArn=topic_arn(),
            Message=json.dumps(build_message(event)),
            # Duplicated from the body so Phase 3 can attach a subscription
            # filter policy — SNS filters on attributes only, never on the
            # message body (unless the subscription opts into body filtering).
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": event.event_type,
                },
            },
        )
    except ClientError as exc:
        # A LocalStack restart deletes the topic while we still hold its ARN.
        # Forget it so the next attempt recreates it instead of 404ing forever.
        if "NotFound" in exc.response.get("Error", {}).get("Code", ""):
            log.warning("topic ARN went stale (LocalStack restart?); will re-resolve")
            forget_topic_arn()
        raise

    return response["MessageId"]
