"""Create the topic, the three queues, and the subscriptions between them.

Run standalone:
    set -a; source .env; set +a
    python -m bootstrap.main

Or as a one-shot Compose service:
    docker compose run --rm bootstrap

IDEMPOTENT BY DESIGN — re-running changes nothing. That is not a nicety:
LocalStack's free tier forgets every topic, queue and subscription on restart,
so setup has to be a script that can run on every boot, never commands typed
by hand. Which is how real infrastructure works anyway.
"""

import json
import sys

from shared import aws, config
from shared.log import setup

log = setup("bootstrap")

# One publish must land in all three. Adding a fourth consumer later is one
# entry here and zero changes to the relay — that is the point of fan-out.
QUEUE_NAMES = (
    config.BILLING_QUEUE_NAME,
    config.SHIPPING_QUEUE_NAME,
    config.NOTIFY_QUEUE_NAME,
)

# Phase 6 / FR-07: each queue gets its OWN dead letter queue, never a shared
# one. A poison message must be traceable to the consumer that could not handle
# it — and one consumer filling a shared DLQ would bury the others' failures.
DLQ_FOR = {
    config.BILLING_QUEUE_NAME: config.BILLING_DLQ_NAME,
    config.SHIPPING_QUEUE_NAME: config.SHIPPING_DLQ_NAME,
    config.NOTIFY_QUEUE_NAME: config.NOTIFY_DLQ_NAME,
}

# Only OrderCreated exists today, so this filter accepts everything we publish
# and changes nothing yet. It is here to show WHERE routing lives: give
# notify-queue {"event_type": ["OrderCancelled"]} and it stops receiving
# OrderCreated — no code change in the relay or any consumer.
#
# SNS matches filters against MESSAGE ATTRIBUTES, not the body, which is why
# relay/publisher.py duplicates event_type into an attribute.
# https://docs.aws.amazon.com/sns/latest/dg/sns-message-filtering.html
FILTER_POLICY = {"event_type": ["OrderCreated"]}


def _queue_attributes() -> dict[str, str]:
    """Settings applied to every queue. All values must be strings."""
    return {
        # How long a received message is hidden from other consumers while one
        # works on it. Too short and a slow consumer causes a redelivery it
        # didn't expect; too long and a crashed consumer blocks the message
        # until it expires. Set to 1 to force redeliveries for the Phase 4
        # duplicate test.
        "VisibilityTimeout": str(config.SQS_VISIBILITY_TIMEOUT_SECONDS),

        # Long polling. With 0, receive_message returns instantly and empty,
        # so a consumer loop spins — burning CPU and API calls to learn
        # nothing. With 20, the call waits up to 20s for a message to appear.
        "ReceiveMessageWaitTimeSeconds": str(config.SQS_WAIT_TIME_SECONDS),

        # How long an unconsumed message survives. Note this interacts with
        # the Notifications consumer's 48h Redis TTL: a message redelivered
        # after the key expires would be treated as new. 4 days > 48h, so the
        # window is real. The Postgres consumers have no expiry and dedupe
        # forever.
        "MessageRetentionPeriod": str(config.SQS_MESSAGE_RETENTION_SECONDS),
    }


def _redrive_policy(dlq_arn: str) -> dict:
    """Tell SQS to give up on a message after N deliveries and move it aside.

    THE COUNTER IS DELIVERIES, NOT FAILURES. SQS cannot see whether a consumer
    succeeded — it only knows the message was received and never deleted. So
    ApproximateReceiveCount rises for a genuine crash, a handler that raised,
    AND a handler that simply took longer than the visibility timeout. All
    three look identical from the outside.

    That is why maxReceiveCount must comfortably exceed the redeliveries a
    HEALTHY consumer causes on its own; too low and ordinary slow work lands in
    the DLQ. AWS's guidance is the same.

    Note the DLQ is not a different kind of resource — it is an ordinary queue
    that happens to be named as a redrive target. It gets no SNS subscription
    and no queue policy, because SQS itself moves the message, not SNS.
    https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
    """
    return {
        "deadLetterTargetArn": dlq_arn,
        # A JSON *number* in a string-valued attribute: the policy is itself
        # JSON, so this ends up as {"maxReceiveCount": 5}, not "5".
        "maxReceiveCount": config.SQS_MAX_RECEIVE_COUNT,
    }


def _queue_policy(queue_arn: str, topic_arn: str) -> dict:
    """Permission for SNS to write into this queue.

    THE CLASSIC FIRST-TIME GOTCHA. A queue is private by default. Subscribing
    it to a topic succeeds even without this — and then delivers nothing, with
    no error anywhere. Green subscription, empty queue, silent failure.

    Scoped tightly on purpose:
      Principal  the SNS service, not "*" (which would be every AWS account)
      Action     sqs:SendMessage only — SNS never needs to read or delete
      Resource   this one queue
      Condition  and only when the message comes from OUR topic

    Without the Condition, any SNS topic in any account could write here — the
    "confused deputy" problem: a trusted service tricked into acting for
    someone else. aws:SourceArn is the standard fix.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowOrderEventsTopicToSendMessages",
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
            }
        ],
    }


def ensure_queue(name: str, dlq_arn: str | None = None) -> tuple[str, str]:
    """Create the queue if absent and apply its settings. Returns (url, arn).

    Created BARE, then configured, rather than passing attributes to
    create_queue. create_queue is only idempotent when the attributes match
    exactly — call it again with a changed value and it raises
    QueueAlreadyExists. Splitting the two makes this re-runnable even after
    you edit a setting, which is the whole point of a bootstrap script.

    `dlq_arn` attaches a redrive policy. Omitted when creating a DLQ itself —
    a DLQ with its own DLQ is a chain nobody monitors.
    """
    url = aws.sqs().create_queue(QueueName=name)["QueueUrl"]

    attributes = _queue_attributes()
    if dlq_arn is not None:
        attributes["RedrivePolicy"] = json.dumps(_redrive_policy(dlq_arn))

    aws.sqs().set_queue_attributes(QueueUrl=url, Attributes=attributes)
    arn = aws.queue_arn(url)
    log.info("queue %-16s ready  %s", name, arn)
    return url, arn


def ensure_subscription(topic_arn: str, queue_url: str, queue_arn: str) -> str:
    """Grant SNS access, subscribe the queue, and apply the filter policy."""
    # Policy first. Subscribing before granting access would leave a working
    # subscription silently dropping every message until this ran.
    aws.sqs().set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(_queue_policy(queue_arn, topic_arn))},
    )

    # Idempotent: same topic + protocol + endpoint returns the existing
    # subscription instead of creating a second one. ReturnSubscriptionArn
    # guarantees a real ARN back rather than the string "pending confirmation".
    subscription_arn = aws.sns().subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        ReturnSubscriptionArn=True,
    )["SubscriptionArn"]

    # Attributes are set separately, not passed to subscribe(), for the same
    # reason queues are created bare: changing one later must not require
    # tearing the subscription down.
    #
    # RawMessageDelivery is deliberately left at its default of false, so SNS
    # wraps our JSON in its own envelope. That costs consumers one extra parse
    # (see shared/messages.py) and buys SNS's metadata — MessageId, Timestamp,
    # TopicArn — which is worth having when tracing a duplicate.
    aws.sns().set_subscription_attributes(
        SubscriptionArn=subscription_arn,
        AttributeName="FilterPolicy",
        AttributeValue=json.dumps(FILTER_POLICY),
    )

    log.info("subscribed              %s", subscription_arn)
    return subscription_arn


def main() -> int:
    log.info("bootstrapping AWS resources at %s", config.AWS_ENDPOINT_URL)

    # create_topic is idempotent too — existing name returns its ARN.
    topic_arn = aws.topic_arn(config.SNS_TOPIC_NAME)
    log.info("topic %-16s ready  %s", config.SNS_TOPIC_NAME, topic_arn)

    for name in QUEUE_NAMES:
        # The DLQ must exist before the redrive policy can name its ARN, so it
        # is created first. It is a plain queue: no subscription, no policy,
        # and no redrive policy of its own.
        dlq_name = DLQ_FOR[name]
        _, dlq_arn = ensure_queue(dlq_name)

        url, arn = ensure_queue(name, dlq_arn=dlq_arn)
        ensure_subscription(topic_arn, url, arn)

    # Verify rather than assume. Every call above could succeed while the
    # result is still wrong — a subscription pointing at the wrong queue, say.
    # Counting what actually exists is cheap and catches that.
    subscriptions = aws.sns().list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    subscribed_arns = {s["Endpoint"] for s in subscriptions}
    expected_arns = {aws.queue_arn(aws.queue_url(n)) for n in QUEUE_NAMES}

    missing = expected_arns - subscribed_arns
    if missing:
        log.error("expected queues are NOT subscribed: %s", ", ".join(sorted(missing)))
        return 1

    # Verify the redrive policies as well, for the same reason the
    # subscriptions are verified: every call above can succeed while the result
    # is still wrong. A queue with no DLQ attached looks completely normal until
    # a poison message arrives and loops forever.
    for name in QUEUE_NAMES:
        attributes = aws.sqs().get_queue_attributes(
            QueueUrl=aws.queue_url(name), AttributeNames=["RedrivePolicy"]
        ).get("Attributes", {})

        if "RedrivePolicy" not in attributes:
            log.error("queue %r has NO redrive policy — a poison message would loop forever", name)
            return 1

        policy = json.loads(attributes["RedrivePolicy"])
        expected_dlq_arn = aws.queue_arn(aws.queue_url(DLQ_FOR[name]))
        if policy.get("deadLetterTargetArn") != expected_dlq_arn:
            log.error(
                "queue %r redrives to %r, expected %r",
                name, policy.get("deadLetterTargetArn"), expected_dlq_arn,
            )
            return 1

        log.info(
            "redrive %-14s -> %s after %s deliveries",
            name, DLQ_FOR[name], policy.get("maxReceiveCount"),
        )

    log.info(
        "bootstrap complete — %d queues subscribed to %r, each with a DLQ",
        len(expected_arns), config.SNS_TOPIC_NAME,
    )
    return 0


if __name__ == "__main__":
    # The exit code matters: Compose's `service_completed_successfully` only
    # releases dependents when this exits 0, so a failed bootstrap must stop
    # the relay from starting rather than let it publish into the void.
    sys.exit(main())
