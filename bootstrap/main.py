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


def ensure_queue(name: str) -> tuple[str, str]:
    """Create the queue if absent and apply its settings. Returns (url, arn).

    Created BARE, then configured, rather than passing attributes to
    create_queue. create_queue is only idempotent when the attributes match
    exactly — call it again with a changed value and it raises
    QueueAlreadyExists. Splitting the two makes this re-runnable even after
    you edit a setting, which is the whole point of a bootstrap script.
    """
    url = aws.sqs().create_queue(QueueName=name)["QueueUrl"]
    aws.sqs().set_queue_attributes(QueueUrl=url, Attributes=_queue_attributes())
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
        url, arn = ensure_queue(name)
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

    log.info(
        "bootstrap complete — %d queues subscribed to %r",
        len(expected_arns), config.SNS_TOPIC_NAME,
    )
    return 0


if __name__ == "__main__":
    # The exit code matters: Compose's `service_completed_successfully` only
    # releases dependents when this exits 0, so a failed bootstrap must stop
    # the relay from starting rather than let it publish into the void.
    sys.exit(main())
