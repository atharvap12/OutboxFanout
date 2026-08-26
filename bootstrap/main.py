"""Create the topic, the three queues, and the subscriptions between them.

Run it standalone:
    set -a; source .env; set +a
    python -m bootstrap.main

Or as a one-shot container:
    docker compose run --rm bootstrap

IDEMPOTENT BY DESIGN — running it again changes nothing. That is a requirement,
not a nicety: LocalStack's free tier forgets every topic, queue and
subscription when it restarts, so setup has to run on every boot. Which is how
real infrastructure works anyway; nobody hand-types production resources.

WHAT FAN-OUT MEANS HERE. Phase 2 published into a topic nobody listened to, so
every message was accepted and thrown away. After this script runs, one publish
becomes three independent copies — one per queue. The relay still publishes
exactly ONCE and knows nothing about queues; duplicating is SNS's job. Adding a
fourth consumer later is one line in this file and zero changes to the relay.
"""

import json
import sys

from shared import aws, config
from shared.log import setup

log = setup("bootstrap")

QUEUE_NAMES = (
    config.BILLING_QUEUE_NAME,
    config.SHIPPING_QUEUE_NAME,
    config.NOTIFY_QUEUE_NAME,
)

# A filter policy is a standing instruction to the sorting office: "only put
# this kind of letter in my pigeonhole."
#
# OrderCreated is the only type we publish today, so this accepts everything
# and changes nothing. It is here to show WHERE routing lives: give notify-queue
# {"event_type": ["OrderCancelled"]} and it stops receiving OrderCreated, with
# no code change in the relay or in any consumer.
#
# IMPORTANT: SNS matches filters against MESSAGE ATTRIBUTES, never against the
# body — to SNS the body is a sealed envelope it does not open. That is why
# relay/publisher.py copies event_type into an attribute.
# https://docs.aws.amazon.com/sns/latest/dg/sns-message-filtering.html
FILTER_POLICY = {"event_type": ["OrderCreated"]}


def _queue_attributes() -> dict[str, str]:
    """Settings applied to every queue. AWS wants all values as strings."""
    return {
        # How long a message is HIDDEN from other consumers while one works on
        # it. Like taking a file off a shared shelf: others must not see it,
        # but if you never put it back it has to reappear eventually.
        # Too short -> a slow consumer causes a redelivery it did not expect.
        # Too long  -> a crashed consumer blocks that message until it expires.
        # Set to 1 to force redeliveries for the Phase 4 duplicate test.
        "VisibilityTimeout": str(config.SQS_VISIBILITY_TIMEOUT_SECONDS),

        # LONG POLLING. With 0, receive_message returns instantly and empty, so
        # a consumer loop spins — burning CPU and API calls to learn nothing.
        # With 20, the call waits up to 20s for a message to turn up. Same idea
        # as the relay sleeping between polls, done by the server instead.
        "ReceiveMessageWaitTimeSeconds": str(config.SQS_WAIT_TIME_SECONDS),

        # How long an unread message survives. Note it interacts with the
        # Notifications consumer's 48h Redis TTL: a message redelivered after
        # that key expires would look new. 4 days > 48h, so the window is real.
        # The Postgres consumers have no expiry and dedupe forever.
        "MessageRetentionPeriod": str(config.SQS_MESSAGE_RETENTION_SECONDS),
    }


def _queue_policy(queue_arn: str, topic_arn: str) -> dict:
    """Permission for SNS to put messages into this queue.

    THE CLASSIC FIRST-TIME GOTCHA, and the design doc warns about it by name.
    A queue is PRIVATE by default. Subscribing it to a topic succeeds without
    this policy — and then delivers nothing, with no error anywhere. Green
    subscription, empty queue, silent failure.

    Installing a pigeonhole is not the same as authorising the postman to put
    things in it.

    Every clause is deliberately narrow:

        Principal   the SNS service — not "*", which is every AWS account alive
        Action      sqs:SendMessage only; SNS never needs to read or delete
        Resource    this one queue
        Condition   ...and only when the message came from OUR topic

    Without that Condition, any SNS topic in any AWS account could write here.
    That is the CONFUSED DEPUTY problem: a trusted middleman (SNS) tricked into
    acting on a stranger's behalf, because the queue trusts the middleman
    rather than the actual sender. aws:SourceArn is the standard fix.
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
    """Create the queue if absent, apply its settings. Returns (url, arn).

    Created BARE, then configured — deliberately not `create_queue(Attributes=...)`.
    create_queue is only idempotent when the attributes match EXACTLY; call it
    again after changing VisibilityTimeout and it raises QueueAlreadyExists.
    Splitting creation from configuration keeps this re-runnable after any edit,
    which is the entire point of a bootstrap script.
    """
    url = aws.sqs().create_queue(QueueName=name)["QueueUrl"]
    aws.sqs().set_queue_attributes(QueueUrl=url, Attributes=_queue_attributes())
    arn = aws.queue_arn(url)
    log.info("queue %-16s ready  %s", name, arn)
    return url, arn


def ensure_subscription(topic_arn: str, queue_url: str, queue_arn: str) -> str:
    """Grant SNS access, subscribe the queue, apply the filter policy."""
    # Policy FIRST. Subscribing before granting access would leave a working
    # subscription silently dropping every message until this line ran.
    aws.sqs().set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(_queue_policy(queue_arn, topic_arn))},
    )

    # Idempotent: same topic + protocol + endpoint returns the EXISTING
    # subscription instead of making a second one. ReturnSubscriptionArn
    # guarantees a real ARN back rather than the string "pending confirmation".
    subscription_arn = aws.sns().subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        ReturnSubscriptionArn=True,
    )["SubscriptionArn"]

    # Attributes set separately, not passed to subscribe(), for the same reason
    # queues are created bare: changing one later must not mean tearing the
    # subscription down and rebuilding it.
    #
    # RawMessageDelivery is deliberately left at its default of FALSE, so SNS
    # wraps our JSON in an envelope of its own. That costs consumers one extra
    # parse (see shared/messages.py) and buys SNS's metadata — MessageId,
    # Timestamp, TopicArn — which is worth having when tracing a duplicate.
    aws.sns().set_subscription_attributes(
        SubscriptionArn=subscription_arn,
        AttributeName="FilterPolicy",
        AttributeValue=json.dumps(FILTER_POLICY),
    )

    log.info("subscribed              %s", subscription_arn)
    return subscription_arn


def main() -> int:
    log.info("bootstrapping AWS resources at %s", config.AWS_ENDPOINT_URL)

    # create_topic is idempotent too — an existing name returns its ARN.
    topic_arn = aws.topic_arn(config.SNS_TOPIC_NAME)
    log.info("topic %-16s ready  %s", config.SNS_TOPIC_NAME, topic_arn)

    for name in QUEUE_NAMES:
        url, arn = ensure_queue(name)
        ensure_subscription(topic_arn, url, arn)

    # VERIFY, do not assume. Every call above could return successfully while
    # the result is still wrong — a subscription pointing at the wrong queue,
    # say. Counting what actually exists is cheap and catches that.
    subscriptions = aws.sns().list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    subscribed = {s["Endpoint"] for s in subscriptions}
    expected = {aws.queue_arn(aws.queue_url(n)) for n in QUEUE_NAMES}

    missing = expected - subscribed
    if missing:
        log.error("these queues are NOT subscribed: %s", ", ".join(sorted(missing)))
        return 1

    log.info("bootstrap complete — %d queues subscribed to %r",
             len(expected), config.SNS_TOPIC_NAME)
    return 0


if __name__ == "__main__":
    # The EXIT CODE matters here in a way it does not for most scripts.
    # docker-compose gives the relay `condition: service_completed_successfully`,
    # which only releases it when this exits 0. So a failed bootstrap must stop
    # the relay from starting, rather than letting it publish into a topic with
    # no subscribers — where SNS would accept every message and discard it.
    sys.exit(main())
