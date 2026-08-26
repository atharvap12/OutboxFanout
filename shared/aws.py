"""boto3 clients pointed at LocalStack (or real AWS, unchanged).

The only difference between this project and production is the endpoint URL.
Nothing here is LocalStack-specific in structure: drop AWS_ENDPOINT_URL and the
same code talks to real SNS and SQS. That is the whole reason the endpoint is
configuration rather than a hardcoded string.
"""

from functools import lru_cache

import boto3
from botocore.config import Config

from shared import config

# Retry policy applied to every client.
#
# "standard" mode retries throttling and transient 5xx errors with exponential
# backoff. Worth being deliberate about: the relay's at-least-once guarantee
# depends on publish failures being *reported*, not silently swallowed. A
# publish that raises after exhausting retries leaves the outbox row
# unpublished, so the next poll picks it up again. That is correct behaviour —
# the row is the source of truth, not the SNS call.
_BOTO_CONFIG = Config(
    region_name=config.AWS_REGION,
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=10,
)


def _client(service_name: str):
    """Build a boto3 client with our endpoint and credentials."""
    return boto3.client(
        service_name,
        endpoint_url=config.AWS_ENDPOINT_URL,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
        config=_BOTO_CONFIG,
    )


# lru_cache gives one client per process. boto3 clients are expensive to build
# (they parse a service model on creation) and are thread-safe once built, so
# reusing one is both faster and correct. Creating a client per loop iteration
# is a classic, quietly expensive mistake.
@lru_cache(maxsize=1)
def sns():
    """Shared SNS client."""
    return _client("sns")


@lru_cache(maxsize=1)
def sqs():
    """Shared SQS client."""
    return _client("sqs")


# ---------------------------------------------------------------------------
# Name -> identifier lookups
#
# ARNs and queue URLs are never hardcoded. LocalStack's free tier forgets every
# topic and queue on restart, so identifiers change; we always resolve them
# from the stable *name* at runtime.
# ---------------------------------------------------------------------------

def topic_arn(name: str) -> str:
    """ARN for a topic, creating it if it does not exist.

    create_topic is idempotent in the AWS API: calling it for a name that
    already exists returns the existing ARN rather than erroring. That is what
    makes a re-runnable bootstrap possible, and it is why this is safe to call
    on every start.
    """
    return sns().create_topic(Name=name)["TopicArn"]


def queue_url(name: str) -> str:
    """URL for an existing queue.

    Deliberately does NOT create it. A consumer discovering that its queue is
    missing should fail loudly — silently creating one would mean it sits
    polling an empty queue that nothing is subscribed to, looking healthy while
    processing nothing. Queue creation belongs to the bootstrap step.
    """
    return sqs().get_queue_url(QueueName=name)["QueueUrl"]


def queue_arn(url: str) -> str:
    """ARN for a queue, given its URL.

    One queue, two identifiers, and they are NOT interchangeable:

        URL   what you receive and delete against  (an https address)
        ARN   what SNS subscribes to, and what IAM policies name

    Nothing converts one into the other by string manipulation — you have to
    ask. This is that ask.
    """
    attributes = sqs().get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]
    return attributes["QueueArn"]
