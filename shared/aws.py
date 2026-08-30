"""boto3 clients for SNS and SQS.

endpoint_url is the only thing making this LocalStack rather than real AWS.
"""

from functools import lru_cache

import boto3
from botocore.config import Config

from shared import config

# Retries cover throttling and transient 5xx. Note they are also a source of
# duplicates: if SNS accepts a publish but the ack is lost, the retry delivers
# it twice. Standard SNS topics have no deduplication.
# A publish that fails after all attempts must raise, so the outbox row stays
# unpublished and the next poll retries it.
_BOTO_CONFIG = Config(
    region_name=config.AWS_REGION,
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=10,
)


def _client(service_name: str):
    return boto3.client(
        service_name,
        endpoint_url=config.AWS_ENDPOINT_URL,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
        config=_BOTO_CONFIG,
    )


# Cached: constructing a client parses a service model (~7ms) and the result
# is thread-safe. Lazy via a function so importing this module costs nothing.
@lru_cache(maxsize=1)
def sns():
    return _client("sns")


# SQS needs its own config: long polling holds the HTTP response open for
# SQS_WAIT_TIME_SECONDS, so a read_timeout shorter than that turns every
# receive into a ReadTimeoutError — the connection is fine, we just hung up
# before the server was done waiting. The margin covers the round trip.
_SQS_CONFIG = _BOTO_CONFIG.merge(
    Config(read_timeout=config.SQS_WAIT_TIME_SECONDS + 10)
)


@lru_cache(maxsize=1)
def sqs():
    return boto3.client(
        "sqs",
        endpoint_url=config.AWS_ENDPOINT_URL,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
        config=_SQS_CONFIG,
    )


def topic_arn(name: str) -> str:
    """ARN for a topic, creating it if absent.

    create_topic is idempotent — an existing name returns its ARN — which is
    what makes a re-runnable bootstrap possible.
    """
    return sns().create_topic(Name=name)["TopicArn"]


def queue_url(name: str) -> str:
    """URL for an existing queue. Deliberately does not create it.

    Auto-creating would leave a consumer polling an unsubscribed queue: green
    healthchecks, zero messages, no error. Failing loudly is better. Queue
    creation belongs to the bootstrap step.
    """
    return sqs().get_queue_url(QueueName=name)["QueueUrl"]


def queue_arn(url: str) -> str:
    """ARN for a queue, given its URL.

    Two identifiers for one queue, and they are not interchangeable: you
    RECEIVE and DELETE against the URL, but SNS subscribes and IAM policies
    reference the ARN. This is the only way to get from one to the other.
    """
    attributes = sqs().get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]
    return attributes["QueueArn"]
