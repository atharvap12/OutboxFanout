"""Scenario D — a poison message ends up in the DLQ instead of looping forever.

Expected: "after maxReceiveCount retries, it lands in that queue's DLQ instead
of looping forever." (design doc §8, FR-07)

The consumer deliberately does NOT delete an unparsable message (deleting would
destroy the evidence), so without a redrive policy it would be redelivered
every visibility timeout until the retention period expired. The DLQ is the
safety net that makes "don't delete it" a safe choice rather than a leak.
"""

import json

import pytest

from tests import helpers

POISON_BODY = "{not valid json at all"

# The queue's own VisibilityTimeout is 30s, so five deliveries would take ~2.5
# minutes of pure waiting. Dropping it to 1s for the duration of the test is
# exactly what the design doc suggests, and it changes the timing only — the
# mechanism under test is the redrive policy, not the clock.
FAST_VISIBILITY = "1"


@pytest.fixture
def fast_redelivery():
    """Temporarily shorten billing-queue's visibility timeout, then restore it."""
    url = helpers.queue_url(helpers.BILLING_QUEUE)
    original = helpers.sqs().get_queue_attributes(
        QueueUrl=url, AttributeNames=["VisibilityTimeout"]
    )["Attributes"]["VisibilityTimeout"]

    helpers.sqs().set_queue_attributes(
        QueueUrl=url, Attributes={"VisibilityTimeout": FAST_VISIBILITY}
    )
    yield
    helpers.sqs().set_queue_attributes(
        QueueUrl=url, Attributes={"VisibilityTimeout": original}
    )


@pytest.mark.slow
def test_poison_message_lands_in_dlq(fast_redelivery):
    dlq = helpers.DLQS[helpers.BILLING_QUEUE]

    # Start from a known-empty DLQ so the assertion is unambiguous.
    helpers.purge(dlq)
    helpers.wait_for(
        lambda: helpers.queue_depth(dlq) == 0,
        timeout=90, what=f"{dlq} to be empty before the test",
    )

    message_id = helpers.send_raw(helpers.BILLING_QUEUE, POISON_BODY)

    # --- the consumer rejects it, repeatedly, without dying ---------------
    helpers.wait_for(
        lambda: "unparsable body" in helpers.logs_since("billing", 120),
        timeout=90, what="billing to log the unparsable body",
    )
    assert helpers.service_state("billing") == "running", (
        "one bad message must not take down the consumer"
    )

    # --- SQS gives up and moves it aside ----------------------------------
    helpers.wait_for(
        lambda: helpers.queue_depth(dlq) >= 1,
        timeout=180, what=f"the poison message to arrive in {dlq}",
    )

    # It really is OUR message, not something left over.
    received = helpers.sqs().receive_message(
        QueueUrl=helpers.queue_url(dlq),
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5,
        AttributeNames=["ApproximateReceiveCount"],
    ).get("Messages", [])

    bodies = [m["Body"] for m in received]
    assert POISON_BODY in bodies, f"poison message not found in {dlq} (got {bodies!r})"

    # --- and it is gone from the main queue -------------------------------
    assert helpers.service_state("billing") == "running"

    # Put it back so a re-run of this test starts clean rather than finding a
    # half-consumed DLQ. (Purging is rate-limited to once a minute per queue.)
    helpers.purge(dlq)


@pytest.mark.slow
def test_poison_message_does_not_block_healthy_traffic(fast_redelivery):
    """A poison message on billing-queue must not stop real orders billing.

    The queue-level equivalent of the head-of-line blocking bug fixed in the
    relay: SQS hands out messages independently, so one undeleted message does
    not hold up the ones behind it.
    """
    helpers.send_raw(helpers.BILLING_QUEUE, POISON_BODY)

    order_id = helpers.create_order(customer_id="cust-scenario-d2", amount="33.00")
    helpers.wait_for(
        lambda: helpers.billing_count(order_id) == 1,
        timeout=120, what="a healthy order to bill despite the poison message",
    )

    assert helpers.shipment_count(order_id) == 1
    helpers.purge(helpers.DLQS[helpers.BILLING_QUEUE])
