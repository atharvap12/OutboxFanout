"""Scenario B — the same event delivered several times on purpose.

Scenario A produces one duplicate as a side effect of a crash. This one attacks
the idempotency checks directly: deliver the same event repeatedly and confirm
the side effects never multiply.

Expected: "exactly one billing_record row, one shipment row, and the
Redis-marked notification — never two." (design doc §8)
"""

import pytest

from tests import helpers

REDELIVERIES = 3


@pytest.mark.slow
def test_repeated_delivery_produces_one_side_effect_each():
    order_id = helpers.create_order(customer_id="cust-scenario-b", amount="55.00")
    helpers.wait_for_all_three(order_id, timeout=60)

    first_key = helpers.notify_key(order_id)
    assert first_key is not None

    # The dedup key stores the CLAIMING event_id, and the outbox row id does
    # not change when a row is republished. So every redelivery below carries
    # the same event_id — which is exactly why deduping on it works, and why
    # deduping on an SNS or SQS MessageId would not.
    outbox_id = helpers.outbox_row(order_id)["id"]
    assert first_key == outbox_id, "the Redis key should record the outbox row id"

    for attempt in range(1, REDELIVERIES + 1):
        helpers.republish(order_id)
        helpers.wait_for(
            lambda: helpers.outbox_row(order_id)["published"] is True,
            timeout=90, what=f"relay to republish (delivery {attempt + 1})",
        )
        helpers.wait_for(
            lambda: "DUPLICATE" in helpers.logs_since("billing", 60),
            timeout=90, what=f"billing to log duplicate {attempt}",
        )

        assert helpers.billing_count(order_id) == 1, f"billed twice on delivery {attempt + 1}"
        assert helpers.shipment_count(order_id) == 1, f"shipped twice on delivery {attempt + 1}"
        assert len(helpers.rds().keys(f"notify:processed:{order_id}")) == 1

    # The claim never changed hands: the first delivery won and every later one
    # was turned away. If the notify consumer had ever released and re-taken
    # the key, a second email would have gone out.
    assert helpers.notify_key(order_id) == first_key, "the Redis claim was overwritten"


@pytest.mark.slow
def test_duplicate_delivered_straight_to_one_queue():
    """Bypass SNS entirely and put a second copy directly on billing-queue.

    Scenario A and the test above both duplicate at the RELAY. This duplicates
    at the QUEUE, which is what an SQS visibility-timeout redelivery looks
    like — a different path to the same place, and it must be equally harmless.
    """
    order_id = helpers.create_order(customer_id="cust-scenario-b2", amount="21.00")
    helpers.wait_for_all_three(order_id, timeout=60)

    # Rebuild the exact SNS envelope shape the consumers expect. RawMessageDelivery
    # is off, so the body is SNS's wrapper with our JSON as a STRING inside
    # "Message" — see shared/messages.py.
    import json

    envelope = helpers.outbox_row(order_id)
    inner = {
        "event_id": envelope["id"],
        "event_type": "OrderCreated",
        "order_id": order_id,
        "occurred_at": "2026-01-01T00:00:00Z",
        "payload": {
            "order_id": order_id,
            "customer_id": "cust-scenario-b2",
            "item": "Test widget",
            "amount": "21.00",
        },
    }
    body = json.dumps({
        "Type": "Notification",
        "MessageId": "manually-injected-duplicate",
        "TopicArn": "arn:aws:sns:us-east-1:000000000000:order-events",
        "Message": json.dumps(inner),
    })

    helpers.send_raw(helpers.BILLING_QUEUE, body)

    helpers.wait_for(
        lambda: "already billed" in helpers.logs_since("billing", 60),
        timeout=90, what="billing to catch the injected duplicate",
    )
    assert helpers.billing_count(order_id) == 1, "billed twice from a queue-level duplicate"
