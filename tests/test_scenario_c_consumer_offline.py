"""Scenario C — one consumer offline, the others completely unaffected.

Expected: "Billing and Shipping process normally and immediately. notify-queue
quietly accumulates messages. When the consumer restarts, it drains the backlog
with no message loss." (design doc §8)

This is what fanning out through SNS buys over having the relay call three
consumers itself. Each queue holds its own durable copy with its own delivery
state and no shared cursor, so one consumer being down is not an event to the
others — it is not even visible to them.
"""

import pytest

from tests import helpers

ORDERS_WHILE_DOWN = 3


@pytest.mark.slow
def test_notifications_offline_others_unaffected(restore_services):
    helpers.compose("stop", "notifications")
    helpers.wait_for(
        lambda: helpers.service_state("notifications") == "exited",
        timeout=60, what="notifications to stop",
    )

    # Orders keep flowing while it is down.
    order_ids = [
        helpers.create_order(customer_id=f"cust-scenario-c-{n}", amount=f"{n + 1}.00")
        for n in range(ORDERS_WHILE_DOWN)
    ]

    # --- the healthy consumers do not care -------------------------------
    for order_id in order_ids:
        helpers.wait_for(
            lambda oid=order_id: helpers.billing_count(oid) == 1 and helpers.shipment_count(oid) == 1,
            timeout=90, what=f"billing and shipping to process {order_id} with notify down",
        )

    # --- the offline consumer's work is waiting, not lost -----------------
    for order_id in order_ids:
        assert helpers.notify_key(order_id) is None, "notifications processed while stopped?"

    assert helpers.queue_depth(helpers.NOTIFY_QUEUE) >= ORDERS_WHILE_DOWN, (
        "notify-queue should be holding the backlog"
    )

    # Nothing is blocked upstream either: the relay published every row and
    # moved on. A consumer being down must not back-pressure the producer.
    for order_id in order_ids:
        assert helpers.outbox_row(order_id)["published"] is True

    # --- restart: the backlog drains itself -------------------------------
    helpers.compose("up", "-d", "notifications", env={"CRASH_AFTER_MARK": "0"})

    for order_id in order_ids:
        helpers.wait_for(
            lambda oid=order_id: helpers.notify_key(oid) is not None,
            timeout=120, what=f"notifications to drain {order_id} after restart",
        )

    # Every order notified exactly once, and no double-processing of the ones
    # billing/shipping had already handled.
    for order_id in order_ids:
        assert helpers.billing_count(order_id) == 1
        assert helpers.shipment_count(order_id) == 1
        assert len(helpers.rds().keys(f"notify:processed:{order_id}")) == 1
