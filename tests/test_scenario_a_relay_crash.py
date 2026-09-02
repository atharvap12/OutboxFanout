"""Scenario A — the relay crashes between publishing and marking the row.

"This is the single most important proof in the whole project." (design doc §8)

The relay publishes to SNS, then marks the outbox row published. Those two
steps cannot share a transaction, so a crash in the gap leaves a row that WAS
published but still looks unsent. On restart it is published again.

That duplicate is not a bug — it is the deliberate consequence of choosing
at-least-once over at-most-once. What this test proves is that the duplicate is
HARMLESS: all three consumers no-op on it, using two different mechanisms.
"""

import pytest

from tests import helpers


@pytest.mark.slow
def test_relay_crash_between_publish_and_mark(restore_services):
    # --- arrange: a relay armed to die in the gap -------------------------
    helpers.compose("up", "-d", "relay", env={"CRASH_AFTER_PUBLISH": "1"})
    helpers.wait_for(
        lambda: helpers.service_state("relay") == "running",
        timeout=60, what="relay to start with CRASH_AFTER_PUBLISH=1",
    )

    order_id = helpers.create_order(customer_id="cust-scenario-a", amount="99.99")

    # --- act 1: the crash --------------------------------------------------
    helpers.wait_for(
        lambda: helpers.service_state("relay") == "exited",
        timeout=60, what="relay to crash after publishing",
    )

    # 17, not 1: a dedicated exit code proves the process died exactly where we
    # aimed it, rather than falling over for some unrelated reason.
    assert helpers.exit_code("relay") == 17, "relay should exit 17 at the injected crash"

    row = helpers.outbox_row(order_id)
    assert row is not None
    assert row["published"] is False, (
        "the outbox row must still look UNSENT — that is what makes the "
        "republish happen, and it is the whole mechanism under test"
    )

    # The message is nevertheless already in the queues: the publish happened
    # before the crash. This is the inconsistency the outbox pattern converts
    # a silent loss into.
    helpers.wait_for_all_three(order_id, timeout=60)

    # --- act 2: restart, and the row is published a second time ------------
    helpers.compose("up", "-d", "relay", env={"CRASH_AFTER_PUBLISH": "0"})
    helpers.wait_for(
        lambda: helpers.outbox_row(order_id)["published"] is True,
        timeout=90, what="the restarted relay to republish and mark the row",
    )

    # --- assert: one logical event, one side effect each -------------------
    # Give the consumers time to receive and dedupe the second copy. Polling
    # for "still 1" is not possible, so wait for the duplicate to be LOGGED and
    # then assert the counts.
    helpers.wait_for(
        lambda: "DUPLICATE" in helpers.logs_since("billing", 300),
        timeout=90, what="billing to log the duplicate",
    )

    assert helpers.billing_count(order_id) == 1, "double billing — the exact thing this prevents"
    assert helpers.shipment_count(order_id) == 1, "shipped twice"

    keys = helpers.rds().keys(f"notify:processed:{order_id}")
    assert len(keys) == 1, "more than one dedup key for one order"

    # And the duplicate was visibly *caught*, not merely absent. Asserting only
    # on the counts would pass even if the event had never arrived at all.
    for service, marker in (
        ("billing", "already billed"),
        ("shipping", "already shipped"),
        ("notifications", "already notified"),
    ):
        logs = helpers.logs_since(service, 300)
        assert marker in logs, f"{service} did not log catching the duplicate"
