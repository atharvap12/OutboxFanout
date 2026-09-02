"""Regression test for head-of-line blocking in the relay — the Phase 6 fix.

THE BUG (found in the Phase 2 quiz, deliberately left in until now):

relay_batch() selects `ORDER BY created_at` and, on any failure, ran a bare
`break`. So one row SNS will never accept — an oversized payload, say — sat
first in every batch forever and abandoned every healthy row behind it. The
outbox stopped draining permanently, while the relay logged an error every 2
seconds and otherwise looked perfectly healthy.

THE FIX: an `attempts` counter plus `failed_at`, so a row that keeps failing is
PARKED and stops being selected — a dead-letter queue for the outbox table,
which is why it belongs with the SQS DLQs in Phase 6. Known-permanent SNS
errors park on the first attempt instead of stalling the batch five more times.

This test builds the exact poison row that used to wedge the relay and proves a
healthy row queued behind it still gets published.
"""

import json

import pytest

from tests import helpers

# Over SNS's 256 KB limit. LocalStack enforces it and returns
# Code='InvalidParameter', which relay/service.py classifies as PERMANENT.
OVERSIZED_BYTES = 300 * 1024


@pytest.mark.slow
def test_poison_outbox_row_does_not_block_healthy_rows(restore_services):
    # The relay must be stopped while we build the poison row, or it would
    # publish the original (small, valid) payload before we could bloat it.
    helpers.compose("stop", "relay")
    helpers.wait_for(
        lambda: helpers.service_state("relay") == "exited",
        timeout=60, what="relay to stop",
    )

    # --- the poison row, created FIRST so it sorts first in every batch ----
    poison_order = helpers.create_order(customer_id="cust-poison", amount="1.00")
    helpers.sql(
        "UPDATE outbox SET payload = jsonb_set(payload, '{item}', %s::jsonb), "
        "published = false, published_at = NULL, attempts = 0, failed_at = NULL "
        "WHERE order_id = %s",
        (json.dumps("x" * OVERSIZED_BYTES), poison_order),
    )

    # --- a healthy row queued behind it ------------------------------------
    healthy_order = helpers.create_order(customer_id="cust-healthy", amount="2.00")

    assert helpers.outbox_row(poison_order)["published"] is False
    assert helpers.outbox_row(healthy_order)["published"] is False

    # --- act ----------------------------------------------------------------
    helpers.compose("up", "-d", "relay", env={"CRASH_AFTER_PUBLISH": "0"})

    # THE ASSERTION THAT WOULD HAVE FAILED BEFORE THE FIX. With the old bare
    # `break`, the poison row was first in every batch and this row was never
    # reached — not slowly, never.
    helpers.wait_for(
        lambda: helpers.outbox_row(healthy_order)["published"] is True,
        timeout=120,
        what="the healthy row to publish even though an unpublishable row sorts ahead of it",
    )

    # And it really was delivered, not just marked.
    helpers.wait_for_all_three(healthy_order, timeout=90)

    # --- the poison row is parked, not silently dropped --------------------
    row = helpers.wait_for(
        lambda: (r := helpers.outbox_row(poison_order))["failed_at"] is not None and r,
        timeout=120, what="the poison row to be parked",
    )

    assert row["published"] is False, (
        "a parked row must NOT be marked published — that would claim it was "
        "delivered when it never was, which is the silent loss this whole "
        "architecture exists to prevent"
    )
    assert row["attempts"] >= 1
    assert "InvalidParameter" in (row["last_error"] or ""), (
        f"expected the SNS error recorded for diagnosis, got {row['last_error']!r}"
    )

    # Parked on the FIRST attempt, because InvalidParameter is classified as
    # permanent. Without classification it would have burned all 5 attempts,
    # stalling the batch four more times than necessary.
    assert row["attempts"] == 1, (
        f"a known-permanent error should park immediately, took {row['attempts']} attempts"
    )

    # Parking is not deletion: the row is still there, with the reason, waiting
    # for a human. Clearing failed_at puts it back in the poll.
    assert helpers.outbox_row(poison_order) is not None
