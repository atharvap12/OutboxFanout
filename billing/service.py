"""Bill an order exactly once, using the database as the arbiter."""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.db import session_scope
from shared.log import get_logger

from billing.models import BillingRecord

log = get_logger(__name__)


def handle(event: dict) -> None:
    """Process one OrderCreated event. Raises if it cannot be processed at all,
    which leaves the message on the queue for redelivery."""
    payload = event["payload"]
    order_id = uuid.UUID(payload["order_id"])

    with session_scope() as session:
        stmt = (
            pg_insert(BillingRecord)
            .values(
                id=uuid.uuid4(),
                order_id=order_id,
                event_id=uuid.UUID(event["event_id"]),
                customer_id=payload["customer_id"],
                # str -> Decimal. The relay shipped it as a string precisely so
                # this parse is exact; Decimal(float) would not be.
                amount=Decimal(payload["amount"]),
                processed_at=datetime.now(timezone.utc),
            )
            # The check and the claim are ONE statement. An "does it exist?"
            # SELECT followed by an INSERT has a window between them where a
            # second consumer can slip through; ON CONFLICT has no window
            # because the uniqueness test happens inside the insert itself.
            # Same idea as SELECT ... FOR UPDATE in the relay and SET NX in
            # Phase 5: do the check inside the thing that claims.
            .on_conflict_do_nothing(index_elements=["order_id"])
            # RETURNING is how we learn which branch happened. DO NOTHING
            # raises no exception either way, so without this the two outcomes
            # are indistinguishable. An insert returns one row; a conflict
            # returns none.
            #
            # NOT result.rowcount: on this driver it comes back as -1
            # ("unavailable") for INSERT, so `rowcount == 1` is never true and
            # every event would be misreported as a duplicate. Measured, not
            # assumed — see VERIFY/VERIFY-PHASE-4.md.
            .returning(BillingRecord.id)
        )
        fresh = session.execute(stmt).scalar_one_or_none() is not None

    if fresh:
        log.info(
            "💳 BILLED order %s — %s %s (event %s)",
            order_id, payload["customer_id"], payload["amount"], event["event_id"],
        )
    else:
        log.info(
            "🔁 DUPLICATE ignored for order %s — already billed (event %s)",
            order_id, event["event_id"],
        )
