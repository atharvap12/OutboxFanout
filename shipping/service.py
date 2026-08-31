"""Ship an order exactly once, letting the database be the judge.

Structurally identical to billing/service.py, which carries the full
explanation of ON CONFLICT DO NOTHING, why the check must live inside the
claim, and why RETURNING is used instead of rowcount. Read that one first.

What this file exists to prove: NONE OF THAT WAS SPECIFIC TO BILLING. A second
consumer, a second queue, a second table, the same six lines of mechanism — and
the two never coordinate.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.db import session_scope
from shared.log import get_logger

from shipping.models import Shipment

log = get_logger(__name__)


def _tracking_number() -> str:
    """A fake courier tracking number, standing in for a real integration."""
    return f"TRK-{uuid.uuid4().hex[:12].upper()}"


def handle(event: dict) -> None:
    """Process one OrderCreated event, or recognise it as one we already did."""
    payload = event["payload"]
    order_id = uuid.UUID(payload["order_id"])

    # Generated before the claim, and discarded if the claim loses. Harmless
    # precisely because generating it has no side effect — see the note on the
    # tracking_number column in models.py.
    tracking = _tracking_number()

    with session_scope() as session:
        stmt = (
            pg_insert(Shipment)
            .values(
                id=uuid.uuid4(),
                order_id=order_id,
                event_id=uuid.UUID(event["event_id"]),
                customer_id=payload["customer_id"],
                item=payload["item"],
                tracking_number=tracking,
                processed_at=datetime.now(timezone.utc),
            )
            # Check and claim in one statement; no window between them.
            .on_conflict_do_nothing(index_elements=["order_id"])
            # Returns the new id on insert, nothing on conflict. NOT rowcount,
            # which is -1 ("unavailable") on this driver — the long version of
            # that story is in billing/service.py.
            .returning(Shipment.id)
        )
        fresh = session.execute(stmt).scalar_one_or_none() is not None

    # Outside the transaction, so we only claim work that actually committed.
    if fresh:
        log.info(
            "📦 SHIPPED order %s — %r tracking %s (event %s)",
            order_id, payload["item"], tracking, event["event_id"],
        )
    else:
        log.info(
            "🔁 DUPLICATE ignored for order %s — already shipped (event %s)",
            order_id, event["event_id"],
        )
