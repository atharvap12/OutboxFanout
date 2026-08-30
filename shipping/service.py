"""Ship an order exactly once, using the database as the arbiter."""

import uuid
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.db import session_scope
from shared.log import get_logger

from shipping.models import Shipment

log = get_logger(__name__)


def _tracking_number() -> str:
    return f"TRK-{uuid.uuid4().hex[:12].upper()}"


def handle(event: dict) -> None:
    """Process one OrderCreated event. Raises if it cannot be processed at all,
    which leaves the message on the queue for redelivery."""
    payload = event["payload"]
    order_id = uuid.UUID(payload["order_id"])
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
            .on_conflict_do_nothing(index_elements=["order_id"])
            # Returns the new id on insert, nothing on conflict. See the long
            # note in billing/service.py for why this is not rowcount.
            .returning(Shipment.id)
        )
        fresh = session.execute(stmt).scalar_one_or_none() is not None

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
