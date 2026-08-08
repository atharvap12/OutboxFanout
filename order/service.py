"""The atomic write: order row and outbox event, staged as one unit."""

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from shared import config
from shared.log import get_logger

from order.models import Order, OutboxEvent, _utc_now
from order.schemas import OrderCreate

log = get_logger(__name__)

EVENT_TYPE_ORDER_CREATED = "OrderCreated"


def _build_payload(order: Order) -> dict[str, Any]:
    """Self-contained JSON snapshot of the order.

    amount is a string: JSON's only number type is float, so 499.99 would
    eventually round-trip imprecisely. created_at is ISO 8601 for the same
    reason — JSON has no date type.
    """
    return {
        "order_id": str(order.id),
        "customer_id": order.customer_id,
        "item": order.item,
        "amount": str(order.amount),
        "created_at": order.created_at.isoformat(),
    }


def create_order(session: Session, data: OrderCreate) -> Order:
    """Stage the order and its outbox event. Does NOT commit — the caller owns
    the transaction boundary (see routes.py), so this is reusable from a
    request, a bulk import, or a test that rolls back."""
    # id and created_at are set explicitly rather than left to the column
    # defaults: a SQLAlchemy `default=` is applied at flush time, so the
    # attributes would still be None here, and the payload needs both values.
    order = Order(
        id=uuid.uuid4(),
        customer_id=data.customer_id,
        item=data.item,
        amount=Decimal(data.amount),
        created_at=_utc_now(),
    )
    session.add(order)

    event = OutboxEvent(
        order_id=order.id,
        event_type=EVENT_TYPE_ORDER_CREATED,
        payload=_build_payload(order),
        published=False,
    )

    # Fault injection (Phase 1 STOP condition): null a NOT NULL column so
    # Postgres rejects the insert. A Python raise would only show that Python
    # stopped early; this proves the database discarded the orders insert too.
    if config.BREAK_OUTBOX_INSERT:
        log.warning("BREAK_OUTBOX_INSERT on — sabotaging the outbox row")
        event.event_type = None  # type: ignore[assignment]

    session.add(event)

    # Flush so constraint violations surface here, inside a transaction we can
    # still discard, rather than during commit.
    session.flush()

    log.info("staged order %s and outbox event %s (uncommitted)", order.id, event.id)
    return order
