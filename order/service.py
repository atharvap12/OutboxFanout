"""The atomic write. This is the heart of Phase 1.

Everything else in this service is plumbing around these few lines.
"""

from decimal import Decimal
from typing import Any
import uuid

from sqlalchemy.orm import Session

from shared import config
from shared.log import get_logger

from order.models import Order, OutboxEvent, _utc_now
from order.schemas import OrderCreate

log = get_logger(__name__)

EVENT_TYPE_ORDER_CREATED = "OrderCreated"


def _build_payload(order: Order) -> dict[str, Any]:
    """Freeze the order into a plain, self-contained JSON snapshot.

    Two conversions here that look fussy but are not:

    amount -> str
        JSON has exactly one number type, and it is a float. Writing
        Decimal("499.99") into JSON as a number would turn it into
        499.99000000000001 somewhere down the line. Sending it as the STRING
        "499.99" preserves it exactly; the consumer parses it back into a
        Decimal. Money travels as text.

    created_at -> ISO 8601 string
        JSON has no date type either. ISO format ("2026-08-07T09:15:00+00:00")
        is unambiguous, sorts correctly as plain text, and every language can
        parse it.

    The snapshot deliberately duplicates data that already exists in the
    orders table. That duplication IS the feature: this is a photograph of the
    order at this instant, and a photograph doesn't change when the subject
    does.
    """
    return {
        "order_id": str(order.id),
        "customer_id": order.customer_id,
        "item": order.item,
        "amount": str(order.amount),
        "created_at": order.created_at.isoformat(),
    }


def create_order(session: Session, data: OrderCreate) -> Order:
    """Write the order and its outbox event as one indivisible unit.

    IMPORTANT: this function does not commit. It stages both rows and hands
    control back. The caller owns the transaction boundary — see routes.py.

    Why that split? A function that commits can only ever be used one way. A
    function that stages can be composed: the same code works inside a web
    request, a bulk import, or a test that rolls everything back afterwards.
    """
    # 1. Build the order. Note that we pass id AND created_at explicitly,
    #    rather than letting the column defaults supply them.
    #
    #    Why: a `default=` on a SQLAlchemy column is a FLUSH-time default, not
    #    a constructor default. SQLAlchemy fills it in at the moment it writes
    #    the row — so right after Order(...) the attribute is still None.
    #    Since we need both values NOW to build the event payload, we set them
    #    ourselves. The column defaults stay as a safety net for any other
    #    code path that creates an Order.
    #
    #    The general lesson: "it has a default" and "it has a value yet" are
    #    two different questions. A form can have a box marked "office use
    #    only — filled at submission" and still be blank in your hand.
    order = Order(
        id=uuid.uuid4(),
        customer_id=data.customer_id,
        item=data.item,
        amount=Decimal(data.amount),
        created_at=_utc_now(),
    )

    # 2. Stage it. Nothing has reached the database yet.
    #    session.add() is putting a form in your folder, not filing it.
    session.add(order)

    # 3. Build the delivery note describing what just happened.
    event = OutboxEvent(
        order_id=order.id,
        event_type=EVENT_TYPE_ORDER_CREATED,
        payload=_build_payload(order),
        published=False,
    )

    # ------------------------------------------------------------------
    # FAULT INJECTION — Phase 1 STOP condition.
    #
    # Setting a NOT NULL column to None makes POSTGRES reject the second
    # insert. We deliberately do NOT just `raise` here: a Python exception
    # would only prove that Python stopped early. Letting the database refuse
    # the row proves the database threw away the FIRST insert too.
    #
    # Turn on with BREAK_OUTBOX_INSERT=1.
    # ------------------------------------------------------------------
    if config.BREAK_OUTBOX_INSERT:
        log.warning(
            "BREAK_OUTBOX_INSERT is on — sabotaging the outbox row on purpose. "
            "The orders row must disappear with it."
        )
        event.event_type = None  # type: ignore[assignment]

    session.add(event)

    # 4. Send both INSERTs to the database, still inside the transaction.
    #
    #    flush() is the crucial move. Without it, SQLAlchemy would hold both
    #    rows until commit, and a constraint violation would surface during
    #    the commit — harder to reason about. Flushing here means the database
    #    checks NOT NULL, foreign keys, and types NOW, while we are still
    #    safely inside a transaction we can throw away.
    #
    #    Flush is the cashier typing both items into the till. The total is
    #    real, errors show up immediately — but nothing is paid for yet, and
    #    walking away still voids everything.
    session.flush()

    log.info(
        "staged order %s and outbox event %s (not committed yet)",
        order.id,
        event.id,
    )
    return order
