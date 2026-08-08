"""HTTP layer. Thin on purpose — it translates, it does not decide.

A route's job is: take the request, hand it to the service, turn the result
into a response. Business rules live in service.py. Keeping routes thin means
the same logic is reachable from a script or a test without pretending to be
a web request.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from shared.db import get_session
from shared.log import get_logger

from order import service
from order.models import Order, OutboxEvent
from order.schemas import OrderCreate, OrderResponse, OutboxEventResponse

log = get_logger(__name__)

router = APIRouter()


@router.post(
    "/orders",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an order (atomically, with its outbox event)",
)
def create_order(
    payload: OrderCreate,
    session: Session = Depends(get_session),
) -> Order:
    """Accept an order and record it.

    Returns 201 the moment the database transaction commits — we do NOT wait
    for the relay, SNS, or any consumer. That is the promise the outbox
    pattern lets us make honestly: the event is already durably recorded in
    the same database as the order, so it cannot be lost, even though it has
    not been sent yet.

    Compare with the naive design, where the endpoint would call SNS itself.
    Then a slow SNS makes your checkout slow, and an SNS outage makes your
    checkout fail — for an order the customer is quite happy to place.
    """
    try:
        order = service.create_order(session, payload)

        # THE transaction boundary. One commit, at the end, written by hand.
        #
        # Up to this instant, both rows exist only inside the transaction —
        # invisible to everyone else, and erasable. This line is the cashier
        # pressing CONFIRM. Afterwards both rows are real, together.
        #
        # It lives here, not in the dependency, because FastAPI runs a
        # dependency's cleanup AFTER the response has gone out. A commit that
        # failed down there would leave the customer holding a 201 for an
        # order that does not exist.
        session.commit()

    except IntegrityError as exc:
        # The database refused something: a NOT NULL, a foreign key, a
        # uniqueness rule. The session has already been rolled back by the
        # get_session dependency, so NOTHING was written — not the outbox
        # row, and not the order either.
        #
        # This is the branch our Phase 1 proof exercises.
        log.error("order rejected by database constraint: %s", exc.orig)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Order could not be stored; nothing was saved.",
        ) from exc

    except SQLAlchemyError as exc:
        log.exception("unexpected database failure while creating order")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable; nothing was saved.",
        ) from exc

    log.info("order %s committed with its outbox event", order.id)
    return order


@router.get(
    "/orders/{order_id}",
    response_model=OrderResponse,
    summary="Fetch one order",
)
def get_order(
    order_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> Order:
    order = session.get(Order, order_id)
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No such order"
        )
    return order


# ---------------------------------------------------------------------------
# Inspection endpoints.
#
# Not part of the product — these exist so you can watch the out-tray fill up
# and drain during Phases 2-6 without opening psql. The design doc asks for
# "visibility over cleverness"; this is that.
# ---------------------------------------------------------------------------


@router.get(
    "/outbox",
    response_model=list[OutboxEventResponse],
    summary="Peek at the outbox (debugging aid, not a real API)",
)
def list_outbox(
    unpublished_only: bool = False,
    limit: int = 50,
    session: Session = Depends(get_session),
) -> list[OutboxEvent]:
    stmt = select(OutboxEvent).order_by(OutboxEvent.created_at.desc()).limit(limit)
    if unpublished_only:
        stmt = stmt.where(OutboxEvent.published.is_(False))
    return list(session.scalars(stmt))


@router.get("/health", summary="Liveness probe")
def health() -> dict[str, str]:
    return {"status": "ok"}
