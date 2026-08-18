"""HTTP layer. Translates requests; business logic lives in service.py."""

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
    summary="Create an order atomically with its outbox event",
)
def create_order(
    payload: OrderCreate,
    session: Session = Depends(get_session),
) -> Order:
    """Returns 201 as soon as the transaction commits — it does not wait for
    the relay, SNS, or any consumer. The event is already durable in the same
    database, so it cannot be lost despite not having been sent."""
    try:
        order = service.create_order(session, payload)

        # The transaction boundary. Committed here rather than in the
        # dependency because FastAPI runs dependency cleanup after the
        # response is sent, so a failure there could not change the status
        # code the client already received.
        session.commit()

    except IntegrityError as exc:
        # A constraint rejected the write; get_session has rolled back, so
        # neither row was stored. This is the branch the Phase 1 proof hits.
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


@router.get("/orders/{order_id}", response_model=OrderResponse, summary="Fetch one order")
def get_order(
    order_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> Order:
    order = session.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No such order")
    return order


# Inspection endpoints — debugging aids, not part of the product API.


@router.get("/outbox", response_model=list[OutboxEventResponse], summary="Peek at the outbox")
def list_outbox(
    unpublished_only: bool = False,
    limit: int = 50,
    session: Session = Depends(get_session),
) -> list[OutboxEvent]:
    stmt = select(OutboxEvent).order_by(OutboxEvent.created_at.desc()).limit(limit)
    if unpublished_only:
        # `== False`, not `.is_(False)`: the partial index is declared
        # `WHERE published = false`, and Postgres's predicate prover does not
        # recognise `IS false` as implying it — measured at a seq scan over
        # 50k rows instead of an index scan. Verified with EXPLAIN, see
        # VERIFY/VERIFY-PHASE-2.md.
        stmt = stmt.where(OutboxEvent.published == False)  # noqa: E712
    return list(session.scalars(stmt))


@router.get("/health", summary="Liveness probe")
def health() -> dict[str, str]:
    return {"status": "ok"}
