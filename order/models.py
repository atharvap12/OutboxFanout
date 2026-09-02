"""The orders and outbox tables.

Both live in the same database, so one transaction covers both writes. There
is no moment where the order exists but its pending event does not — which is
the entire outbox pattern.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


def _utc_now() -> datetime:
    """Timezone-aware UTC. A naive datetime is a number with no units."""
    return datetime.now(timezone.utc)


class Order(Base):
    __tablename__ = "orders"

    # Generated in Python, not by the database: the id is needed to build the
    # outbox payload before either row is written.
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    item: Mapped[str] = mapped_column(String(255), nullable=False)

    # Numeric, never Float — binary floating point cannot represent 0.1
    # exactly, so 0.1 + 0.2 != 0.3. Unacceptable for money.
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )

    def __repr__(self) -> str:
        return f"<Order {self.id} {self.item!r} {self.amount}>"


class OutboxEvent(Base):
    """A pending message, written in the same transaction as its order.
    The relay publishes unpublished rows to SNS."""

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False
    )

    # Own column, not buried in the payload, so consumers can filter without
    # parsing the body.
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)

    # A full snapshot, not just the order_id. An event describes the past: a
    # consumer re-querying the orders table would read current state instead,
    # so an amended or deleted order means billing the wrong amount. It also
    # keeps consumers off the Order Service's schema — the event is the
    # contract.
    # JSONB over JSON: parsed once into binary, indexable, faster to read.
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # server_default so rows inserted outside this app also get false.
    published: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"), default=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )

    # Strictly redundant with `published`, kept because `WHERE published =
    # false` reads clearly and indexes well. The two must never disagree.
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Phase 6: parking a row the relay can never publish ---------------
    # Failed publish attempts. The relay stops selecting a row once this
    # reaches OUTBOX_MAX_ATTEMPTS — a dead-letter queue for the outbox table,
    # and the fix for head-of-line blocking (see relay/service.py).
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"), default=0
    )

    # Why it last failed, kept so a parked row can be diagnosed without
    # re-running it. Truncated: some botocore messages are enormous.
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # When it was parked. NULL means "still being retried"; a value means the
    # relay has given up and a human needs to look.
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # Partial index: contains only unpublished rows, and rows drop out of
        # it once marked published. Stays the size of the backlog rather than
        # of all history, which matters for a query the relay runs every 2s
        # forever.
        #
        # The predicate stays `published = false` and deliberately does NOT
        # mention `attempts`: baking a threshold into an index would mean
        # rebuilding the index to retune OUTBOX_MAX_ATTEMPTS. Postgres uses the
        # index for the published check and filters attempts afterwards.
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=text("published = false"),
        ),
    )

    def __repr__(self) -> str:
        state = "published" if self.published else "PENDING"
        return f"<OutboxEvent {self.id} {self.event_type} {state}>"
