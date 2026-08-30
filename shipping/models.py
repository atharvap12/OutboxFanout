"""The shipments table.

Same mechanism as billing_records, different columns — which is the point.
Shipping stores what shipping cares about (the item, a tracking number) and
never sees `amount`. Two consumers, two independent projections of one event,
two independent dedup stores. Nothing is shared but the message.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


class Shipment(Base):
    __tablename__ = "shipments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # The idempotency key: one shipment per order, enforced by the database.
    # Note Billing has its own copy of this constraint on its own table —
    # deleting a billing_records row must never let Shipping ship twice.
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, unique=True
    )

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    item: Mapped[str] = mapped_column(String(255), nullable=False)

    # Generated on every attempt, but only the winning INSERT persists one —
    # so a duplicate delivery cannot mint a second tracking number.
    tracking_number: Mapped[str] = mapped_column(String(32), nullable=False)

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<Shipment order={self.order_id} tracking={self.tracking_number}>"
