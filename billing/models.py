"""The billing_records table.

This table is BOTH the dedup store and the side effect. "Charging the
customer" is simulated by inserting the row, so the thing that proves we have
not billed twice is the same write as the billing itself — one row, one
constraint, one transaction, no gap. That is the case where a Postgres UNIQUE
constraint beats an application-level check, and why the design doc assigns it
to Billing rather than to Notifications.

No foreign key to orders: Billing is a separate service that happens to share
a Postgres instance here for convenience. A FK would couple it to the Order
Service's schema and make the event contract a lie.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


class BillingRecord(Base):
    __tablename__ = "billing_records"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # The idempotency key, and the entire mechanism. UNIQUE means the DATABASE
    # refuses a second billing for this order — not a code path that could be
    # forgotten. Keyed on order_id, not event_id: the rule is "bill this order
    # once", however many OrderCreated events happen to arrive.
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, unique=True
    )

    # Which outbox event produced this row. Traceability only. Never the dedup
    # key: a relay republish reuses it, but SNS and SQS both mint fresh ids per
    # delivery, so any transport id would dedupe nothing.
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)

    # Numeric, never Float — see the same reasoning on orders.amount. Arrives
    # as a JSON string and is parsed back to Decimal here.
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<BillingRecord order={self.order_id} amount={self.amount}>"
