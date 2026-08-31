"""The shipments table.

Same mechanism as billing_records, DIFFERENT COLUMNS — and the difference is
the interesting part.

    ONE EVENT, TWO READERS, TWO DIFFERENT NOTES TAKEN.

    A single announcement goes out: "order 47: Standing desk, £899, customer
    Ada." The accounts clerk writes down the amount and the customer. The
    warehouse writes down the item and a tracking number. Neither writes down
    the other's fields, because neither needs them.

Shipping never stores `amount`. It has no opinion about money and no reason to
know. If the finance team later changes how amounts are represented, this table
does not care — which is what "independent services" actually buys you, stated
in terms of a schema rather than a slogan.

The dedup reasoning (why UNIQUE, why order_id, why never a transport id, why no
foreign key to orders) is written out in full in billing/models.py.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


class Shipment(Base):
    """One row per order shipped. Two rows for one order is impossible."""

    __tablename__ = "shipments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # ------------------------------------------------------------------
    # Shipping's OWN idempotency key, on Shipping's OWN table.
    #
    # Worth stating plainly: this constraint and the one on billing_records are
    # two separate constraints that happen to be spelled the same way. They
    # share nothing. If somebody deleted every billing_records row tomorrow,
    # Shipping would still refuse to ship order 47 twice, because its evidence
    # lives in its own table.
    #
    # That is the design doc's "each consumer owns its own idempotency store".
    # A single shared dedup table would be less code and would couple three
    # independent services through one row — turning any one consumer's bad
    # deploy into everyone's outage.
    # ------------------------------------------------------------------
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, unique=True
    )

    # Tracing only, never the dedup key. See billing/models.py.
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    item: Mapped[str] = mapped_column(String(255), nullable=False)

    # A generated value, which raises a question worth answering: a duplicate
    # delivery generates a SECOND tracking number in Python before the INSERT
    # runs — so does the customer end up with two?
    #
    # No. The number is generated, handed to a statement that Postgres then
    # REFUSES, and thrown away with the rest of the rejected row. Only the
    # winning insert's value is ever persisted or logged as real.
    #
    # The general shape: it is fine to do throwaway work before the claim, as
    # long as the claim is the only thing with a side effect. It would NOT be
    # fine to call a courier's API here — that is the Phase 5 problem, where
    # the side effect escapes the database and cannot be rolled back.
    tracking_number: Mapped[str] = mapped_column(String(32), nullable=False)

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<Shipment order={self.order_id} tracking={self.tracking_number}>"
