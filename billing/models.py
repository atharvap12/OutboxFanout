"""The billing_records table — and why a UNIQUE constraint is the right tool.

THE ONE THING THAT MAKES THIS TABLE SPECIAL: the row IS the side effect.

"Charging the customer" is simulated by inserting a billing_records row. So the
write that PROVES we have not billed twice and the write that CONSTITUTES the
billing are the same statement, in the same transaction. There is no gap
between "mark it done" and "do it" for a crash to fall into, because they are
one act.

    A NIGHTCLUB DOOR. The bouncer has a guest list. He does not (a) check the
    list, then (b) let you in, then (c) tick your name — three steps with two
    gaps where a power cut lets you walk in twice. He ticks and admits in one
    motion, and the pen physically cannot write your name twice on the same
    line.

    That "cannot write it twice" is the UNIQUE constraint. Postgres refuses,
    at the storage level, no matter which process asks or how many ask at once.

WHY THIS BEATS AN APPLICATION-LEVEL CHECK

    if not already_billed(order_id):     # SELECT
        charge(order_id)                 # <-- two consumers can BOTH be here
        mark_billed(order_id)            # INSERT

Between the SELECT and the INSERT there is a window. Two consumers handling the
same duplicate can both look, both see nothing, and both charge. The window is
small — microseconds — which is precisely what makes it a bug you cannot
reproduce on demand and will meet in production at 3am.

A UNIQUE constraint has no window. Postgres serialises the two inserts itself;
one wins and one is rejected. And the check CANNOT BE FORGOTTEN: a new code
path that inserts without checking still hits the constraint. Compare the Redis
approach in Phase 5, where forgetting to check is a silent double-send.

Reference: PostgreSQL — Unique Constraints —
https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINTS
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


class BillingRecord(Base):
    """One row per order billed. Two rows for one order is impossible."""

    __tablename__ = "billing_records"

    # Our own primary key. NOT order_id — a table's identity and a business
    # rule are different things, and conflating them means you could never
    # store two rows per order even if the business later wanted to (refunds,
    # adjustments). The uniqueness we want is expressed separately, below.
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # ------------------------------------------------------------------
    # THE IDEMPOTENCY KEY. This one line is the entire Phase 4 mechanism.
    #
    # `unique=True` tells Postgres to build a unique index and reject any
    # second row with the same order_id. Not "the application checks" —
    # THE DATABASE REFUSES.
    #
    # WHY order_id AND NOT event_id: the business rule is "bill this order
    # once", not "handle this message once". If the relay republishes the same
    # outbox row three times, that is three deliveries of ONE event and the
    # order_id is identical in all three — so we catch it. Keying on event_id
    # would also work for that case, but would let a *different* event about
    # the same order bill it a second time, which is not what "don't double-
    # bill" means.
    #
    # AND NEVER, EVER A TRANSPORT ID. The SNS MessageId is minted fresh on
    # every publish; the SQS MessageId is minted fresh per queue per copy. Both
    # change when the exact thing you are trying to detect happens, so both
    # would dedupe precisely nothing. THE DEDUP KEY MUST COME FROM YOUR DOMAIN,
    # NOT FROM THE PIPE IT TRAVELLED THROUGH.
    # ------------------------------------------------------------------
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, unique=True
    )

    # Which outbox event produced this row. FOR TRACING ONLY — never the dedup
    # key. Storing it means that when you find a duplicate in the logs you can
    # answer "was this one event sent twice, or two different events?" without
    # guessing.
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)

    # Numeric, never Float — the same reasoning as orders.amount. Floats are
    # binary and 0.1 has no exact binary form, so 0.1 + 0.2 is
    # 0.30000000000000004. Tolerable for a temperature reading, not for a bill.
    #
    # It arrives from JSON as the STRING "899.00" precisely so that parsing it
    # back to Decimal here is exact. Decimal(str) is exact; Decimal(float) is
    # not, because the damage was already done before Decimal saw it.
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    # When WE handled it — deliberately not the same as the event's
    # occurred_at. The gap between the two is end-to-end pipeline lag, and you
    # can only measure it if you keep both.
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<BillingRecord order={self.order_id} amount={self.amount}>"

    # ------------------------------------------------------------------
    # NOTE THE ABSENCE: there is no ForeignKey to orders.id.
    #
    # Tempting, since both tables live in the same Postgres instance here. But
    # that is a convenience of this project, not a fact about the design.
    # Billing is a separate service; in production this table would live in
    # Billing's own database, where a FK to another service's table is not
    # merely bad practice but physically impossible.
    #
    # A FK would also make the event contract a lie: it would mean Billing
    # secretly depends on the Order Service's schema, so the "independent
    # failure domains" claim in the design doc would be false the moment
    # someone renamed a column.
    # ------------------------------------------------------------------
