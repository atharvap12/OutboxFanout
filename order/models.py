"""The two tables that make the outbox pattern work.

Big picture, in one analogy:

    Imagine a shop where the till and the courier are separate people.
    The dangerous version: the cashier rings up the sale, then walks outside
    to shout the delivery details to a courier. If they trip on the way out,
    the sale happened but nobody was ever told to deliver it.

    The outbox version: the cashier writes the sale in the ledger AND drops a
    delivery note into an out-tray — both in the same motion, same drawer.
    A courier empties the out-tray later. The cashier never leaves the till.

    `orders` is the ledger. `outbox` is the out-tray.

Because both live in the same database, one transaction covers both. There is
no moment where the sale exists but the delivery note doesn't.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    Boolean,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


def _utc_now() -> datetime:
    """Current time, always timezone-aware UTC.

    Never use datetime.now() with no timezone. A "naive" timestamp is a number
    with no units — like writing "meet me at 5" without saying which city.
    The moment your app and your database disagree about the timezone, you get
    events that appear to happen in the future or the past.
    """
    return datetime.now(timezone.utc)


class Order(Base):
    """A customer's order. The ledger entry."""

    __tablename__ = "orders"

    # We generate the UUID in Python, not in the database.
    #
    # Why: we need the id RIGHT NOW to write it into the outbox event's
    # payload. If the database generated it, we'd have to send the INSERT and
    # ask for the value back before we could build the event — extra round
    # trip, extra ordering constraint. Generating it here means the id exists
    # before either row is written.
    #
    # UUID vs an auto-incrementing number: a counter requires the database to
    # hand out the next value, so it can only be assigned at insert time, and
    # it leaks how many orders you've ever taken. A UUID is just 128 random
    # bits — anyone can mint one, anywhere, without coordinating.
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    customer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    item: Mapped[str] = mapped_column(String(255), nullable=False)

    # Numeric, NOT Float. This is not a style preference — it is money.
    #
    # Floats store numbers in binary, and 0.1 has no exact binary form, the
    # same way 1/3 has no exact decimal form. So 0.1 + 0.2 == 0.30000000000004.
    # Harmless for a temperature reading; unacceptable for a bill.
    # Numeric(12, 2) means "up to 12 digits, exactly 2 after the point",
    # stored exactly. Max value 9,999,999,999.99 — plenty.
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )

    def __repr__(self) -> str:
        return f"<Order {self.id} {self.item!r} {self.amount}>"


class OutboxEvent(Base):
    """A pending message. The delivery note in the out-tray.

    Written in the SAME transaction as the order it describes. A relay process
    (Phase 2) reads unpublished rows and sends them to SNS.
    """

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # Foreign key: the database refuses to keep a delivery note for an order
    # that doesn't exist. A cheap, permanent guarantee against orphan events.
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False
    )

    # What KIND of thing happened: "OrderCreated". Kept in its own column
    # rather than buried in the JSON so consumers can filter on it without
    # parsing the body — like the subject line on an envelope.
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)

    # The full snapshot of the order at the moment it happened.
    #
    # NOT just the order_id. This is the single most important design decision
    # in the table. An event is a statement about the PAST: "at 14:32, this
    # order was created, with these exact values." If a consumer had only the
    # id and had to look the order up later, it would read whatever the order
    # says NOW — which may have been amended, or deleted entirely. Billing
    # would charge the wrong amount, or crash on a missing row.
    #
    # It also keeps the services separate. If Billing had to query the orders
    # table, Billing would depend on the Order Service's schema, and renaming
    # a column would silently break three other services. Self-contained means
    # the EVENT is the contract.
    #
    # JSONB, not JSON: JSON stores the raw text you gave it (whitespace and
    # all) and re-parses on every read. JSONB parses once and stores a binary
    # form — smaller, faster to query, and indexable.
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Has the relay sent this yet? Starts false.
    #
    # server_default is deliberate: it is written into the TABLE definition,
    # so a row inserted by psql or any other tool still gets false. A
    # Python-side default only applies to rows this application creates.
    published: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"), default=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, nullable=False
    )

    # Null until the relay publishes it.
    #
    # Worth asking: isn't `published` redundant, given a NULL here means
    # unpublished? Strictly, yes — one column could do both jobs. We keep both
    # because `WHERE published = false` states the intent plainly, and because
    # a boolean is the natural thing to put a partial index on (below).
    # Redundancy that buys clarity is usually worth it; just know it IS
    # redundancy, and that the two must never disagree.
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # A PARTIAL index — indexes only the rows where published = false.
        #
        # Why it matters: the relay runs "find unpublished rows" every 2
        # seconds, forever. Over time almost every row is published, so that
        # query is hunting for a shrinking handful of needles in a growing
        # haystack. A normal index would cover all million rows, most of them
        # useless to this query, and grow forever.
        #
        # A partial index only contains the needles. Rows drop OUT of it the
        # moment they're marked published. So the index stays roughly the size
        # of your backlog — usually near zero — no matter how many orders you
        # have taken in total. It is the difference between a to-do list and a
        # diary.
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=text("published = false"),
        ),
    )

    def __repr__(self) -> str:
        state = "published" if self.published else "PENDING"
        return f"<OutboxEvent {self.id} {self.event_type} {state}>"
