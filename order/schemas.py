"""The shapes the API accepts and returns — Pydantic, not SQLAlchemy.

Two different things are called "models" in a project like this, and keeping
them apart matters:

    models.py  (SQLAlchemy) = what the DATABASE looks like.  Internal.
    schemas.py (Pydantic)   = what the API looks like.       Public contract.

Analogy: models.py is the kitchen; schemas.py is the menu. Customers order
from the menu. You can rearrange the kitchen — rename a shelf, move the
fridge — without reprinting the menu, and you can describe a dish on the menu
without exposing how it's made.

Collapse the two and every database change becomes a breaking API change, and
internal columns leak out to customers by accident.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class OrderCreate(BaseModel):
    """What a client is allowed to send to POST /orders.

    This is a gate, not a suggestion. Anything not listed here is rejected,
    and anything listed is checked before a single line of our code runs. A
    client cannot invent an `id`, or set `created_at` to last year, because
    those fields simply are not on the form.
    """

    customer_id: str = Field(
        min_length=1,
        max_length=64,
        description="Who is ordering",
        examples=["cust-42"],
    )
    item: str = Field(
        min_length=1,
        max_length=255,
        description="What they ordered",
        examples=["Mechanical keyboard"],
    )
    # gt=0 means strictly greater than zero: no free orders, no negative
    # orders (which would be a refund pretending to be a purchase).
    # Decimal, not float — same reason as the Numeric column in models.py.
    amount: Decimal = Field(
        gt=0,
        max_digits=12,
        decimal_places=2,
        description="Order total",
        examples=["499.99"],
    )


class OrderResponse(BaseModel):
    """What we send back after creating an order."""

    id: uuid.UUID
    customer_id: str
    item: str
    amount: Decimal
    created_at: datetime

    # from_attributes lets Pydantic read a SQLAlchemy object directly
    # (order.id, order.item, ...) instead of us hand-copying every field into
    # a dict. Without it, Pydantic only accepts dicts.
    model_config = ConfigDict(from_attributes=True)


class OutboxEventResponse(BaseModel):
    """Read-only view of an outbox row.

    Not part of the real API — exposed only so you can watch the out-tray
    fill up and drain during the phases, without reaching for psql.
    """

    id: uuid.UUID
    order_id: uuid.UUID
    event_type: str
    payload: dict
    published: bool
    created_at: datetime
    published_at: datetime | None

    model_config = ConfigDict(from_attributes=True)
