"""Pydantic request/response models — the API contract.

Kept separate from models.py (SQLAlchemy, the database schema) so a column
change is not automatically a breaking API change, and internal fields do not
leak into responses.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class OrderCreate(BaseModel):
    """Accepted request body. Fields not listed here are rejected, so a client
    cannot supply its own `id` or `created_at`."""

    customer_id: str = Field(min_length=1, max_length=64, examples=["cust-42"])
    item: str = Field(min_length=1, max_length=255, examples=["Mechanical keyboard"])
    # gt=0 rules out free orders and negative amounts (a refund in disguise).
    amount: Decimal = Field(
        gt=0, max_digits=12, decimal_places=2, examples=["499.99"]
    )


class OrderResponse(BaseModel):
    id: uuid.UUID
    customer_id: str
    item: str
    amount: Decimal
    created_at: datetime

    # Lets Pydantic read attributes off a SQLAlchemy object directly.
    model_config = ConfigDict(from_attributes=True)


class OutboxEventResponse(BaseModel):
    """Read-only view of an outbox row. Debugging aid, not part of the API."""

    id: uuid.UUID
    order_id: uuid.UUID
    event_type: str
    payload: dict
    published: bool
    created_at: datetime
    published_at: datetime | None

    model_config = ConfigDict(from_attributes=True)
