"""Notifications consumer. Idempotency: Redis SET NX EX.

The same guarantee as Billing and Shipping, implemented the other way, so the
two approaches can be compared directly.

The difference that matters: here the dedup marker and the side effect live in
DIFFERENT SYSTEMS. Billing's UNIQUE constraint and its billing_records row are
one write in one transaction; a Redis key and a sent email are two operations
that no transaction spans. That gap is the whole of Phase 5.
"""
