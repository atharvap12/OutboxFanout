"""Shipping consumer. Idempotency: a Postgres UNIQUE constraint on order_id.

Deliberately the same mechanism as Billing against a different queue and a
different table — the point of FR-05 is that the pattern generalises without
the two consumers sharing any dedup state.
"""
