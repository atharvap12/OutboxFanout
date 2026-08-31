"""Shipping consumer. Idempotency: a Postgres UNIQUE constraint on order_id.

Deliberately THE SAME MECHANISM as Billing, against a different queue and a
different table. That repetition is the point of FR-05, not an accident: it
demonstrates that the pattern generalises to a second consumer without the two
sharing a single line of dedup state.

The comments here are shorter than Billing's on purpose. Where the reasoning is
identical, this file points at billing/ rather than restating it — one
explanation that stays correct beats two that drift apart.
"""
