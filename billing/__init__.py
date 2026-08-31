"""Billing consumer. Idempotency: a Postgres UNIQUE constraint on order_id.

An INDEPENDENT SERVICE. Its entire contract with the rest of the system is the
message that lands on billing-queue — it must never import order.models, never
read the orders table, and never care whether the Order Service is even
running. The Dockerfile enforces that physically by not copying order/ into
the image at all, so a stray import is a startup crash rather than a rule
somebody has to remember.
"""
