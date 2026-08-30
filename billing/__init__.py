"""Billing consumer. Idempotency: a Postgres UNIQUE constraint on order_id.

An independent service. Its only contract is the message on billing-queue —
it must never import order.models or read the orders table. The Dockerfile
enforces that by not copying order/ into the image at all.
"""
