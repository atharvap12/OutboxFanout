"""Order Service — accepts orders and records them atomically.

Never talks to SNS, SQS, or any broker; it only writes to its own database.
That restriction is the outbox pattern.
"""
