"""Order Service — accepts new orders and records them atomically.

This service NEVER talks to SNS, SQS, or any message broker. It only ever
writes to its own database. That single restriction is the entire outbox
pattern; everything else is consequence.
"""
