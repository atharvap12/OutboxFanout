"""Code shared by every OutboxFanout service.

Deliberately small. Anything here is imported by services that are meant to be
independent, so only genuinely cross-cutting plumbing belongs in it:
configuration, database sessions, AWS clients, logging.

Business logic does NOT belong here. In particular, Billing must never import
anything that knows about orders or the outbox — its only contract is the
message that arrives on its queue.
"""
