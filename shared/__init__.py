"""Cross-cutting plumbing shared by every service: config, database sessions,
AWS clients, logging.

Business logic does not belong here. In particular, Billing must never import
anything that knows about orders or the outbox — its only contract is the
message that arrives on its queue.
"""
