"""Notifications consumer. Idempotency: Redis SET NX EX.

The same guarantee as Billing and Shipping, deliberately implemented the other
way so the two can be compared from direct experience rather than from an
article. That comparison is, per the design doc, "the core learning artifact of
the project".

THE DIFFERENCE THAT MATTERS IS NOT "REDIS INSTEAD OF POSTGRES".

Swapping the storage engine would be a boring difference. The real one is this:

    Billing      the dedup marker IS the side effect. One row, one UNIQUE
                 constraint, one transaction. There is no "between" for a crash
                 to land in, because marking and doing are a single act.

    Notifications  the dedup marker (a Redis key) and the side effect (an email
                 leaving the building) are in TWO SYSTEMS that no transaction
                 spans. They are two separate acts, in an order, with a gap.

That gap is the whole of Phase 5. It is the same shape as the relay's
Postgres/SNS problem one layer down the pipe, and it is why this consumer can
lose a notification where Billing cannot lose a billing record — a fact this
phase proves on purpose rather than asserts.
"""
