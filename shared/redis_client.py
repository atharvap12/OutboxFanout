"""The Redis connection used by the Notifications consumer's dedup store.

Deliberately a separate module from shared/db.py, and not because Redis needs
different plumbing. It is a statement about ownership: this is ONE consumer's
idempotency store, nothing else in the system reads it, and the design doc is
explicit that consumers must not share dedup state.

    "Each consumer owns its own idempotency store. Consumers are independent
     services; they should not share dedup state."

A single shared dedup table (or key prefix) would be less code and would quietly
couple three independent services through one row — so one consumer's bad deploy
becomes everyone's outage.

Reference: redis-py documentation — https://redis.readthedocs.io/en/stable/
"""

from functools import lru_cache

import redis

from shared import config


@lru_cache(maxsize=1)
def client() -> redis.Redis:
    """One Redis client per process. Reused, never rebuilt per call.

    IMPORTANT MENTAL MODEL: a `redis.Redis` object is NOT a connection. It is a
    CONNECTION POOL with a command API bolted on. It lends out a socket for each
    command and takes it back afterwards, it is thread-safe, and it reconnects
    on its own when a socket dies.

    So building one per message would open and abandon sockets forever, for no
    benefit whatsoever — exactly the same mistake as building a boto3 client
    inside a loop (see shared/aws.py), and exactly the reason the SQLAlchemy
    engine is a module-level global in shared/db.py. Three libraries, one rule:
    THE EXPENSIVE, THREAD-SAFE, POOLED THING IS CREATED ONCE.
    """
    return redis.Redis.from_url(
        config.REDIS_URL,

        # (a) GIVE ME str, NOT bytes.
        #     Redis speaks bytes, and redis-py faithfully hands you bytes by
        #     default: b'1a2acb08-...' rather than '1a2acb08-...'. Every value we
        #     store is text (an event_id), and forgetting this produces the
        #     classic confusing bug where a comparison silently fails because
        #     b'abc' != 'abc' in Python. Decoding once, here, is better than
        #     remembering to .decode() at every call site.
        decode_responses=True,

        # (b) TIMEOUTS, BECAUSE THE DEFAULT IS "WAIT FOREVER".
        #     Without these, a network stall or a hung Redis blocks the consumer
        #     loop indefinitely — no error, no log line, just a consumer that
        #     appears alive and processes nothing. That is a worse failure than
        #     crashing, because nothing alerts on it.
        #
        #     Note these are SHORT (5s) where SQS's read timeout is LONG (30s).
        #     Not inconsistency — the opposite. A timeout is an assertion about
        #     expected latency: Redis answers in microseconds, so 5 seconds
        #     already means something is badly wrong. SQS long polling is
        #     *designed* to take 20 seconds, so a short timeout there would break
        #     it (the bug documented in shared/aws.py).
        socket_connect_timeout=5,
        socket_timeout=5,

        # (c) NOTICE A DEAD CONNECTION BEFORE USING IT.
        #     If a pooled socket has been idle and the server has gone away, the
        #     next command fails on a corpse. This pings idle connections so the
        #     failure is detected and the socket replaced instead.
        #
        #     Exactly the same problem `pool_pre_ping=True` solves for the
        #     SQLAlchemy engine, and it matters here for exactly the same
        #     project-specific reason: this stack gets `docker compose down`-ed
        #     constantly, which kills every connection the pool still believes
        #     in.
        health_check_interval=30,
    )
