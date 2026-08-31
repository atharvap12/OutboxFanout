"""Redis client for the Notifications consumer's dedup store.

Deliberately separate from shared/db.py: this is a different consumer's
idempotency store with different guarantees, and nothing else in the system
reads it.
"""

from functools import lru_cache

import redis

from shared import config


# One client per process. redis-py's Redis object is a connection POOL, not a
# connection — it is thread-safe and reconnects on its own, so building one per
# call would leak sockets for no benefit.
@lru_cache(maxsize=1)
def client() -> redis.Redis:
    return redis.Redis.from_url(
        config.REDIS_URL,
        # Return str instead of bytes. Every value we store is text.
        decode_responses=True,
        # Without timeouts a network stall blocks the consumer loop forever.
        socket_connect_timeout=5,
        socket_timeout=5,
        # Detect a dead connection handed back by the pool — same reasoning as
        # pool_pre_ping on the SQLAlchemy engine, since `docker compose
        # down`/`up` kills every connection the pool still holds.
        health_check_interval=30,
    )
