"""Shared Redis connection factory.

We hand out a single module-level client. redis-py clients are thread safe and
hold an internal connection pool, so api threads and the worker loop can share
one instance. decode_responses=True means we get str back instead of bytes,
which keeps the queue code free of decode() calls.
"""

import redis

from . import config

_client = None


def get_client():
    global _client
    if _client is None:
        _client = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    return _client
