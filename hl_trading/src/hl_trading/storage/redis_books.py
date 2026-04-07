"""Optional Redis mirror of serialized L2 books for cross-process readers."""

from __future__ import annotations

import logging

import orjson
import redis

logger = logging.getLogger(__name__)


class RedisBookMirror:
    def __init__(self, redis_url: str) -> None:
        self._r: redis.Redis = redis.from_url(redis_url, decode_responses=False)

    def publish_book(self, coin: str, payload: dict) -> None:
        key = f"hl:l2book:{coin.upper()}"
        try:
            self._r.setex(key, 60, orjson.dumps(payload))
        except Exception:
            logger.exception("redis book publish failed coin=%s", coin)
