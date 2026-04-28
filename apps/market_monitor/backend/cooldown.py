from __future__ import annotations

import logging
import time
from typing import Dict, Optional

try:
    import redis.asyncio as redis
except ModuleNotFoundError:  # pragma: no cover - optional dependency fallback
    redis = None

logger = logging.getLogger(__name__)


class AlertCooldownManager:
    def __init__(
        self,
        redis_url: str = "redis://redis:6379/0",
        prefix: str = "cooldown",
        use_redis: bool = True,
    ) -> None:
        self.redis_url = redis_url
        self.prefix = prefix
        self.use_redis = use_redis
        self._redis: Optional[redis.Redis] = None
        self._local_expiry: Dict[str, float] = {}

    async def setup(self) -> None:
        if not self.use_redis:
            logger.info("Cooldown manager uses in-memory store only.")
            return

        if redis is None:
            logger.warning("redis package not installed, fallback to in-memory cooldown store.")
            self._redis = None
            return

        try:
            self._redis = redis.from_url(self.redis_url, decode_responses=True)
            await self._redis.ping()
            logger.info("Cooldown manager connected to Redis: %s", self.redis_url)
        except Exception as exc:
            logger.warning("Redis unavailable, fallback to in-memory cooldown store: %s", exc)
            self._redis = None

    async def close(self) -> None:
        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                logger.exception("Failed to close Redis cooldown connection.")

    async def is_in_cooldown(
        self,
        symbol: str,
        rule_name: str,
        window: str,
        direction: str,
    ) -> bool:
        key = self._build_key(symbol, rule_name, window, direction)
        if self._redis is not None:
            try:
                exists = await self._redis.exists(key)
                return bool(exists)
            except Exception:
                logger.exception("Redis exists() failed, fallback to in-memory check.")

        return self._is_local_active(key)

    async def mark_triggered(
        self,
        symbol: str,
        rule_name: str,
        window: str,
        direction: str,
        cooldown_minutes: int,
        level: str = "medium",
        high_priority_extra_minutes: int = 0,
    ) -> None:
        key = self._build_key(symbol, rule_name, window, direction)
        effective_minutes = self._effective_cooldown_minutes(
            cooldown_minutes=cooldown_minutes,
            level=level,
            high_priority_extra_minutes=high_priority_extra_minutes,
        )
        ttl_seconds = max(1, int(effective_minutes * 60))

        if self._redis is not None:
            try:
                await self._redis.set(key, "1", ex=ttl_seconds)
                return
            except Exception:
                logger.exception("Redis set() failed, fallback to in-memory mark.")

        self._local_expiry[key] = time.time() + ttl_seconds

    async def allow_and_mark(
        self,
        symbol: str,
        rule_name: str,
        window: str,
        direction: str,
        cooldown_minutes: int,
        level: str = "medium",
        high_priority_extra_minutes: int = 0,
    ) -> bool:
        in_cd = await self.is_in_cooldown(
            symbol=symbol,
            rule_name=rule_name,
            window=window,
            direction=direction,
        )
        if in_cd:
            return False

        await self.mark_triggered(
            symbol=symbol,
            rule_name=rule_name,
            window=window,
            direction=direction,
            cooldown_minutes=cooldown_minutes,
            level=level,
            high_priority_extra_minutes=high_priority_extra_minutes,
        )
        return True

    def _build_key(self, symbol: str, rule_name: str, window: str, direction: str) -> str:
        return f"{self.prefix}:{symbol}:{rule_name}:{window}:{direction}"

    def _is_local_active(self, key: str) -> bool:
        self._gc_local()
        expiry = self._local_expiry.get(key, 0)
        return expiry > time.time()

    def _gc_local(self) -> None:
        now = time.time()
        expired = [key for key, expiry in self._local_expiry.items() if expiry <= now]
        for key in expired:
            self._local_expiry.pop(key, None)

    def _effective_cooldown_minutes(
        self,
        cooldown_minutes: int,
        level: str,
        high_priority_extra_minutes: int,
    ) -> int:
        base = max(1, int(cooldown_minutes))
        extra = max(0, int(high_priority_extra_minutes))
        if str(level or "").strip().lower() == "high":
            return base + extra
        return base
