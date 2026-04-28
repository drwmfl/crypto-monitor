from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from candidates.storage_paths import resolve_runtime_dir
    from factors.factor_models import FactorSnapshot, recent_health_entry
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir
    from apps.market_monitor.backend.factors.factor_models import FactorSnapshot, recent_health_entry


class AccumulationPoolProvider:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.runtime_dir = resolve_runtime_dir(self.settings)
        self.pool_file = str(self.settings.get("pool_file") or "accumulation_pool.json")
        self.max_age_hours = max(1.0, _safe_float(self.settings.get("max_age_hours"), 36.0))
        self.cache_ttl_sec = max(5.0, _safe_float(self.settings.get("cache_ttl_sec"), 60.0))
        self._cache_loaded_at = 0.0
        self._cache: Dict[str, Any] = {}

    async def fetch(self, symbol: str, base_asset: str) -> FactorSnapshot:
        snapshot = FactorSnapshot.empty(symbol=symbol, base_asset=base_asset)
        if not self.enabled:
            snapshot.source_health["accumulation_pool"] = recent_health_entry("accumulation_pool", True, "disabled")
            return snapshot

        payload = self._load_pool()
        if not payload:
            snapshot.source_health["accumulation_pool"] = recent_health_entry("accumulation_pool", False, "pool_missing")
            return snapshot

        generated_at = str(payload.get("generated_at") or "")
        age_hours = _age_hours(generated_at)
        if age_hours is not None and age_hours > self.max_age_hours:
            snapshot.source_health["accumulation_pool"] = recent_health_entry(
                "accumulation_pool",
                False,
                f"stale={age_hours:.1f}h",
            )
            return snapshot

        symbols = payload.get("symbols")
        if not isinstance(symbols, dict):
            snapshot.source_health["accumulation_pool"] = recent_health_entry(
                "accumulation_pool",
                False,
                "invalid_pool",
            )
            return snapshot

        item = symbols.get(str(symbol or "").upper())
        if not isinstance(item, dict):
            snapshot.source_health["accumulation_pool"] = recent_health_entry(
                "accumulation_pool",
                True,
                "not_in_pool",
            )
            return snapshot

        normalized = dict(item)
        normalized["in_accumulation_pool"] = True
        normalized["pool_generated_at"] = generated_at
        if age_hours is not None:
            normalized["pool_age_hours"] = round(age_hours, 2)

        snapshot.accumulation.update(normalized)
        snapshot.source_health["accumulation_pool"] = recent_health_entry(
            "accumulation_pool",
            True,
            f"status={normalized.get('status', 'unknown')}",
        )
        return snapshot

    def _load_pool(self) -> Dict[str, Any]:
        now = time.time()
        if self._cache and (now - self._cache_loaded_at) < self.cache_ttl_sec:
            return self._cache

        path = self._pool_path()
        if not path.exists():
            self._cache = {}
            self._cache_loaded_at = now
            return {}

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        self._cache = payload if isinstance(payload, dict) else {}
        self._cache_loaded_at = now
        return self._cache

    def _pool_path(self) -> Path:
        path = Path(self.pool_file)
        return path if path.is_absolute() else self.runtime_dir / path


def _age_hours(value: str) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0)


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
