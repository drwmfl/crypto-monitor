from __future__ import annotations

import copy
import logging
import time
from typing import Any, Dict, Optional

try:
    from candidates.candidate_models import Candidate
    from factors.accumulation_pool import AccumulationPoolProvider
    from factors.binance_factors import BinanceFactorProvider
    from factors.factor_models import FactorSnapshot, merge_factor_snapshot, recent_health_entry
    from factors.microstructure import MicrostructureProvider
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.factors.accumulation_pool import AccumulationPoolProvider
    from apps.market_monitor.backend.factors.binance_factors import BinanceFactorProvider
    from apps.market_monitor.backend.factors.factor_models import FactorSnapshot, merge_factor_snapshot, recent_health_entry
    from apps.market_monitor.backend.factors.microstructure import MicrostructureProvider

logger = logging.getLogger(__name__)


class FactorEnricher:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.cache_ttl_sec = max(1.0, _safe_float(self.settings.get("cache_ttl_sec"), 60.0))
        self.min_base_score = max(0.0, _safe_float(self.settings.get("min_base_score"), 40.0))
        self.min_event_count = max(1, _safe_int(self.settings.get("min_event_count"), 1))
        binance_settings = dict(self.settings.get("binance", {}) or {})
        if self.settings.get("runtime_dir") and not binance_settings.get("runtime_dir"):
            binance_settings["runtime_dir"] = self.settings.get("runtime_dir")
        self.binance_provider = BinanceFactorProvider(binance_settings)
        microstructure_settings = (
            dict(binance_settings.get("microstructure", {}) or {})
            if isinstance(binance_settings.get("microstructure"), dict)
            else {}
        )
        self.microstructure_provider = MicrostructureProvider(microstructure_settings)
        accumulation_settings = dict(self.settings.get("accumulation_pool", {}) or {})
        if self.settings.get("runtime_dir") and not accumulation_settings.get("runtime_dir"):
            accumulation_settings["runtime_dir"] = self.settings.get("runtime_dir")
        self.accumulation_provider = AccumulationPoolProvider(accumulation_settings)
        self._cache: Dict[str, tuple[float, Dict[str, Any]]] = {}

    def should_enrich(self, candidate: Candidate, base_score: float) -> bool:
        if not self.enabled:
            return False
        if float(base_score or 0.0) >= self.min_base_score:
            return True
        return int(candidate.event_count or 0) >= self.min_event_count and len(candidate.windows) >= 2

    async def enrich(self, candidate: Candidate) -> Candidate:
        if not self.enabled:
            return candidate

        cache_key = candidate.symbol.upper()
        now = time.time()
        cached = self._cache.get(cache_key)
        snapshot_dict: Dict[str, Any]
        if cached and (now - cached[0]) < self.cache_ttl_sec:
            snapshot_dict = copy.deepcopy(cached[1])
        else:
            price = _safe_float((candidate.latest_features or {}).get("price"), 0.0)
            merged = FactorSnapshot.empty(symbol=candidate.symbol, base_asset=candidate.base_asset)
            try:
                binance_snapshot = await self.binance_provider.fetch(
                    candidate.symbol,
                    candidate.base_asset,
                    price=price,
                    context=_candidate_context(candidate),
                )
                merged = _merge_snapshots(merged, binance_snapshot)
            except Exception as exc:
                logger.debug("Binance factor enrich failed: symbol=%s err=%s", candidate.symbol, exc)
                merged.source_health["binance"] = recent_health_entry("binance", False, str(exc))

            try:
                accumulation_snapshot = await self.accumulation_provider.fetch(candidate.symbol, candidate.base_asset)
                merged = _merge_snapshots(merged, accumulation_snapshot)
            except Exception as exc:
                logger.debug("Accumulation pool enrich failed: symbol=%s err=%s", candidate.symbol, exc)
                merged.source_health["accumulation_pool"] = recent_health_entry("accumulation_pool", False, str(exc))

            snapshot_dict = merge_factor_snapshot(candidate.factor_snapshot, merged)
            self._cache[cache_key] = (now, copy.deepcopy(snapshot_dict))

        if self.microstructure_provider.enabled:
            try:
                micro_snapshot = await self.microstructure_provider.fetch(
                    candidate.symbol,
                    candidate.base_asset,
                    context=_micro_context(candidate, snapshot_dict),
                )
                snapshot_dict = merge_factor_snapshot(snapshot_dict, micro_snapshot)
            except Exception as exc:
                logger.debug("Microstructure factor enrich failed: symbol=%s err=%s", candidate.symbol, exc)
                current_health = dict(snapshot_dict.get("source_health") or {})
                current_health["microstructure"] = recent_health_entry("microstructure", False, str(exc))
                snapshot_dict["source_health"] = current_health

        _derive_factor_changes(candidate, snapshot_dict)
        candidate.factor_snapshot = snapshot_dict
        candidate.factor_updated_at = str(snapshot_dict.get("updated_at") or "")
        _apply_factor_sections(candidate, snapshot_dict)
        return candidate


def _merge_snapshots(left: FactorSnapshot, right: FactorSnapshot) -> FactorSnapshot:
    left.derivatives.update(right.derivatives or {})
    left.orderbook.update(right.orderbook or {})
    left.liquidation.update(right.liquidation or {})
    left.accumulation.update(right.accumulation or {})
    left.source_health.update(right.source_health or {})
    left.updated_at = right.updated_at or left.updated_at
    return left


def _apply_factor_sections(candidate: Candidate, snapshot: Dict[str, Any]) -> None:
    candidate.derivatives = dict(snapshot.get("derivatives") or {})
    candidate.orderbook = dict(snapshot.get("orderbook") or {})
    candidate.liquidation = dict(snapshot.get("liquidation") or {})
    candidate.accumulation = dict(snapshot.get("accumulation") or {})


def _derive_factor_changes(candidate: Candidate, snapshot: Dict[str, Any]) -> None:
    derivatives = snapshot.get("derivatives")
    if not isinstance(derivatives, dict):
        return
    previous = candidate.derivatives or {}
    prev_oi = _safe_float(previous.get("oi_usdt") or previous.get("oi_amount"), 0.0)
    curr_oi = _safe_float(derivatives.get("oi_usdt") or derivatives.get("oi_amount"), 0.0)
    if curr_oi > 0 and prev_oi > 0 and "oi_change_pct" not in derivatives:
        derivatives["oi_change_pct"] = (curr_oi - prev_oi) / prev_oi
        derivatives["oi_change_usdt"] = curr_oi - prev_oi


def _candidate_context(candidate: Candidate) -> Dict[str, Any]:
    latest = dict(candidate.latest_features or {})
    latest["event_count"] = candidate.event_count
    latest["windows"] = list(candidate.windows or [])
    latest["directions"] = dict(candidate.directions or {})
    return latest


def _micro_context(candidate: Candidate, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    latest = _candidate_context(candidate)
    return {
        "latest": latest,
        "direction": str(latest.get("direction") or ""),
        "derivatives": dict(snapshot.get("derivatives") or {}),
        "liquidation": dict(snapshot.get("liquidation") or {}),
    }


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


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
