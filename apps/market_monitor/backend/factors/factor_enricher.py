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

    async def enrich(self, candidate: Candidate, *, force_refresh: bool = False) -> Candidate:
        if not self.enabled:
            return candidate

        cache_key = candidate.symbol.upper()
        now = time.time()
        cached = self._cache.get(cache_key)
        snapshot_dict: Dict[str, Any]
        if not force_refresh and cached and (now - cached[0]) < self.cache_ttl_sec:
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
        _annotate_factor_completeness(snapshot_dict)
        candidate.factor_snapshot = snapshot_dict
        candidate.factor_updated_at = str(snapshot_dict.get("updated_at") or "")
        _apply_factor_sections(candidate, snapshot_dict)
        return candidate

    async def prewarm_microstructure(self, candidate: Candidate) -> bool:
        if not self.enabled or not self.microstructure_provider.enabled:
            return False
        try:
            await self.microstructure_provider.fetch(
                candidate.symbol,
                candidate.base_asset,
                context=_micro_context(candidate, candidate.factor_snapshot or {}),
            )
            return True
        except Exception as exc:
            logger.debug("Microstructure prewarm failed: symbol=%s err=%s", candidate.symbol, exc)
            return False


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


def _annotate_factor_completeness(snapshot: Dict[str, Any]) -> None:
    derivatives = snapshot.get("derivatives") if isinstance(snapshot.get("derivatives"), dict) else {}
    orderbook = snapshot.get("orderbook") if isinstance(snapshot.get("orderbook"), dict) else {}
    liquidation = snapshot.get("liquidation") if isinstance(snapshot.get("liquidation"), dict) else {}
    source_health = snapshot.get("source_health") if isinstance(snapshot.get("source_health"), dict) else {}
    liquidation_available, liquidation_status = _liquidation_data_state(liquidation, source_health)

    groups = {
        "oi": _has_any_number(
            derivatives,
            (
                "oi_amount",
                "oi_usdt",
                "oi_change_pct",
                "oi_change_pct_5m",
                "oi_change_pct_15m",
                "oi_change_pct_1h",
                "oi_change_pct_4h",
            ),
        ),
        "funding": _has_any_number(derivatives, ("funding_rate", "mark_price")),
        "taker_flow": _has_any_number(derivatives, ("taker_buy_ratio", "taker_buy_vol", "taker_sell_vol")),
        "micro": _safe_float(derivatives.get("micro_last_trade_ts"), 0.0) > 0
        or _safe_float(derivatives.get("trade_notional_usdt_1m"), 0.0) > 0,
        "orderbook": _safe_float(orderbook.get("bid_notional"), 0.0) > 0
        and _safe_float(orderbook.get("ask_notional"), 0.0) > 0,
        "liquidation": liquidation_available,
    }
    statuses = {
        name: "available" if value else "missing"
        for name, value in groups.items()
    }
    statuses["liquidation"] = liquidation_status
    total = len(groups)
    available = sum(1 for value in groups.values() if value)
    missing = [name for name, value in groups.items() if not value]
    snapshot["factor_completeness"] = {
        "available": available,
        "total": total,
        "pct": round((available / total * 100.0) if total else 0.0, 2),
        "groups": groups,
        "statuses": statuses,
        "missing": missing,
    }


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


def _has_any_number(payload: Dict[str, Any], keys: tuple[str, ...]) -> bool:
    for key in keys:
        if _safe_float(payload.get(key), 0.0) != 0.0 or key in payload and payload.get(key) is not None:
            return True
    return False


def _liquidation_data_state(liquidation: Dict[str, Any], source_health: Dict[str, Any]) -> tuple[bool, str]:
    if _has_nonzero_liquidation(liquidation):
        return True, "active"

    if bool(liquidation.get("source")) or "order_count" in liquidation:
        return True, "none_recent"

    has_micro_liq_fields = any(str(key).startswith("micro_liq_") for key in liquidation.keys())
    has_micro_timestamp = "micro_updated_at_ms" in liquidation or "micro_last_liq_ts" in liquidation
    if _microstructure_tracking(source_health) and (has_micro_liq_fields or has_micro_timestamp):
        return True, "none_recent"

    return False, "missing"


def _has_nonzero_liquidation(liquidation: Dict[str, Any]) -> bool:
    numeric_keys = (
        "order_count",
        "total_qty",
        "total_usdt",
        "long_liq_usdt",
        "short_liq_usdt",
        "micro_last_liq_ts",
        "micro_liq_count_1m",
        "micro_liq_count_3m",
        "micro_liq_count_5m",
        "micro_liq_long_usdt_1m",
        "micro_liq_long_usdt_3m",
        "micro_liq_long_usdt_5m",
        "micro_liq_short_usdt_1m",
        "micro_liq_short_usdt_3m",
        "micro_liq_short_usdt_5m",
    )
    return any(_safe_float(liquidation.get(key), 0.0) > 0.0 for key in numeric_keys)


def _microstructure_tracking(source_health: Dict[str, Any]) -> bool:
    health = source_health.get("microstructure")
    if not isinstance(health, dict):
        return False
    if health.get("ok") is False:
        return False
    message = str(health.get("message") or "").strip().lower()
    return message in {"tracking", "ok", "connected", "subscribed"} or bool(health.get("ok"))


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
