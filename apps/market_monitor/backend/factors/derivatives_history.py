from __future__ import annotations

import bisect
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


WINDOW_MS: Dict[str, int] = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
}

DEFAULT_MAX_WINDOW_SAMPLE_LAG_SEC: Dict[str, int] = {
    "5m": 10 * 60,
    "15m": 20 * 60,
    "1h": 90 * 60,
}

DEFAULT_SHADOW_THRESHOLDS: Dict[str, float] = {
    "basis_fast_min_bps": 5.0,
    "basis_fast_fallback_bps": 15.0,
    "basis_sustained_min_bps": 10.0,
    "basis_sustained_fallback_bps": 25.0,
    "basis_delta_zscore": 1.5,
    "basis_extreme_percentile": 95.0,
    "funding_dynamic_min_bps": 0.05,
    "funding_dynamic_fallback_bps": 0.25,
    "funding_delta_zscore": 1.5,
}


class DerivativesHistoryStore:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.runtime_dir = resolve_runtime_dir(self.settings)
        history_file = str(self.settings.get("history_file") or "derivatives_shadow_history.json").strip()
        self.history_path = self.runtime_dir / history_file
        self.max_samples_per_symbol = max(100, _safe_int(self.settings.get("max_samples_per_symbol"), 3000))
        self.min_sample_interval_ms = max(
            0,
            int(_safe_float(self.settings.get("min_sample_interval_sec"), 30.0) * 1000),
        )
        self.basis_bootstrap_ttl_sec = max(
            30,
            _safe_int(self.settings.get("basis_bootstrap_ttl_sec"), 3600),
        )
        self.basis_bootstrap_retry_sec = max(
            30,
            _safe_int(self.settings.get("basis_bootstrap_retry_sec"), 1800),
        )
        self.min_stats_samples = max(8, _safe_int(self.settings.get("min_stats_samples"), 20))
        self.save_interval_sec = max(0.0, _safe_float(self.settings.get("save_interval_sec"), 10.0))
        self.max_window_sample_lag_ms = _parse_window_lag_ms(
            self.settings.get("max_window_sample_lag_sec")
        )
        self._state: Dict[str, Any] = self._load()
        self._last_save_ts = 0.0
        self._dirty = False

    def should_bootstrap_basis(self, symbol: str) -> bool:
        if not self.enabled:
            return False
        symbol = _normalize_symbol(symbol)
        attempted_at = _safe_float(
            self._state.setdefault("basis_bootstrap_attempted_at", {}).get(symbol),
            0.0,
        )
        if attempted_at and (time.time() - attempted_at) < self.basis_bootstrap_retry_sec:
            return False
        loaded_at = _safe_float(
            self._state.setdefault("basis_bootstrap_loaded_at", {}).get(symbol),
            0.0,
        )
        if loaded_at and (time.time() - loaded_at) < self.basis_bootstrap_ttl_sec:
            return False
        basis_samples = [sample for sample in self._samples(symbol) if _basis_value(sample) is not None]
        if len(basis_samples) < self.min_stats_samples:
            return True
        newest = _safe_int(basis_samples[-1].get("timestamp_ms"), 0)
        oldest = _safe_int(basis_samples[0].get("timestamp_ms"), 0)
        return newest <= 0 or (newest - oldest) < (24 * 60 * 60 * 1000)

    def reserve_basis_bootstrap(self, symbol: str) -> bool:
        symbol = _normalize_symbol(symbol)
        if not symbol or not self.should_bootstrap_basis(symbol):
            return False
        self._state.setdefault("basis_bootstrap_attempted_at", {})[symbol] = time.time()
        self._dirty = True
        self._save_if_due()
        return True

    def mark_basis_bootstrapped(self, symbol: str) -> None:
        symbol = _normalize_symbol(symbol)
        now = time.time()
        self._state.setdefault("basis_bootstrap_attempted_at", {})[symbol] = now
        self._state.setdefault("basis_bootstrap_loaded_at", {})[symbol] = now
        self._dirty = True
        self._save_if_due()

    def merge_basis_rows(self, symbol: str, rows: Iterable[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        samples: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            ts_ms = _safe_int(row.get("timestamp") or row.get("time"), 0)
            basis_rate = _safe_optional_float(row.get("basisRate"))
            if ts_ms <= 0 or basis_rate is None:
                continue
            samples.append(
                {
                    "timestamp_ms": ts_ms,
                    "basis_bps": basis_rate * 10000.0,
                    "basis_absolute": _safe_float(row.get("basis"), 0.0),
                    "futures_price": _safe_float(row.get("futuresPrice"), 0.0),
                    "index_price": _safe_float(row.get("indexPrice"), 0.0),
                    "source": "binance_basis_history",
                }
            )
        if samples:
            self._merge_samples(symbol, samples)

    def record_current(
        self,
        symbol: str,
        *,
        market_basis_bps: Optional[float],
        mark_basis_bps: Optional[float],
        funding_rate: Optional[float],
        funding_interval_hours: float,
        index_price: float = 0.0,
        mark_price: float = 0.0,
        market_mid_price: float = 0.0,
        timestamp_ms: Optional[int] = None,
    ) -> None:
        if not self.enabled:
            return
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return
        samples = self._samples(symbol)
        ts_ms = int(timestamp_ms or time.time() * 1000)
        if samples and self.min_sample_interval_ms > 0:
            latest_ts = _safe_int(samples[-1].get("timestamp_ms"), 0)
            if latest_ts > 0 and (ts_ms - latest_ts) < self.min_sample_interval_ms:
                ts_ms = latest_ts

        interval_hours = max(1.0, float(funding_interval_hours or 8.0))
        funding_8h = None if funding_rate is None else float(funding_rate) * (8.0 / interval_hours)
        primary_basis = market_basis_bps if market_basis_bps is not None else mark_basis_bps
        sample: Dict[str, Any] = {
            "timestamp_ms": ts_ms,
            "funding_interval_hours": interval_hours,
            "index_price": float(index_price or 0.0),
            "mark_price": float(mark_price or 0.0),
            "market_mid_price": float(market_mid_price or 0.0),
            "source": "binance_current",
        }
        if primary_basis is not None:
            sample["basis_bps"] = float(primary_basis)
        if market_basis_bps is not None:
            sample["market_basis_bps"] = float(market_basis_bps)
        if mark_basis_bps is not None:
            sample["mark_basis_bps"] = float(mark_basis_bps)
        if funding_rate is not None:
            sample["funding_rate"] = float(funding_rate)
            sample["funding_rate_8h"] = float(funding_8h or 0.0)
        self._merge_samples(symbol, [sample])

    def metrics(self, symbol: str) -> Dict[str, Any]:
        samples = self._samples(_normalize_symbol(symbol))
        if not samples:
            return {}
        result: Dict[str, Any] = {
            "derivatives_shadow_history_samples": len(samples),
            "derivatives_shadow_history_span_minutes": round(
                max(
                    0,
                    _safe_int(samples[-1].get("timestamp_ms"), 0)
                    - _safe_int(samples[0].get("timestamp_ms"), 0),
                )
                / 60000.0,
                2,
            ),
        }
        result.update(self._basis_metrics(samples))
        result.update(self._funding_metrics(samples))
        return result

    def _basis_metrics(self, samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        basis_samples = [sample for sample in samples if _basis_value(sample) is not None]
        if not basis_samples:
            return {"basis_shadow_status": "missing"}
        latest = basis_samples[-1]
        latest_ts = _safe_int(latest.get("timestamp_ms"), 0)
        latest_value = _basis_value(latest)
        if latest_ts <= 0 or latest_value is None:
            return {"basis_shadow_status": "missing"}
        timestamps = [_safe_int(sample.get("timestamp_ms"), 0) for sample in basis_samples]
        result: Dict[str, Any] = {
            "basis_bps_now": latest_value,
            "market_basis_bps": _safe_optional_float(latest.get("market_basis_bps")),
            "mark_basis_bps": _safe_optional_float(latest.get("mark_basis_bps")),
            "basis_history_sample_count": len(basis_samples),
            "basis_history_span_minutes": round(max(0, latest_ts - timestamps[0]) / 60000.0, 2),
            "basis_latest_ts": latest_ts,
        }
        valid_windows: List[str] = []
        for window, window_ms in WINDOW_MS.items():
            previous = _sample_for_window(
                basis_samples,
                timestamps,
                latest_ts,
                window_ms,
                self.max_window_sample_lag_ms[window],
                _basis_value,
            )
            previous_value = _basis_value(previous) if previous else None
            if previous_value is None:
                continue
            delta = latest_value - previous_value
            result[f"basis_delta_{window}_bps"] = delta
            valid_windows.append(window)
            zscore = _window_delta_zscore(
                basis_samples,
                timestamps,
                window_ms,
                current_delta=delta,
                value_getter=_basis_value,
                min_count=self.min_stats_samples,
                max_baseline_lag_ms=self.max_window_sample_lag_ms[window],
            )
            if zscore is not None:
                result[f"basis_delta_zscore_{window}"] = zscore

        values_24h = _values_since(basis_samples, latest_ts - 24 * 60 * 60 * 1000, _basis_value)
        values_7d = _values_since(basis_samples, latest_ts - 7 * 24 * 60 * 60 * 1000, _basis_value)
        if len(values_24h) >= self.min_stats_samples:
            result["basis_percentile_24h"] = _percentile_rank(values_24h, latest_value)
            zscore = _robust_zscore(values_24h, latest_value)
            if zscore is not None:
                result["basis_zscore_24h"] = zscore
        if len(values_7d) >= self.min_stats_samples:
            zscore = _robust_zscore(values_7d, latest_value)
            if zscore is not None:
                result["basis_zscore_7d"] = zscore
        result["basis_valid_windows"] = valid_windows
        result["basis_shadow_status"] = "ready" if {"5m", "15m"}.issubset(valid_windows) else "warming"
        return result

    def _funding_metrics(self, samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        funding_samples = [sample for sample in samples if _funding_value(sample) is not None]
        if not funding_samples:
            return {"funding_shadow_status": "missing"}
        latest = funding_samples[-1]
        latest_ts = _safe_int(latest.get("timestamp_ms"), 0)
        latest_value = _funding_value(latest)
        if latest_ts <= 0 or latest_value is None:
            return {"funding_shadow_status": "missing"}
        timestamps = [_safe_int(sample.get("timestamp_ms"), 0) for sample in funding_samples]
        result: Dict[str, Any] = {
            "funding_rate_signed": _safe_float(latest.get("funding_rate"), 0.0),
            "funding_rate_8h": latest_value,
            "funding_interval_hours": _safe_float(latest.get("funding_interval_hours"), 8.0),
            "funding_history_sample_count": len(funding_samples),
            "funding_history_span_minutes": round(max(0, latest_ts - timestamps[0]) / 60000.0, 2),
            "funding_latest_ts": latest_ts,
        }
        valid_windows: List[str] = []
        for window, window_ms in WINDOW_MS.items():
            previous = _sample_for_window(
                funding_samples,
                timestamps,
                latest_ts,
                window_ms,
                self.max_window_sample_lag_ms[window],
                _funding_value,
            )
            previous_value = _funding_value(previous) if previous else None
            if previous_value is None:
                continue
            delta = latest_value - previous_value
            result[f"funding_delta_{window}"] = delta
            result[f"funding_delta_{window}_bps"] = delta * 10000.0
            valid_windows.append(window)
            zscore = _window_delta_zscore(
                funding_samples,
                timestamps,
                window_ms,
                current_delta=delta,
                value_getter=_funding_value,
                min_count=self.min_stats_samples,
                max_baseline_lag_ms=self.max_window_sample_lag_ms[window],
            )
            if zscore is not None:
                result[f"funding_delta_zscore_{window}"] = zscore

        values_24h = _values_since(funding_samples, latest_ts - 24 * 60 * 60 * 1000, _funding_value)
        if len(values_24h) >= self.min_stats_samples:
            result["funding_percentile_24h"] = _percentile_rank(values_24h, latest_value)
            zscore = _robust_zscore(values_24h, latest_value)
            if zscore is not None:
                result["funding_zscore_24h"] = zscore
        result["funding_valid_windows"] = valid_windows
        result["funding_shadow_status"] = "ready" if "15m" in valid_windows else "warming"
        return result

    def _merge_samples(self, symbol: str, incoming: List[Dict[str, Any]]) -> None:
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return
        existing = self._samples(symbol)
        by_ts: Dict[int, Dict[str, Any]] = {
            _safe_int(sample.get("timestamp_ms"), 0): dict(sample)
            for sample in existing
            if _safe_int(sample.get("timestamp_ms"), 0) > 0
        }
        changed = False
        for sample in incoming:
            ts_ms = _safe_int(sample.get("timestamp_ms"), 0)
            if ts_ms <= 0:
                continue
            previous = by_ts.get(ts_ms, {})
            merged = dict(previous)
            merged.update({key: value for key, value in sample.items() if value is not None})
            if merged != previous:
                by_ts[ts_ms] = merged
                changed = True
        if not changed:
            return
        samples = [by_ts[ts] for ts in sorted(by_ts)]
        if len(samples) > self.max_samples_per_symbol:
            samples = samples[-self.max_samples_per_symbol :]
        self._state.setdefault("symbols", {})[symbol] = samples
        self._dirty = True
        self._save_if_due()

    def _samples(self, symbol: str) -> List[Dict[str, Any]]:
        symbols = self._state.setdefault("symbols", {})
        samples = symbols.get(symbol)
        if isinstance(samples, list):
            return samples
        symbols[symbol] = []
        return symbols[symbol]

    def _load(self) -> Dict[str, Any]:
        if not self.history_path.exists():
            return {
                "version": 1,
                "symbols": {},
                "basis_bootstrap_attempted_at": {},
                "basis_bootstrap_loaded_at": {},
            }
        try:
            payload = json.loads(self.history_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("version", 1)
                payload.setdefault("symbols", {})
                payload.setdefault("basis_bootstrap_attempted_at", {})
                payload.setdefault("basis_bootstrap_loaded_at", {})
                return payload
        except Exception:
            pass
        return {
            "version": 1,
            "symbols": {},
            "basis_bootstrap_attempted_at": {},
            "basis_bootstrap_loaded_at": {},
        }

    def _save_if_due(self) -> None:
        if not self._dirty:
            return
        now = time.time()
        if self.save_interval_sec > 0 and (now - self._last_save_ts) < self.save_interval_sec:
            return
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.history_path.with_suffix(self.history_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(self._state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(self.history_path)
        self._dirty = False
        self._last_save_ts = now


def classify_derivatives_shadow(
    derivatives: Dict[str, Any],
    *,
    context: Optional[Dict[str, Any]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context = context or {}
    cfg = dict(DEFAULT_SHADOW_THRESHOLDS)
    if isinstance(thresholds, dict):
        for key, value in thresholds.items():
            cfg[key] = _safe_float(value, cfg.get(key, 0.0))

    basis_now = _safe_optional_float(derivatives.get("basis_bps_now"))
    basis_5m = _safe_optional_float(derivatives.get("basis_delta_5m_bps"))
    basis_15m = _safe_optional_float(derivatives.get("basis_delta_15m_bps"))
    basis_z_5m = abs(_safe_float(derivatives.get("basis_delta_zscore_5m"), 0.0))
    basis_z_15m = abs(_safe_float(derivatives.get("basis_delta_zscore_15m"), 0.0))
    basis_percentile = _safe_optional_float(derivatives.get("basis_percentile_24h"))

    fast_basis = basis_5m is not None and abs(basis_5m) >= cfg["basis_fast_min_bps"] and (
        basis_z_5m >= cfg["basis_delta_zscore"]
        or abs(basis_5m) >= cfg["basis_fast_fallback_bps"]
    )
    sustained_basis = (
        basis_5m is not None
        and basis_15m is not None
        and _same_nonzero_sign(basis_5m, basis_15m)
        and abs(basis_15m) >= cfg["basis_sustained_min_bps"]
        and (
            basis_z_15m >= cfg["basis_delta_zscore"]
            or abs(basis_15m) >= cfg["basis_sustained_fallback_bps"]
        )
    )
    basis_move = basis_15m if sustained_basis and basis_15m is not None else basis_5m
    basis_state = "warming" if basis_now is None else "stable"
    if basis_now is not None and basis_move is not None and (fast_basis or sustained_basis):
        if basis_now >= 0 and basis_move > 0:
            basis_state = "positive_expansion"
        elif basis_now >= 0 and basis_move < 0:
            basis_state = "positive_contraction"
        elif basis_now < 0 and basis_move < 0:
            basis_state = "negative_expansion"
        else:
            basis_state = "negative_contraction"

    extreme_cutoff = max(50.0, min(100.0, cfg["basis_extreme_percentile"]))
    basis_extreme = "none"
    if basis_percentile is not None and basis_now is not None:
        if basis_percentile >= extreme_cutoff and basis_now > 0:
            basis_extreme = "positive"
        elif basis_percentile <= (100.0 - extreme_cutoff) and basis_now < 0:
            basis_extreme = "negative"

    funding_15m_bps = _safe_optional_float(derivatives.get("funding_delta_15m_bps"))
    funding_z_15m = abs(_safe_float(derivatives.get("funding_delta_zscore_15m"), 0.0))
    funding_state = "warming"
    if funding_15m_bps is not None:
        dynamic = abs(funding_15m_bps) >= cfg["funding_dynamic_min_bps"] and (
            funding_z_15m >= cfg["funding_delta_zscore"]
            or abs(funding_15m_bps) >= cfg["funding_dynamic_fallback_bps"]
        )
        funding_state = "rising" if dynamic and funding_15m_bps > 0 else "falling" if dynamic else "stable"

    direction = str(context.get("direction") or "").strip().lower()
    oi_regime = str(derivatives.get("oi_shadow_regime") or "unknown").strip().lower()
    oi_turn_direction = str(derivatives.get("oi_turn_direction") or "none").strip().lower()
    opportunity_modifier = 0.0
    risk_modifier = 0.0
    reasons: List[str] = []

    if direction == "up":
        if oi_regime == "new_longs":
            opportunity_modifier += 2.0
            reasons.append("oi_new_longs")
        elif oi_regime == "short_cover":
            opportunity_modifier -= 1.0
            risk_modifier += 2.0
            reasons.append("oi_short_cover")
        if basis_state == "positive_expansion" and basis_extreme == "none" and oi_regime == "new_longs":
            opportunity_modifier += 1.5
            reasons.append("basis_confirms_new_longs")
        if basis_state == "positive_expansion" and basis_extreme == "positive":
            risk_modifier += 4.0 + (2.0 if oi_regime == "new_longs" else 0.0)
            reasons.append("positive_basis_crowding")
        if oi_turn_direction == "down":
            risk_modifier += 3.0
            reasons.append("oi_turn_down")
        if funding_state == "rising" and basis_state == "positive_expansion" and oi_regime == "new_longs":
            risk_modifier += 3.0
            reasons.append("funding_long_crowding")
    elif direction == "down":
        if oi_regime == "new_shorts":
            opportunity_modifier += 1.0
            reasons.append("oi_new_shorts")
        if basis_state == "negative_expansion" and basis_extreme == "negative":
            risk_modifier += 4.0 + (2.0 if oi_regime == "new_shorts" else 0.0)
            reasons.append("negative_basis_crowding")
        if oi_turn_direction == "up" and oi_regime == "new_shorts":
            risk_modifier += 2.0
            reasons.append("oi_short_build")
        if funding_state == "falling" and basis_state == "negative_expansion" and oi_regime == "new_shorts":
            risk_modifier += 3.0
            reasons.append("funding_short_crowding")

    return {
        "derivatives_shadow_policy_version": "derivatives-v1-shadow",
        "basis_shadow_state": basis_state,
        "basis_shadow_extreme": basis_extreme,
        "basis_shadow_fast": fast_basis,
        "basis_shadow_sustained": sustained_basis,
        "funding_shadow_state": funding_state,
        "derivatives_shadow_opportunity_modifier": round(max(-4.0, min(4.0, opportunity_modifier)), 2),
        "derivatives_shadow_risk_modifier": round(max(0.0, min(10.0, risk_modifier)), 2),
        "derivatives_shadow_reasons": reasons,
    }


def _sample_for_window(
    samples: List[Dict[str, Any]],
    timestamps: List[int],
    latest_ts: int,
    window_ms: int,
    max_lag_ms: int,
    value_getter: Any,
) -> Optional[Dict[str, Any]]:
    target_ts = latest_ts - window_ms
    previous = _sample_at_or_before(samples, timestamps, target_ts)
    if not previous or value_getter(previous) is None:
        return None
    previous_ts = _safe_int(previous.get("timestamp_ms"), 0)
    lag_ms = target_ts - previous_ts
    if lag_ms < 0 or lag_ms > max_lag_ms:
        return None
    return previous


def _window_delta_zscore(
    samples: List[Dict[str, Any]],
    timestamps: List[int],
    window_ms: int,
    *,
    current_delta: float,
    value_getter: Any,
    min_count: int,
    max_baseline_lag_ms: int,
) -> Optional[float]:
    deltas: List[float] = []
    for index, sample in enumerate(samples):
        current_value = value_getter(sample)
        if current_value is None:
            continue
        ts_ms = timestamps[index]
        previous = _sample_at_or_before(samples, timestamps, ts_ms - window_ms)
        previous_value = value_getter(previous) if previous else None
        if previous_value is None:
            continue
        previous_ts = _safe_int(previous.get("timestamp_ms"), 0)
        lag_ms = (ts_ms - window_ms) - previous_ts
        if lag_ms < 0 or lag_ms > max_baseline_lag_ms:
            continue
        deltas.append(current_value - previous_value)
    if len(deltas) < min_count:
        return None
    return _robust_zscore(deltas, current_delta)


def _values_since(samples: List[Dict[str, Any]], min_ts: int, value_getter: Any) -> List[float]:
    values: List[float] = []
    for sample in samples:
        if _safe_int(sample.get("timestamp_ms"), 0) < min_ts:
            continue
        value = value_getter(sample)
        if value is not None:
            values.append(value)
    return values


def _percentile_rank(values: List[float], current: float) -> float:
    if not values:
        return 0.0
    less = sum(1 for value in values if value < current)
    equal = sum(1 for value in values if value == current)
    return round((less + 0.5 * equal) / len(values) * 100.0, 2)


def _robust_zscore(values: List[float], current: float) -> Optional[float]:
    if len(values) < 2:
        return None
    median = statistics.median(values)
    deviations = [abs(value - median) for value in values]
    mad = statistics.median(deviations)
    if mad > 0:
        return round(0.67448975 * (current - median) / mad, 4)
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / max(1, len(values) - 1)
    stdev = math.sqrt(variance)
    if stdev <= 0:
        return None
    return round((current - mean) / stdev, 4)


def _sample_at_or_before(
    samples: List[Dict[str, Any]],
    timestamps: List[int],
    target_ts: int,
) -> Optional[Dict[str, Any]]:
    index = bisect.bisect_right(timestamps, target_ts) - 1
    if index < 0:
        return None
    return samples[index]


def _basis_value(sample: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(sample, dict):
        return None
    for key in ("market_basis_bps", "basis_bps", "mark_basis_bps"):
        value = _safe_optional_float(sample.get(key))
        if value is not None:
            return value
    return None


def _funding_value(sample: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(sample, dict):
        return None
    return _safe_optional_float(sample.get("funding_rate_8h"))


def _parse_window_lag_ms(value: Any) -> Dict[str, int]:
    result = {
        window: max(1, int(seconds * 1000))
        for window, seconds in DEFAULT_MAX_WINDOW_SAMPLE_LAG_SEC.items()
    }
    if not isinstance(value, dict):
        return result
    for window in WINDOW_MS:
        seconds = _safe_float(value.get(window), 0.0)
        if seconds > 0:
            result[window] = int(seconds * 1000)
    return result


def _same_nonzero_sign(left: float, right: float) -> bool:
    return (left > 0 and right > 0) or (left < 0 and right < 0)


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("/", "")


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
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _safe_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default
