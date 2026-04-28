from __future__ import annotations

import bisect
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


WINDOW_MS: Dict[str, int] = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "24h": 24 * 60 * 60 * 1000,
    "48h": 48 * 60 * 60 * 1000,
}

DEFAULT_THRESHOLDS: Dict[str, float] = {
    "price_up_pct": 1.5,
    "price_down_pct": -1.5,
    "price_flat_abs_pct": 2.0,
    "oi_l0_1h_pct": 0.06,
    "oi_l1_5m_pct": 0.02,
    "oi_l1_15m_pct": 0.03,
    "oi_l2_15m_pct": 0.05,
    "oi_l2_1h_pct": 0.10,
    "oi_l3_4h_pct": 0.25,
    "oi_l3_24h_pct": 0.50,
    "oi_small_abs_pct": 0.005,
    "oi_negative_pct": -0.02,
    "oi_zscore_l3": 2.5,
    "rvol_confirm": 2.0,
    "taker_buy_confirm": 0.55,
    "funding_overheated": 0.0015,
}


class OIHistoryStore:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.runtime_dir = resolve_runtime_dir(self.settings)
        history_file = str(self.settings.get("history_file") or "oi_history.json").strip()
        self.history_path = self.runtime_dir / history_file
        self.max_samples_per_symbol = max(50, _safe_int(self.settings.get("max_samples_per_symbol"), 1200))
        self.bootstrap_ttl_sec = max(30, _safe_int(self.settings.get("bootstrap_ttl_sec"), 300))
        self.min_bootstrap_samples = max(2, _safe_int(self.settings.get("min_bootstrap_samples"), 12))
        self.min_zscore_samples = max(8, _safe_int(self.settings.get("min_zscore_samples"), 20))
        self.save_interval_sec = max(0.0, _safe_float(self.settings.get("save_interval_sec"), 2.0))
        self._state: Dict[str, Any] = self._load()
        self._last_save_ts = 0.0
        self._dirty = False

    def should_bootstrap(self, symbol: str) -> bool:
        if not self.enabled:
            return False
        symbol = _normalize_symbol(symbol)
        samples = self._samples(symbol)
        bootstrap_loaded_at = self._bootstrap_loaded_at(symbol)
        now = time.time()
        if bootstrap_loaded_at and (now - bootstrap_loaded_at) < float(self.bootstrap_ttl_sec):
            return False
        if len(samples) < self.min_bootstrap_samples:
            return True
        newest = _safe_int(samples[-1].get("timestamp_ms"), 0)
        oldest = _safe_int(samples[0].get("timestamp_ms"), 0)
        return (newest - oldest) < WINDOW_MS["4h"]

    def mark_bootstrapped(self, symbol: str) -> None:
        symbol = _normalize_symbol(symbol)
        self._state.setdefault("bootstrap_loaded_at", {})[symbol] = time.time()
        self._dirty = True
        self._save_if_due()

    def merge_historical_rows(self, symbol: str, rows: Iterable[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        samples: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            ts_ms = _safe_int(row.get("timestamp") or row.get("time") or row.get("T"), 0)
            oi_amount = _safe_float(row.get("sumOpenInterest") or row.get("openInterest"), 0.0)
            oi_usdt = _safe_float(row.get("sumOpenInterestValue") or row.get("openInterestValue"), 0.0)
            if ts_ms <= 0 or (oi_amount <= 0 and oi_usdt <= 0):
                continue
            samples.append(
                {
                    "timestamp_ms": ts_ms,
                    "oi_amount": oi_amount,
                    "oi_usdt": oi_usdt,
                    "source": "open_interest_hist",
                }
            )
        if samples:
            self._merge_samples(symbol, samples)

    def record_current(
        self,
        symbol: str,
        *,
        oi_amount: float,
        oi_usdt: float,
        price: Optional[float] = None,
        timestamp_ms: Optional[int] = None,
    ) -> None:
        if not self.enabled:
            return
        ts_ms = int(timestamp_ms or time.time() * 1000)
        if oi_amount <= 0 and oi_usdt <= 0:
            return
        sample = {
            "timestamp_ms": ts_ms,
            "oi_amount": float(oi_amount or 0.0),
            "oi_usdt": float(oi_usdt or 0.0),
            "price": float(price or 0.0),
            "source": "open_interest_current",
        }
        self._merge_samples(symbol, [sample])

    def metrics(self, symbol: str) -> Dict[str, Any]:
        samples = self._samples(_normalize_symbol(symbol))
        if not samples:
            return {}
        latest = samples[-1]
        latest_ts = _safe_int(latest.get("timestamp_ms"), 0)
        latest_value = _oi_value(latest)
        if latest_ts <= 0 or latest_value <= 0:
            return {}

        timestamps = [_safe_int(sample.get("timestamp_ms"), 0) for sample in samples]
        metrics: Dict[str, Any] = {
            "oi_history_sample_count": len(samples),
            "oi_history_span_minutes": round(max(0, latest_ts - timestamps[0]) / 60000.0, 2),
            "oi_history_latest_ts": latest_ts,
        }
        primary_window = ""
        primary_change: Optional[float] = None
        primary_change_usdt: Optional[float] = None

        for window, window_ms in WINDOW_MS.items():
            previous = _sample_at_or_before(samples, timestamps, latest_ts - window_ms)
            if not previous:
                continue
            prev_value = _oi_value(previous)
            if prev_value <= 0:
                continue
            change = (latest_value - prev_value) / prev_value
            change_usdt = latest_value - prev_value
            metrics[f"oi_change_pct_{window}"] = change
            metrics[f"oi_change_usdt_{window}"] = change_usdt
            zscore = _window_change_zscore(samples, timestamps, window_ms, current_change=change, min_count=self.min_zscore_samples)
            if zscore is not None:
                metrics[f"oi_zscore_{window}"] = zscore
            if primary_change is None or abs(change) > abs(primary_change):
                primary_window = window
                primary_change = change
                primary_change_usdt = change_usdt

        if primary_change is not None:
            metrics["oi_primary_window"] = primary_window
            metrics["oi_primary_change_pct"] = primary_change
            metrics["oi_change_pct"] = primary_change
            if primary_change_usdt is not None:
                metrics["oi_change_usdt"] = primary_change_usdt
        return metrics

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

    def _bootstrap_loaded_at(self, symbol: str) -> float:
        payload = self._state.setdefault("bootstrap_loaded_at", {})
        return _safe_float(payload.get(symbol), 0.0) if isinstance(payload, dict) else 0.0

    def _load(self) -> Dict[str, Any]:
        if not self.history_path.exists():
            return {"version": 1, "symbols": {}, "bootstrap_loaded_at": {}}
        try:
            payload = json.loads(self.history_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("version", 1)
                payload.setdefault("symbols", {})
                payload.setdefault("bootstrap_loaded_at", {})
                return payload
        except Exception:
            pass
        return {"version": 1, "symbols": {}, "bootstrap_loaded_at": {}}

    def _save_if_due(self) -> None:
        if not self._dirty:
            return
        now = time.time()
        if self.save_interval_sec > 0 and (now - self._last_save_ts) < self.save_interval_sec:
            return
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.history_path.with_suffix(self.history_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.history_path)
        self._dirty = False
        self._last_save_ts = now


def classify_oi_regime(
    derivatives: Dict[str, Any],
    *,
    context: Optional[Dict[str, Any]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context = context or {}
    cfg = dict(DEFAULT_THRESHOLDS)
    if isinstance(thresholds, dict):
        for key, value in thresholds.items():
            cfg[key] = _safe_float(value, cfg.get(key, 0.0))

    price_change = _best_price_change(context)
    rvol = _safe_float(context.get("rvol"), 0.0)
    taker_buy_ratio = _safe_float(derivatives.get("taker_buy_ratio"), 0.0)
    funding_rate = abs(_safe_float(derivatives.get("funding_rate"), 0.0))

    oi_changes = {
        window: _safe_optional_float(derivatives.get(f"oi_change_pct_{window}"))
        for window in WINDOW_MS
    }
    primary_window, primary_change = _primary_oi_change(oi_changes)
    primary_abs = abs(primary_change) if primary_change is not None else 0.0
    max_zscore = max(
        [
            abs(_safe_float(derivatives.get(f"oi_zscore_{window}"), 0.0))
            for window in WINDOW_MS
        ]
        or [0.0]
    )

    price_up = price_change >= cfg["price_up_pct"]
    price_down = price_change <= cfg["price_down_pct"]
    price_flat = abs(price_change) <= cfg["price_flat_abs_pct"]
    volume_confirmed = rvol >= cfg["rvol_confirm"] if rvol > 0 else False
    taker_confirmed = taker_buy_ratio >= cfg["taker_buy_confirm"] if taker_buy_ratio > 0 else False
    overheated_funding = funding_rate >= cfg["funding_overheated"] if funding_rate > 0 else False

    oi_5m = oi_changes.get("5m") or 0.0
    oi_15m = oi_changes.get("15m") or 0.0
    oi_1h = oi_changes.get("1h") or 0.0
    oi_4h = oi_changes.get("4h") or 0.0
    oi_24h = oi_changes.get("24h") or 0.0

    signal_level = "none"
    if oi_4h >= cfg["oi_l3_4h_pct"] or oi_24h >= cfg["oi_l3_24h_pct"] or max_zscore >= cfg["oi_zscore_l3"]:
        signal_level = "L3"
    elif oi_15m >= cfg["oi_l2_15m_pct"] or oi_1h >= cfg["oi_l2_1h_pct"]:
        signal_level = "L2"
    elif oi_5m >= cfg["oi_l1_5m_pct"] or oi_15m >= cfg["oi_l1_15m_pct"]:
        signal_level = "L1"
    elif oi_1h >= cfg["oi_l0_1h_pct"]:
        signal_level = "L0"

    if primary_change is None:
        regime = "unknown"
    elif price_up and primary_change <= cfg["oi_negative_pct"] and volume_confirmed:
        regime = "short_cover"
    elif price_up and primary_change >= cfg["oi_l1_15m_pct"] and (volume_confirmed or taker_confirmed):
        regime = "new_longs"
    elif price_down and primary_change >= cfg["oi_l1_15m_pct"] and volume_confirmed:
        regime = "new_shorts"
    elif price_down and primary_change <= cfg["oi_negative_pct"] and volume_confirmed:
        regime = "deleveraging"
    elif price_flat and primary_change >= cfg["oi_l0_1h_pct"]:
        regime = "accumulation"
    elif volume_confirmed and primary_abs <= cfg["oi_small_abs_pct"]:
        regime = "churn"
    else:
        regime = "neutral"

    confidence = _regime_confidence(
        signal_level=signal_level,
        regime=regime,
        volume_confirmed=volume_confirmed,
        taker_confirmed=taker_confirmed,
        overheated_funding=overheated_funding,
    )
    reason = _build_reason(
        regime=regime,
        signal_level=signal_level,
        primary_window=primary_window,
        primary_change=primary_change,
        price_change=price_change,
        rvol=rvol,
        taker_buy_ratio=taker_buy_ratio,
        overheated_funding=overheated_funding,
    )
    return {
        "oi_regime": regime,
        "oi_signal_level": signal_level,
        "oi_regime_confidence": confidence,
        "oi_reason": reason,
        "oi_price_change_pct_used": price_change,
        "oi_volume_confirmed": volume_confirmed,
        "oi_taker_confirmed": taker_confirmed,
        "oi_funding_overheated": overheated_funding,
    }


def _window_change_zscore(
    samples: List[Dict[str, Any]],
    timestamps: List[int],
    window_ms: int,
    *,
    current_change: float,
    min_count: int,
) -> Optional[float]:
    changes: List[float] = []
    for index, sample in enumerate(samples):
        ts_ms = timestamps[index]
        previous = _sample_at_or_before(samples, timestamps, ts_ms - window_ms)
        if not previous:
            continue
        prev_value = _oi_value(previous)
        curr_value = _oi_value(sample)
        if prev_value <= 0 or curr_value <= 0:
            continue
        changes.append((curr_value - prev_value) / prev_value)
    if len(changes) < min_count:
        return None
    mean = sum(changes) / len(changes)
    variance = sum((value - mean) ** 2 for value in changes) / max(1, len(changes) - 1)
    stdev = math.sqrt(variance)
    if stdev <= 0:
        return None
    return (current_change - mean) / stdev


def _sample_at_or_before(samples: List[Dict[str, Any]], timestamps: List[int], target_ts: int) -> Optional[Dict[str, Any]]:
    index = bisect.bisect_right(timestamps, target_ts) - 1
    if index < 0:
        return None
    return samples[index]


def _oi_value(sample: Dict[str, Any]) -> float:
    oi_usdt = _safe_float(sample.get("oi_usdt"), 0.0)
    if oi_usdt > 0:
        return oi_usdt
    return _safe_float(sample.get("oi_amount"), 0.0)


def _best_price_change(context: Dict[str, Any]) -> float:
    for key in ("change_pct", "change_15m_pct", "change_1h_pct"):
        value = _safe_optional_float(context.get(key))
        if value is not None:
            return value
    return 0.0


def _primary_oi_change(oi_changes: Dict[str, Optional[float]]) -> Tuple[str, Optional[float]]:
    primary_window = ""
    primary_change: Optional[float] = None
    for window in ("15m", "1h", "5m", "4h", "24h", "48h"):
        change = oi_changes.get(window)
        if change is None:
            continue
        if primary_change is None or abs(change) > abs(primary_change):
            primary_window = window
            primary_change = change
    return primary_window, primary_change


def _regime_confidence(
    *,
    signal_level: str,
    regime: str,
    volume_confirmed: bool,
    taker_confirmed: bool,
    overheated_funding: bool,
) -> float:
    score = {"none": 0.0, "L0": 35.0, "L1": 55.0, "L2": 75.0, "L3": 90.0}.get(signal_level, 0.0)
    if regime in {"new_longs", "accumulation"}:
        score += 8.0
    elif regime in {"short_cover", "churn", "neutral"}:
        score -= 8.0
    elif regime in {"new_shorts", "deleveraging"}:
        score -= 15.0
    if volume_confirmed:
        score += 5.0
    if taker_confirmed:
        score += 5.0
    if overheated_funding:
        score -= 10.0
    return round(max(0.0, min(100.0, score)), 2)


def _build_reason(
    *,
    regime: str,
    signal_level: str,
    primary_window: str,
    primary_change: Optional[float],
    price_change: float,
    rvol: float,
    taker_buy_ratio: float,
    overheated_funding: bool,
) -> str:
    parts = [f"{signal_level}:{regime}"]
    if primary_window and primary_change is not None:
        parts.append(f"OI {primary_window} {primary_change * 100.0:+.2f}%")
    parts.append(f"price {price_change:+.2f}%")
    if rvol > 0:
        parts.append(f"RVOL {rvol:.2f}x")
    if taker_buy_ratio > 0:
        parts.append(f"taker_buy {taker_buy_ratio * 100.0:.1f}%")
    if overheated_funding:
        parts.append("funding overheated")
    return "; ".join(parts)


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
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def _safe_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default
