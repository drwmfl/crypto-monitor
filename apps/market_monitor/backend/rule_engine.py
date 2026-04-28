from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from alert_config import AlertConfig
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import AlertConfig

logger = logging.getLogger(__name__)

MIN_ATR_K_BY_RULE = {
    "short_breakout": 1.5,
    "trend_acceleration": 2.0,
    "long_anomaly": 3.0,
}


@dataclass
class MarketSnapshot:
    symbol: str
    window: str
    open_price: float
    close_price: float
    volume: float
    avg_volume: float
    event_time: Optional[datetime] = None
    indicators: Dict[str, Any] = field(default_factory=dict)

    @property
    def change_pct(self) -> float:
        if self.open_price <= 0:
            return 0.0
        return (self.close_price - self.open_price) / self.open_price * 100.0

    @property
    def abs_change(self) -> float:
        return abs(self.close_price - self.open_price)

    @property
    def direction(self) -> str:
        if self.change_pct > 0:
            return "up"
        if self.change_pct < 0:
            return "down"
        return "flat"


def evaluate_snapshot(snapshot: MarketSnapshot, config: AlertConfig) -> List[Dict[str, Any]]:
    return check_rules(snapshot, config)


def check_rules(snapshot: MarketSnapshot, config: AlertConfig) -> List[Dict[str, Any]]:
    if snapshot.direction == "flat":
        return []

    events: List[Dict[str, Any]] = []
    events.extend(_eval_short_breakout(snapshot, config))
    events.extend(_eval_trend_acceleration(snapshot, config))
    events.extend(_eval_long_anomaly(snapshot, config))
    return events


def evaluate_batch(snapshots: Iterable[MarketSnapshot], config: AlertConfig) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        events.extend(evaluate_snapshot(snapshot, config))
    return events


def _eval_short_breakout(snapshot: MarketSnapshot, config: AlertConfig) -> List[Dict[str, Any]]:
    rule_name = "short_breakout"
    rule_cfg = config.rule(rule_name)
    if not _rule_enabled_for_window(rule_cfg, snapshot.window):
        return []

    reasons: List[str] = []
    floor_ok, floor_reason = _check_min_change_floor(snapshot, rule_cfg)
    if not floor_ok:
        return []
    if floor_reason:
        reasons.append(floor_reason)

    force_hit, force_reason = _check_force_trigger(snapshot, rule_cfg)
    if force_hit:
        reasons.append(force_reason)
        force_level = str(rule_cfg.get("force_trigger_severity", "high"))
        force_confidence = _estimate_confidence(
            snapshot=snapshot,
            config=config,
            rule_name=rule_name,
            rule_cfg=rule_cfg,
            force_trigger=True,
        )
        return [_build_event(rule_name, snapshot, reasons, force_level, confidence=force_confidence)]

    if config.atr_enabled():
        price_ok, price_reason = _check_atr_threshold(snapshot, config, rule_name)
    else:
        price_ok, price_reason = _check_fixed_threshold(snapshot, config, rule_name)
    if not price_ok:
        return []
    reasons.append(price_reason)

    volume_ok, volume_reason = _check_volume_obv_filters(snapshot, rule_cfg)
    if not volume_ok:
        return []
    reasons.append(volume_reason)

    if bool(rule_cfg.get("use_rsi_filter", False)):
        rsi_ok, rsi_reason = _check_rsi_filter(snapshot, rule_cfg)
        if not rsi_ok:
            return []
        reasons.append(rsi_reason)

    confidence = _estimate_confidence(
        snapshot=snapshot,
        config=config,
        rule_name=rule_name,
        rule_cfg=rule_cfg,
    )
    return [_build_event(rule_name, snapshot, reasons, str(rule_cfg.get("severity", "medium")), confidence=confidence)]


def _eval_trend_acceleration(snapshot: MarketSnapshot, config: AlertConfig) -> List[Dict[str, Any]]:
    """
    Rule expression:
      (ATR/fixed-threshold breakout) OR (BB breakout + optional MACD confirm).
    """
    rule_name = "trend_acceleration"
    rule_cfg = config.rule(rule_name)
    if not _rule_enabled_for_window(rule_cfg, snapshot.window):
        return []

    reasons: List[str] = []
    floor_ok, floor_reason = _check_min_change_floor(snapshot, rule_cfg)
    if not floor_ok:
        return []
    if floor_reason:
        reasons.append(floor_reason)

    if config.atr_enabled():
        atr_hit, atr_reason = _check_atr_threshold(snapshot, config, rule_name)
    else:
        atr_hit, atr_reason = _check_fixed_threshold(snapshot, config, rule_name)

    use_bb_breakout = bool(rule_cfg.get("use_bb_breakout", False))
    use_macd_confirm = bool(rule_cfg.get("use_macd_confirm", False))

    bb_hit = False
    bb_reason = ""
    if use_bb_breakout:
        bb_hit, bb_reason = _check_bb_breakout(snapshot)

    macd_ok = True
    macd_reason = ""
    if use_macd_confirm:
        macd_ok, macd_reason = _check_macd_confirm(snapshot)

    bb_path_hit = bb_hit and (macd_ok if use_macd_confirm else True)
    if not atr_hit and not bb_path_hit:
        return []

    volume_ok, volume_reason = _check_volume_obv_filters(snapshot, rule_cfg)
    if not volume_ok:
        return []
    reasons.append(volume_reason)

    if atr_hit and atr_reason:
        reasons.append(atr_reason)

    if bb_hit and bb_reason:
        reasons.append(bb_reason)
        if use_macd_confirm and macd_ok and macd_reason:
            reasons.append(macd_reason)

    confidence = _estimate_confidence(
        snapshot=snapshot,
        config=config,
        rule_name=rule_name,
        rule_cfg=rule_cfg,
    )
    return [_build_event(rule_name, snapshot, reasons, str(rule_cfg.get("severity", "medium")), confidence=confidence)]


def _eval_long_anomaly(snapshot: MarketSnapshot, config: AlertConfig) -> List[Dict[str, Any]]:
    rule_name = "long_anomaly"
    rule_cfg = config.rule(rule_name)
    if not _rule_enabled_for_window(rule_cfg, snapshot.window):
        return []

    reasons: List[str] = []
    floor_ok, floor_reason = _check_min_change_floor(snapshot, rule_cfg)
    if not floor_ok:
        return []
    if floor_reason:
        reasons.append(floor_reason)

    if config.atr_enabled():
        atr_ok, atr_reason = _check_atr_threshold(snapshot, config, rule_name)
        if not atr_ok:
            return []
        reasons.append(atr_reason)
    else:
        threshold_ok, threshold_reason = _check_fixed_threshold(snapshot, config, rule_name)
        if not threshold_ok:
            return []
        reasons.append(threshold_reason)

    volume_ok, volume_reason = _check_volume_obv_filters(snapshot, rule_cfg)
    if not volume_ok:
        return []
    reasons.append(volume_reason)

    require_ma_sd = bool(rule_cfg.get("use_ma_sd_filter", True if config.atr_enabled() else False))
    if require_ma_sd:
        ma_ok, ma_reason = _check_ma_sd_filter(snapshot, rule_cfg)
        if not ma_ok:
            return []
        reasons.append(ma_reason)

    confidence = _estimate_confidence(
        snapshot=snapshot,
        config=config,
        rule_name=rule_name,
        rule_cfg=rule_cfg,
    )
    return [_build_event(rule_name, snapshot, reasons, str(rule_cfg.get("severity", "high")), confidence=confidence)]


def _rule_enabled_for_window(rule_cfg: Dict[str, Any], window: str) -> bool:
    if not rule_cfg.get("enabled", True):
        return False
    return window in rule_cfg.get("windows", [])


def _check_fixed_threshold(
    snapshot: MarketSnapshot,
    config: AlertConfig,
    rule_name: str,
) -> Tuple[bool, str]:
    threshold = config.threshold_for(rule_name, snapshot.window, default=0.0)
    change_abs_pct = abs(snapshot.change_pct)
    if change_abs_pct < threshold:
        return False, ""
    return True, f"绝对涨跌幅 {change_abs_pct:.2f}% >= 固定阈值 {threshold:.2f}%"


def _check_min_change_floor(snapshot: MarketSnapshot, rule_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    min_map = rule_cfg.get("min_change_pct", {})
    if not isinstance(min_map, dict):
        return True, ""

    min_change = _safe_float(min_map.get(snapshot.window), 0.0) or 0.0
    if min_change <= 0:
        return True, ""

    actual = abs(snapshot.change_pct)
    if actual < min_change:
        return False, ""
    return True, f"绝对涨跌幅 {actual:.2f}% >= 最低门槛 {min_change:.2f}%"


def _check_atr_threshold(snapshot: MarketSnapshot, config: AlertConfig, rule_name: str) -> Tuple[bool, str]:
    atr_val = _safe_float(snapshot.indicators.get("atr"))
    if atr_val is None or atr_val <= 0:
        return False, ""

    rule_cfg = config.rule(rule_name)
    k_window = config.atr_k_for(snapshot.window, default=1.5)
    rule_multiplier = _safe_float(rule_cfg.get("atr_multiplier"), 1.0) or 1.0
    k_effective = k_window * rule_multiplier
    min_rule_k = MIN_ATR_K_BY_RULE.get(rule_name, 0.0)
    if k_effective < min_rule_k:
        k_effective = min_rule_k

    threshold_abs = atr_val * k_effective
    if snapshot.abs_change < threshold_abs:
        return False, ""

    reason = (
        f"绝对波动 {snapshot.abs_change:.6f} >= ATR阈值 {threshold_abs:.6f} "
        f"(ATR={atr_val:.6f}, k={k_effective:.2f})"
    )
    return True, reason


def _check_volume_obv_filters(snapshot: MarketSnapshot, rule_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    base_multiplier = _safe_float(rule_cfg.get("volume_multiplier"), 1.8) or 1.8
    multiplier = _effective_volume_multiplier(snapshot=snapshot, base_multiplier=base_multiplier)
    base_delta_ratio = _safe_float(rule_cfg.get("volume_delta_min_ratio"), 0.2) or 0.2
    delta_ratio_threshold = _effective_volume_delta_ratio(snapshot=snapshot, base_ratio=base_delta_ratio)

    rvol = _safe_float(snapshot.indicators.get("rvol"))
    if rvol is None:
        if snapshot.avg_volume <= 0:
            return False, ""
        rvol = snapshot.volume / snapshot.avg_volume

    if rvol < multiplier:
        return False, ""

    obv_rising = bool(snapshot.indicators.get("obv_rising", False))
    obv_falling = bool(snapshot.indicators.get("obv_falling", False))

    obv_divergence = bool(snapshot.indicators.get("obv_divergence", False))
    if obv_divergence:
        return False, ""

    vwap = _safe_float(snapshot.indicators.get("vwap"))
    if vwap is None:
        return False, ""

    volume_delta = _safe_float(snapshot.indicators.get("volume_delta"))
    if volume_delta is None:
        return False, ""

    volume_delta_ratio = _safe_float(snapshot.indicators.get("volume_delta_ratio"))
    if volume_delta_ratio is None and snapshot.volume > 0:
        volume_delta_ratio = volume_delta / snapshot.volume
    if volume_delta_ratio is None:
        return False, ""
    if abs(volume_delta_ratio) < delta_ratio_threshold:
        return False, ""

    if snapshot.direction == "up":
        if not obv_rising:
            return False, ""
        if snapshot.close_price <= vwap:
            return False, ""
        if volume_delta <= 0:
            return False, ""
        delta_bias = "Volume Delta 强多头"
        obv_bias = "OBV 上升"
    elif snapshot.direction == "down":
        if not obv_falling:
            return False, ""
        if snapshot.close_price >= vwap:
            return False, ""
        if volume_delta >= 0:
            return False, ""
        delta_bias = "Volume Delta 强空头"
        obv_bias = "OBV 下降"
    else:
        return False, ""

    return (
        True,
        f"放量 {rvol:.1f}x + {obv_bias} + {delta_bias} "
        f"(RVOL阈值 {multiplier:.2f}x, VD阈值 {delta_ratio_threshold:.2f})",
    )

def _effective_volume_multiplier(snapshot: MarketSnapshot, base_multiplier: float) -> float:
    atr_relative = _safe_float(snapshot.indicators.get("atr_relative"))
    if atr_relative is not None and atr_relative > 1.5:
        return max(base_multiplier, 2.1)
    return base_multiplier


def _effective_volume_delta_ratio(snapshot: MarketSnapshot, base_ratio: float) -> float:
    atr_relative = _safe_float(snapshot.indicators.get("atr_relative"))
    if atr_relative is not None and atr_relative > 1.5:
        return max(base_ratio, 0.25)
    return base_ratio


def _check_force_trigger(snapshot: MarketSnapshot, rule_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    force_map = rule_cfg.get("force_trigger_pct", {})
    if not isinstance(force_map, dict):
        return False, ""

    force_threshold = _safe_float(force_map.get(snapshot.window), 0.0) or 0.0
    if force_threshold <= 0:
        return False, ""

    abs_change = abs(snapshot.change_pct)
    if abs_change < force_threshold:
        return False, ""

    min_rvol = _safe_float(rule_cfg.get("force_trigger_min_rvol"), 0.0) or 0.0
    rvol: Optional[float] = None
    if min_rvol > 0:
        rvol = _safe_float(snapshot.indicators.get("rvol"))
        if rvol is None and snapshot.avg_volume > 0:
            rvol = snapshot.volume / snapshot.avg_volume
        if rvol is None or rvol < min_rvol:
            return False, ""

    reason = f"绝对涨跌幅 {abs_change:.2f}% >= 强制触发阈值 {force_threshold:.2f}%"
    if min_rvol > 0 and rvol is not None:
        reason += f" + RVOL {rvol:.2f}x >= {min_rvol:.2f}x"
    return True, reason


def _check_rsi_filter(snapshot: MarketSnapshot, rule_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    rsi = _safe_float(snapshot.indicators.get("rsi"))
    if rsi is None:
        return False, ""

    upper = _safe_float(rule_cfg.get("rsi_upper"), 70.0) or 70.0
    lower = _safe_float(rule_cfg.get("rsi_lower"), 30.0) or 30.0

    if snapshot.direction == "up" and rsi <= upper:
        return False, ""
    if snapshot.direction == "down" and rsi >= lower:
        return False, ""
    return True, f"RSI过滤通过 (RSI={rsi:.2f}, upper={upper:.2f}, lower={lower:.2f})"


def _check_bb_breakout(snapshot: MarketSnapshot) -> Tuple[bool, str]:
    bb_upper = _safe_float(snapshot.indicators.get("bb_upper"))
    bb_lower = _safe_float(snapshot.indicators.get("bb_lower"))
    if bb_upper is None or bb_lower is None:
        return False, ""

    if snapshot.direction == "up" and snapshot.close_price > bb_upper:
        return True, f"布林上轨突破 (close={snapshot.close_price:.6f} > upper={bb_upper:.6f})"
    if snapshot.direction == "down" and snapshot.close_price < bb_lower:
        return True, f"布林下轨跌破 (close={snapshot.close_price:.6f} < lower={bb_lower:.6f})"
    return False, ""


def _check_macd_confirm(snapshot: MarketSnapshot) -> Tuple[bool, str]:
    hist = _safe_float(snapshot.indicators.get("macd_hist"))
    if hist is None:
        return False, ""

    if snapshot.direction == "up" and hist <= 0:
        return False, ""
    if snapshot.direction == "down" and hist >= 0:
        return False, ""
    return True, f"MACD动量确认 (hist={hist:.6f})"


def _check_ma_sd_filter(snapshot: MarketSnapshot, rule_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    ma = _safe_float(snapshot.indicators.get("ma"))
    std = _safe_float(snapshot.indicators.get("std_dev"))
    if ma is None or std is None or std <= 0:
        return False, ""

    sd_multiplier = _safe_float(rule_cfg.get("sd_multiplier"), 2.0) or 2.0
    deviation = abs(snapshot.close_price - ma)
    threshold = sd_multiplier * std
    if deviation < threshold:
        return False, ""
    return True, f"偏离均线 {deviation:.6f} >= {sd_multiplier:.2f}*STD({std:.6f})"


def _build_event(
    rule_name: str,
    snapshot: MarketSnapshot,
    reasons: List[str],
    level: str,
    confidence: Optional[float] = None,
) -> Dict[str, Any]:
    normalized_level = str(level or "medium").strip().lower()
    if normalized_level not in {"low", "medium", "high"}:
        normalized_level = "medium"

    return {
        "symbol": snapshot.symbol,
        "window": snapshot.window,
        "direction": snapshot.direction,
        "change_pct": snapshot.change_pct,
        "price": snapshot.close_price,
        "rule_name": rule_name,
        "reasons": reasons,
        "level": normalized_level,
        "confidence": confidence,
        "event_time": snapshot.event_time or datetime.now(),
    }


def _estimate_confidence(
    snapshot: MarketSnapshot,
    config: AlertConfig,
    rule_name: str,
    rule_cfg: Dict[str, Any],
    force_trigger: bool = False,
) -> float:
    # Confidence score for push display, range [40, 99].
    score = 55.0

    severity = str(rule_cfg.get("severity", "medium")).strip().lower()
    score += {"low": 0.0, "medium": 6.0, "high": 10.0}.get(severity, 6.0)
    if force_trigger:
        score += 8.0

    abs_change_pct = abs(snapshot.change_pct)
    min_change_map = rule_cfg.get("min_change_pct", {})
    if isinstance(min_change_map, dict):
        min_change = _safe_float(min_change_map.get(snapshot.window), 0.0) or 0.0
    else:
        min_change = 0.0
    if min_change > 0:
        score += min(10.0, max(0.0, abs_change_pct / min_change - 1.0) * 10.0)

    if config.atr_enabled():
        atr_val = _safe_float(snapshot.indicators.get("atr"))
        if atr_val is not None and atr_val > 0:
            atr_multiple = snapshot.abs_change / atr_val
            min_rule_k = MIN_ATR_K_BY_RULE.get(rule_name, 1.0)
            score += min(12.0, max(0.0, atr_multiple - min_rule_k) * 3.0)
    else:
        fixed_threshold = config.threshold_for(rule_name, snapshot.window, default=0.0)
        if fixed_threshold > 0:
            score += min(10.0, max(0.0, abs_change_pct / fixed_threshold - 1.0) * 10.0)

    rvol = _safe_float(snapshot.indicators.get("rvol"))
    if rvol is None and snapshot.avg_volume > 0:
        rvol = snapshot.volume / snapshot.avg_volume
    if rvol is not None:
        if rvol >= 1.0:
            score += min(10.0, (rvol - 1.0) * 2.5)
        else:
            score -= min(8.0, (1.0 - rvol) * 8.0)

    if snapshot.direction == "up":
        if bool(snapshot.indicators.get("obv_rising", False)):
            score += 2.0
    elif snapshot.direction == "down":
        if bool(snapshot.indicators.get("obv_falling", False)):
            score += 2.0
    if bool(snapshot.indicators.get("obv_divergence", False)):
        score -= 4.0

    macd_hist = _safe_float(snapshot.indicators.get("macd_hist"))
    if macd_hist is not None:
        if snapshot.direction == "up" and macd_hist > 0:
            score += 2.0
        elif snapshot.direction == "down" and macd_hist < 0:
            score += 2.0
        else:
            score -= 2.0

    score = max(40.0, min(99.0, score))
    return round(score, 1)


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
