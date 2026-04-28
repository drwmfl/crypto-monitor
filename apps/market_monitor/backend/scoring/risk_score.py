from __future__ import annotations

from typing import Any, Dict, Tuple

try:
    from candidates.candidate_models import Candidate
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate


def score_risk(candidate: Candidate) -> Tuple[float, str, Dict[str, Any]]:
    latest = candidate.latest_features or {}
    latest_change = abs(_safe_float(latest.get("change_pct"), 0.0))
    max_change = max(latest_change, float(candidate.max_abs_change_pct or 0.0))
    latest_window = str(latest.get("window") or "")
    rvol = max(_safe_float(latest.get("rvol"), 0.0), float(candidate.max_rvol or 0.0))
    confidence = max(_safe_float(latest.get("confidence"), 0.0), float(candidate.max_confidence or 0.0))

    overheat = 0.0
    if latest_window == "1m":
        overheat = min(30.0, max_change * 5.0)
    elif latest_window == "5m":
        overheat = min(25.0, max_change * 3.0)
    else:
        overheat = min(20.0, max_change * 1.8)

    direction_conflict = 0.0
    if candidate.directions.get("up", 0) > 0 and candidate.directions.get("down", 0) > 0:
        direction_conflict = 15.0

    single_factor = 0.0
    if candidate.event_count <= 1 and len(candidate.windows) <= 1:
        single_factor = 15.0

    weak_confirmation = 0.0
    if rvol <= 1.2:
        weak_confirmation += 8.0
    if confidence and confidence < 65.0:
        weak_confirmation += 7.0
    weak_confirmation = min(15.0, weak_confirmation)

    crowding_noise = 0.0
    if candidate.event_count >= 8:
        crowding_noise = min(15.0, (candidate.event_count - 7) * 3.0)

    candle_state_risk = 8.0 if str(latest.get("candle_state") or "").lower() == "open" else 0.0
    derivatives_risk = _risk_derivatives(candidate)
    orderbook_risk = _risk_orderbook(candidate)
    liquidation_risk = _risk_liquidation(candidate)
    accumulation_risk = _risk_accumulation(candidate)

    total = round(
        min(
            100.0,
            overheat
            + direction_conflict
            + single_factor
            + weak_confirmation
            + crowding_noise
            + candle_state_risk
            + derivatives_risk
            + orderbook_risk
            + liquidation_risk
            + accumulation_risk,
        ),
        2,
    )
    level = "high" if total >= 60.0 else "medium" if total >= 35.0 else "low"
    breakdown = {
        "total": total,
        "overheat": round(overheat, 2),
        "direction_conflict": round(direction_conflict, 2),
        "single_factor": round(single_factor, 2),
        "weak_confirmation": round(weak_confirmation, 2),
        "crowding_noise": round(crowding_noise, 2),
        "open_candle": round(candle_state_risk, 2),
        "derivatives_risk": round(derivatives_risk, 2),
        "orderbook_risk": round(orderbook_risk, 2),
        "liquidation_risk": round(liquidation_risk, 2),
        "accumulation_risk": round(accumulation_risk, 2),
        "raw": {
            "latest_window": latest_window,
            "max_change_pct": round(max_change, 4),
            "rvol": round(rvol, 4),
            "confidence": round(confidence, 4),
            "event_count": candidate.event_count,
        },
    }
    return total, level, breakdown


def _risk_derivatives(candidate: Candidate) -> float:
    derivatives = candidate.derivatives or {}
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").lower()
    oi_change_pct = max(
        abs(_safe_float(derivatives.get("oi_change_pct"), 0.0)),
        abs(_safe_float(derivatives.get("oi_change_pct_5m"), 0.0)),
        abs(_safe_float(derivatives.get("oi_change_pct_15m"), 0.0)),
        abs(_safe_float(derivatives.get("oi_change_pct_1h"), 0.0)),
    )
    funding_rate = abs(_safe_float(derivatives.get("funding_rate"), 0.0))
    taker_buy_ratio = _safe_float(derivatives.get("taker_buy_ratio"), 0.0)
    oi_regime = str(derivatives.get("oi_regime") or "")
    oi_signal_level = str(derivatives.get("oi_signal_level") or "none")
    micro_regime = str(derivatives.get("micro_regime") or "")
    micro_signal_level = str(derivatives.get("micro_signal_level") or "none")
    cvd_30s = _safe_float(derivatives.get("cvd_usdt_30s"), 0.0)
    cvd_1m = _safe_float(derivatives.get("cvd_usdt_1m"), 0.0)
    cvd_5m = _safe_float(derivatives.get("cvd_usdt_5m"), 0.0)
    buy_aggressor_ratio_30s = _safe_float(derivatives.get("buy_aggressor_ratio_30s"), 0.0)
    buy_aggressor_ratio_1m = _safe_float(derivatives.get("buy_aggressor_ratio_1m"), 0.0)
    micro_liq_imbalance_1m = _safe_float(derivatives.get("micro_liq_imbalance_usdt_1m"), 0.0)
    micro_short_liq_1m = _safe_float(derivatives.get("micro_short_liq_usdt_1m"), 0.0)
    risk = 0.0
    if oi_change_pct >= 0.08 and oi_regime not in {"new_longs", "accumulation"}:
        risk += min(12.0, oi_change_pct * 90.0)
    if funding_rate >= 0.0015:
        risk += min(12.0, funding_rate * 6000.0)
    if oi_regime in {"new_shorts", "deleveraging"}:
        risk += 8.0
    elif oi_regime in {"short_cover", "churn"}:
        risk += 5.0
    if direction == "up" and taker_buy_ratio > 0 and taker_buy_ratio < 0.48 and _oi_signal_rank(oi_signal_level) >= 1:
        risk += 5.0
    if _oi_signal_rank(oi_signal_level) >= 3 and funding_rate >= 0.001:
        risk += 4.0
    micro_rank = _micro_signal_rank(micro_signal_level)
    if direction == "up" and micro_regime == "short_cover":
        risk += 6.0
        if micro_short_liq_1m > 0:
            risk += min(4.0, micro_short_liq_1m / 150000.0)
    elif micro_regime == "churn":
        risk += 4.0
    elif direction == "up" and micro_regime == "absorption_reversal":
        if cvd_30s <= 0:
            risk += 2.0
        if 0 < buy_aggressor_ratio_30s < 0.54:
            risk += 2.0
    if direction == "up" and micro_rank >= 1 and cvd_1m < 0:
        risk += 4.0
    if direction == "up" and micro_rank >= 1 and buy_aggressor_ratio_1m < 0.5:
        risk += 3.0
    if direction == "up" and micro_liq_imbalance_1m > 100000.0 and micro_regime not in {
        "new_longs",
        "breakout_continuation",
        "absorption_reversal",
    }:
        risk += 3.0
    if direction == "up" and micro_rank >= 2 and cvd_5m < 0:
        risk += 3.0
    return min(22.0, risk)


def _risk_orderbook(candidate: Candidate) -> float:
    orderbook = candidate.orderbook or {}
    spread_bps = _safe_float(orderbook.get("spread_bps"), 0.0)
    bid_notional = _safe_float(orderbook.get("bid_notional"), 0.0)
    ask_notional = _safe_float(orderbook.get("ask_notional"), 0.0)
    imbalance = abs(_safe_float(orderbook.get("imbalance"), 0.0))
    risk = 0.0
    if spread_bps > 15:
        risk += min(10.0, spread_bps / 3.0)
    if 0 < (bid_notional + ask_notional) < 100000:
        risk += 8.0
    if imbalance >= 0.75:
        risk += 5.0
    return min(18.0, risk)


def _risk_liquidation(candidate: Candidate) -> float:
    liquidation = candidate.liquidation or {}
    long_liq = _safe_float(liquidation.get("long_liq_usdt"), 0.0)
    short_liq = _safe_float(liquidation.get("short_liq_usdt"), 0.0)
    total_liq = long_liq + short_liq
    if total_liq <= 0:
        return 0.0
    return min(12.0, total_liq / 250000.0)


def _risk_accumulation(candidate: Candidate) -> float:
    accumulation = candidate.accumulation or {}
    if not accumulation or not accumulation.get("in_accumulation_pool"):
        return 0.0
    status = str(accumulation.get("status") or "").lower()
    data_quality = str(accumulation.get("data_quality") or "").lower()
    avg_vol = _safe_float(accumulation.get("avg_quote_vol_usdt"), 0.0)
    range_position = _safe_float(accumulation.get("range_position"), 0.0)
    risk = 0.0
    if status == "invalid":
        risk += 8.0
    if data_quality == "estimated":
        risk += 2.0
    if 0 < avg_vol < 500000:
        risk += 4.0
    if range_position > 1.35:
        risk += 3.0
    elif range_position < -0.05:
        risk += 3.0
    return min(8.0, risk)


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _oi_signal_rank(level: str) -> int:
    text = str(level or "").strip().upper()
    return {"L0": 0, "L1": 1, "L2": 2, "L3": 3}.get(text, -1)


def _micro_signal_rank(level: str) -> int:
    text = str(level or "").strip().upper()
    return {"L0": 0, "L1": 1, "L2": 2, "L3": 3}.get(text, -1)
