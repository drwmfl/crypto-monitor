from __future__ import annotations

from typing import Any, Dict, Tuple

try:
    from candidates.candidate_models import Candidate
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate


def score_candidate(candidate: Candidate) -> Tuple[float, Dict[str, Any], str]:
    latest = candidate.latest_features or {}
    latest_change = abs(_safe_float(latest.get("change_pct"), 0.0))
    abs_change = max(latest_change, float(candidate.max_abs_change_pct or 0.0))
    rvol = max(_safe_float(latest.get("rvol"), 0.0), float(candidate.max_rvol or 0.0))
    confidence = max(_safe_float(latest.get("confidence"), 0.0), float(candidate.max_confidence or 0.0))

    price_score = min(25.0, abs_change * 4.0)
    volume_score = 0.0 if rvol <= 1.0 else min(20.0, (rvol - 1.0) * 6.0)
    repeat_score = min(15.0, max(0, candidate.event_count - 1) * 4.0 + (4.0 if candidate.event_count >= 3 else 0.0))
    multi_window_score = min(15.0, max(0, len(candidate.windows) - 1) * 7.5)
    confidence_score = min(20.0, confidence * 0.2)
    severity_score = 0.0
    if candidate.severities.get("high", 0) > 0:
        severity_score += 3.0
    if candidate.sources.get("ws", 0) > 0:
        severity_score += 2.0
    severity_score = min(5.0, severity_score)
    derivatives_score = _score_derivatives(candidate)
    orderbook_score = _score_orderbook(candidate)
    liquidation_score = _score_liquidation(candidate)
    accumulation_score = _score_accumulation(candidate)
    factor_score = min(
        25.0,
        derivatives_score
        + orderbook_score
        + liquidation_score
        + accumulation_score,
    )

    total = round(
        min(
            100.0,
            price_score
            + volume_score
            + repeat_score
            + multi_window_score
            + confidence_score
            + severity_score
            + factor_score,
        ),
        2,
    )
    priority = "actionable" if total >= 75.0 else "watch" if total >= 50.0 else "low"
    breakdown = {
        "total": total,
        "price_momentum": round(price_score, 2),
        "volume_confirmation": round(volume_score, 2),
        "repeat_trigger": round(repeat_score, 2),
        "multi_window": round(multi_window_score, 2),
        "confidence": round(confidence_score, 2),
        "severity_source": round(severity_score, 2),
        "factor_confirmation": round(factor_score, 2),
        "derivatives_confirmation": round(derivatives_score, 2),
        "orderbook_confirmation": round(orderbook_score, 2),
        "liquidation_confirmation": round(liquidation_score, 2),
        "accumulation_confirmation": round(accumulation_score, 2),
        "raw": {
            "abs_change_pct": round(abs_change, 4),
            "rvol": round(rvol, 4),
            "confidence": round(confidence, 4),
            "event_count": candidate.event_count,
            "windows": list(candidate.windows),
        },
    }
    return total, breakdown, priority


def _score_derivatives(candidate: Candidate) -> float:
    derivatives = candidate.derivatives or {}
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").lower()
    oi_change_pct = max(
        0.0,
        _safe_float(derivatives.get("oi_change_pct"), 0.0),
        _safe_float(derivatives.get("oi_change_pct_5m"), 0.0),
        _safe_float(derivatives.get("oi_change_pct_15m"), 0.0),
        _safe_float(derivatives.get("oi_change_pct_1h"), 0.0),
    )
    funding_rate = abs(_safe_float(derivatives.get("funding_rate"), 0.0))
    taker_buy_ratio = _safe_float(derivatives.get("taker_buy_ratio"), 0.0)
    oi_signal_level = str(derivatives.get("oi_signal_level") or "none")
    oi_regime = str(derivatives.get("oi_regime") or "")
    micro_regime = str(derivatives.get("micro_regime") or "")
    micro_signal_level = str(derivatives.get("micro_signal_level") or "none")
    cvd_10s = _safe_float(derivatives.get("cvd_usdt_10s"), 0.0)
    cvd_30s = _safe_float(derivatives.get("cvd_usdt_30s"), 0.0)
    cvd_1m = _safe_float(derivatives.get("cvd_usdt_1m"), 0.0)
    cvd_3m = _safe_float(derivatives.get("cvd_usdt_3m"), 0.0)
    cvd_5m = _safe_float(derivatives.get("cvd_usdt_5m"), 0.0)
    buy_aggressor_ratio_10s = _safe_float(derivatives.get("buy_aggressor_ratio_10s"), 0.0)
    buy_aggressor_ratio_30s = _safe_float(derivatives.get("buy_aggressor_ratio_30s"), 0.0)
    buy_aggressor_ratio_1m = _safe_float(derivatives.get("buy_aggressor_ratio_1m"), 0.0)
    buy_aggressor_ratio_3m = _safe_float(derivatives.get("buy_aggressor_ratio_3m"), 0.0)
    buy_aggressor_ratio_5m = _safe_float(derivatives.get("buy_aggressor_ratio_5m"), 0.0)
    score = 0.0
    if oi_change_pct > 0:
        score += min(4.0, oi_change_pct * 80.0)
    signal_rank = _oi_signal_rank(oi_signal_level)
    if signal_rank >= 3:
        score += 6.0
    elif signal_rank == 2:
        score += 4.0
    elif signal_rank == 1:
        score += 2.5
    elif signal_rank == 0:
        score += 1.0
    if direction == "up" and oi_regime == "new_longs":
        score += 3.0
    elif oi_regime == "accumulation":
        score += 2.0
    elif oi_regime in {"short_cover", "churn"}:
        score += 0.5
    elif oi_regime in {"new_shorts", "deleveraging"}:
        score -= 3.0
    if direction == "up" and taker_buy_ratio >= 0.55:
        score += min(2.0, (taker_buy_ratio - 0.5) * 20.0)
    if direction in {"up", "down"} and _safe_float(derivatives.get("oi_usdt"), 0.0) > 0:
        score += 2.0
    if 0 < funding_rate <= 0.0008:
        score += 1.5
    micro_rank = _micro_signal_rank(micro_signal_level)
    if micro_rank >= 0:
        score += 0.5
    if micro_rank >= 2:
        score += 1.0
    if micro_rank >= 3:
        score += 1.0
    if direction == "up" and micro_regime == "breakout_continuation":
        score += 3.5
        if cvd_1m > 0 and cvd_3m > 0 and cvd_5m > 0:
            score += min(2.0, (cvd_1m / 120000.0) + (cvd_3m / 240000.0) + (cvd_5m / 500000.0))
        if (
            buy_aggressor_ratio_1m >= 0.57
            and buy_aggressor_ratio_3m >= 0.55
            and buy_aggressor_ratio_5m >= 0.54
        ):
            score += 1.0
    elif direction == "up" and micro_regime == "new_longs":
        score += 2.5
        if cvd_1m > 0 and cvd_5m > 0:
            score += min(1.5, max(cvd_1m, 0.0) / 100000.0 + max(cvd_5m, 0.0) / 300000.0)
        if buy_aggressor_ratio_1m >= 0.55 and buy_aggressor_ratio_5m >= 0.53:
            score += 1.0
    elif direction == "up" and micro_regime == "absorption_reversal":
        score += 1.8
        if cvd_10s > 0 and cvd_30s > 0 and cvd_1m > 0:
            score += min(1.4, (cvd_10s / 25000.0) + (cvd_30s / 50000.0) + (cvd_1m / 120000.0))
        if (
            buy_aggressor_ratio_10s >= 0.60
            and buy_aggressor_ratio_30s >= 0.58
            and buy_aggressor_ratio_1m >= 0.55
        ):
            score += 0.8
    elif direction == "up" and micro_regime == "short_cover":
        score += 0.8
    elif micro_regime == "churn":
        score -= 1.0
    return max(0.0, min(14.0, score))


def _score_orderbook(candidate: Candidate) -> float:
    orderbook = candidate.orderbook or {}
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").lower()
    imbalance = _safe_float(orderbook.get("imbalance"), 0.0)
    spread_bps = _safe_float(orderbook.get("spread_bps"), 0.0)
    bid_notional = _safe_float(orderbook.get("bid_notional"), 0.0)
    ask_notional = _safe_float(orderbook.get("ask_notional"), 0.0)
    if bid_notional <= 0 and ask_notional <= 0:
        return 0.0
    score = 0.0
    if direction == "up" and imbalance > 0:
        score += min(6.0, imbalance * 12.0)
    elif direction == "down" and imbalance < 0:
        score += min(6.0, abs(imbalance) * 12.0)
    if 0 < spread_bps <= 8:
        score += 1.0
    return min(7.0, score)


def _score_liquidation(candidate: Candidate) -> float:
    liquidation = candidate.liquidation or {}
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").lower()
    long_liq = _safe_float(liquidation.get("long_liq_usdt"), 0.0)
    short_liq = _safe_float(liquidation.get("short_liq_usdt"), 0.0)
    if direction == "up" and short_liq > long_liq:
        return min(4.0, (short_liq - long_liq) / 100000.0)
    if direction == "down" and long_liq > short_liq:
        return min(4.0, (long_liq - short_liq) / 100000.0)
    return 0.0


def _score_accumulation(candidate: Candidate) -> float:
    accumulation = candidate.accumulation or {}
    if not accumulation or not accumulation.get("in_accumulation_pool"):
        return 0.0
    status = str(accumulation.get("status") or "").lower()
    pool_score = _safe_float(accumulation.get("score"), 0.0)
    vol_ratio = _safe_float(accumulation.get("recent_vol_ratio_7d"), 0.0)
    range_position = _safe_float(accumulation.get("range_position"), 0.0)
    data_quality = str(accumulation.get("data_quality") or "").lower()

    score = 0.0
    if status == "ready":
        score += 4.5
    elif status == "warming":
        score += 3.0
    elif status == "dormant":
        score += 1.5

    if pool_score >= 80:
        score += 1.5
    elif pool_score >= 65:
        score += 1.0
    elif pool_score >= 50:
        score += 0.5
    if vol_ratio >= 2.5:
        score += 1.0
    elif vol_ratio >= 1.5:
        score += 0.5
    if 0.45 <= range_position <= 1.15:
        score += 0.5
    if data_quality == "estimated":
        score *= 0.8
    return max(0.0, min(7.0, score))


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
