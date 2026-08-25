from __future__ import annotations

from typing import Any, Dict, List, Optional


POLICY_VERSION = "position-pressure-v2-display-risk-holdout"
DEFAULT_DISPLAY_STATES = (
    "active_squeeze",
    "exhaustion",
    "position_control",
)


DEFAULT_THRESHOLDS: Dict[str, float] = {
    "crowded_share": 0.70,
    "extreme_share": 0.82,
    "profitable_control_ratio": 0.65,
    "pain_entry_gap_pct": 0.01,
    "pain_profit_ratio": 0.55,
    "smart_flow_imbalance": 0.15,
    "position_retention_floor": -0.03,
    "oi_positive": 0.01,
    "oi_falling": -0.015,
    "market_buy_ratio_up": 0.55,
    "market_buy_ratio_down": 0.45,
    "liquidation_noise_floor_usdt": 20_000.0,
    "liquidation_side_share": 0.60,
    "liquidation_volume_ratio": 0.03,
    "liquidation_oi_ratio": 0.001,
    "liquidation_large_usdt": 100_000.0,
    "exhaustion_abs_change_pct": 6.0,
}


def classify_position_pressure(
    *,
    smart_money: Optional[Dict[str, Any]],
    liquidation_v2: Optional[Dict[str, Any]],
    derivatives: Optional[Dict[str, Any]],
    latest: Optional[Dict[str, Any]],
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = dict(DEFAULT_THRESHOLDS)
    raw_settings = settings or {}
    raw_thresholds = raw_settings.get("thresholds")
    if isinstance(raw_thresholds, dict):
        for key, value in raw_thresholds.items():
            if key in cfg and value is not None:
                cfg[key] = _safe_float(value, cfg[key])

    smart = smart_money if isinstance(smart_money, dict) else {}
    liquidation = liquidation_v2 if isinstance(liquidation_v2, dict) else {}
    deriv = derivatives if isinstance(derivatives, dict) else {}
    event = latest if isinstance(latest, dict) else {}
    direction = str(event.get("direction") or "").strip().lower()
    if direction not in {"up", "down"}:
        direction = "neutral"

    phase = str(raw_settings.get("phase") or "shadow").strip().lower()
    if phase not in {"shadow", "display", "risk", "confirm"}:
        phase = "shadow"
    policy_version = str(raw_settings.get("policy_version") or POLICY_VERSION).strip() or POLICY_VERSION
    display_states = _display_states(raw_settings.get("display_states"))
    display_enabled = _parse_bool(raw_settings.get("display_enabled"), False) and phase in {
        "display",
        "risk",
        "confirm",
    }
    risk_enabled = _parse_bool(raw_settings.get("risk_enabled"), False) and phase in {"risk", "confirm"}
    confirmation_enabled = _parse_bool(raw_settings.get("confirmation_enabled"), False) and phase == "confirm"

    if not _parse_bool(raw_settings.get("enabled"), True):
        return _result(
            direction=direction,
            state="unknown",
            driver="disabled",
            confidence=0.0,
            evidence=[],
            smart_available=False,
            liquidation_available=False,
            phase=phase,
            display_enabled=False,
            risk_enabled=False,
            confirmation_enabled=False,
            policy_version=policy_version,
            display_states=display_states,
        )

    smart_available = bool(smart.get("data_available") and smart.get("is_fresh", True))
    liquidation_available = _liquidation_available(liquidation)
    if direction == "neutral" or (not smart_available and not liquidation_available):
        return _result(
            direction=direction,
            state="unknown",
            driver="unknown",
            confidence=0.0,
            evidence=[],
            smart_available=smart_available,
            liquidation_available=liquidation_available,
            phase=phase,
            display_enabled=display_enabled,
            risk_enabled=risk_enabled,
            confirmation_enabled=confirmation_enabled,
            policy_version=policy_version,
            display_states=display_states,
        )

    side = "short" if direction == "up" else "long"
    opposite_side = "long" if side == "short" else "short"
    cohort = "trader"
    crowd_share = _safe_float(smart.get(f"{cohort}_{side}_share"), 0.0)
    whale_share = _safe_float(smart.get(f"whale_{side}_share"), 0.0)
    profit_ratio = _safe_float(smart.get(f"{cohort}_{side}_profit_ratio"), 0.0)
    entry_gap = _safe_float(smart.get(f"{cohort}_{side}_entry_gap_pct"), 0.0)
    profit_delta = _first_number(
        smart,
        (
            f"{cohort}_{side}_profit_ratio_change_5m",
            f"{cohort}_{side}_profit_ratio_change_15m",
            f"{cohort}_{side}_profit_ratio_change_30m",
        ),
    )
    qty_delta = _first_number(
        smart,
        (
            f"{cohort}_{side}_qty_change_pct_5m",
            f"{cohort}_{side}_qty_change_pct_15m",
            f"{cohort}_{side}_qty_change_pct_30m",
        ),
    )
    smart_flow = _safe_float(smart.get("flow_imbalance_30m"), 0.0)
    whale_flow = _safe_float(smart.get("whale_flow_imbalance_30m"), 0.0)
    directional_smart_flow = smart_flow if direction == "up" else -smart_flow
    directional_whale_flow = whale_flow if direction == "up" else -whale_flow

    oi_change = _first_number(
        deriv,
        (
            "oi_amount_change_pct_15m",
            "oi_amount_change_pct_5m",
            "oi_amount_change_pct_1h",
            "oi_change_pct_15m",
            "oi_change_pct_5m",
            "oi_change_pct_1h",
            "oi_change_pct",
        ),
        default=0.0,
    )
    cvd_1m = _safe_float(deriv.get("cvd_usdt_1m"), 0.0)
    cvd_3m = _safe_float(deriv.get("cvd_usdt_3m"), 0.0)
    buy_ratio = _first_number(
        deriv,
        ("buy_aggressor_ratio_1m", "taker_buy_ratio", "buy_aggressor_ratio_3m"),
        default=0.0,
    )
    if direction == "up":
        market_flow_aligned = (cvd_1m > 0 or cvd_3m > 0) and buy_ratio >= cfg["market_buy_ratio_up"]
    else:
        market_flow_aligned = (cvd_1m < 0 or cvd_3m < 0) and 0 < buy_ratio <= cfg["market_buy_ratio_down"]

    target_liq = _safe_float(liquidation.get(f"micro_{side}_liq_usdt_1m"), 0.0)
    opposite_liq = _safe_float(liquidation.get(f"micro_{opposite_side}_liq_usdt_1m"), 0.0)
    liq_total = target_liq + opposite_liq
    liq_share = target_liq / liq_total if liq_total > 0 else 0.0
    trade_notional = _safe_float(deriv.get("trade_notional_usdt_1m"), 0.0)
    oi_usdt = _safe_float(deriv.get("oi_usdt"), 0.0)
    liq_volume_ratio = target_liq / trade_notional if trade_notional > 0 else 0.0
    liq_oi_ratio = target_liq / oi_usdt if oi_usdt > 0 else 0.0
    liquidation_active = (
        target_liq >= cfg["liquidation_noise_floor_usdt"]
        and liq_share >= cfg["liquidation_side_share"]
        and (
            liq_volume_ratio >= cfg["liquidation_volume_ratio"]
            or liq_oi_ratio >= cfg["liquidation_oi_ratio"]
            or target_liq >= cfg["liquidation_large_usdt"]
        )
    )

    crowded = smart_available and (
        crowd_share >= cfg["crowded_share"] or whale_share >= cfg["extreme_share"]
    )
    in_pain = smart_available and entry_gap >= cfg["pain_entry_gap_pct"] and (
        profit_ratio <= cfg["pain_profit_ratio"] or (profit_delta is not None and profit_delta < 0)
    )
    retained = qty_delta is not None and qty_delta >= cfg["position_retention_floor"]
    oi_positive = oi_change >= cfg["oi_positive"]
    oi_falling = oi_change <= cfg["oi_falling"]
    smart_flow_aligned = directional_smart_flow >= cfg["smart_flow_imbalance"]
    smart_flow_opposed = directional_smart_flow <= -cfg["smart_flow_imbalance"]
    side_control = crowded and profit_ratio >= cfg["profitable_control_ratio"] and entry_gap <= 0
    abs_change = abs(_safe_float(event.get("change_pct"), 0.0))

    evidence: List[str] = []
    if crowded:
        evidence.append(f"{side}_crowded:{crowd_share:.3f}")
    if in_pain:
        evidence.append(f"{side}_pain:{entry_gap:.4f}")
    if qty_delta is not None:
        evidence.append(f"{side}_qty_delta:{qty_delta:+.4f}")
    if smart_flow_aligned:
        evidence.append(f"smart_flow_aligned:{directional_smart_flow:+.3f}")
    elif smart_flow_opposed:
        evidence.append(f"smart_flow_opposed:{directional_smart_flow:+.3f}")
    if directional_whale_flow >= cfg["smart_flow_imbalance"]:
        evidence.append(f"whale_flow_aligned:{directional_whale_flow:+.3f}")
    if market_flow_aligned:
        evidence.append("market_flow_aligned")
    if oi_positive:
        evidence.append(f"oi_positive:{oi_change:+.4f}")
    elif oi_falling:
        evidence.append(f"oi_falling:{oi_change:+.4f}")
    if liquidation_active:
        evidence.append(f"{side}_liquidation_active:{target_liq:.0f}")

    state = "neutral"
    driver = "mixed"
    confidence = 20.0
    confirmation_passed = False
    risk_modifier = 0.0

    weak_after_liquidation = not market_flow_aligned
    if liquidation_active and oi_falling and (
        weak_after_liquidation or abs_change >= cfg["exhaustion_abs_change_pct"]
    ):
        state = "exhaustion"
        driver = "short_cover" if direction == "up" else "long_liquidation"
        confidence = 82.0 if smart_available else 70.0
        risk_modifier = 8.0
    elif liquidation_active:
        state = "active_squeeze"
        driver = "short_cover" if direction == "up" else "long_liquidation"
        confidence = 76.0 if market_flow_aligned else 64.0
        risk_modifier = 5.0 if oi_falling else 2.0
    elif crowded and in_pain and market_flow_aligned and not oi_falling and (retained or oi_positive):
        state = "pre_squeeze"
        driver = "position_pressure"
        confidence = 84.0 if smart_flow_aligned or directional_whale_flow > 0 else 74.0
        confirmation_passed = confidence >= 75.0
    elif side_control and smart_flow_opposed:
        state = "position_control"
        driver = f"profitable_{side}s"
        confidence = 76.0
        risk_modifier = 4.0
    elif crowded:
        state = "crowded"
        driver = f"crowded_{side}s"
        confidence = 58.0
        risk_modifier = 1.0
    elif smart_available and smart_flow_aligned and market_flow_aligned and oi_positive:
        state = "organic_continuation"
        driver = "new_positions"
        confidence = 70.0
    elif liquidation_available:
        state = "no_active_pressure"
        driver = "organic_or_unknown"
        confidence = 42.0

    conflict = state == "position_control"
    shadow_risk_strength, shadow_risk_components = _shadow_risk_strength(
        state=state,
        liquidation_available=liquidation_available,
        target_liquidation_usdt=target_liq,
        liquidation_volume_ratio=liq_volume_ratio,
        liquidation_oi_ratio=liq_oi_ratio,
        oi_change=oi_change,
        abs_change=abs_change,
        market_flow_available=_market_flow_available(deriv),
        market_flow_aligned=market_flow_aligned,
    )
    result = _result(
        direction=direction,
        state=state,
        driver=driver,
        confidence=confidence,
        evidence=evidence,
        smart_available=smart_available,
        liquidation_available=liquidation_available,
        phase=phase,
        display_enabled=display_enabled,
        risk_enabled=risk_enabled,
        confirmation_enabled=confirmation_enabled,
        policy_version=policy_version,
        display_states=display_states,
    )
    result.update(
        {
            "target_position_side": side,
            "crowd_share": round(crowd_share, 6),
            "whale_crowd_share": round(whale_share, 6),
            "target_profit_ratio": round(profit_ratio, 6),
            "target_entry_gap_pct": round(entry_gap, 6),
            "target_qty_change_pct": round(qty_delta, 6) if qty_delta is not None else None,
            "smart_flow_imbalance_30m": round(smart_flow, 6),
            "whale_flow_imbalance_30m": round(whale_flow, 6),
            "oi_change_pct": round(oi_change, 6),
            "target_liquidation_usdt_1m": round(target_liq, 2),
            "liquidation_side_share_1m": round(liq_share, 6),
            "liquidation_volume_ratio_1m": round(liq_volume_ratio, 6),
            "liquidation_oi_ratio_1m": round(liq_oi_ratio, 8),
            "market_flow_aligned": market_flow_aligned,
            "conflict_with_price_direction": conflict,
            "shadow_confirmation_passed": confirmation_passed,
            "shadow_risk_modifier": risk_modifier,
            "shadow_risk_strength": shadow_risk_strength,
            "shadow_risk_components": shadow_risk_components,
            "confirmation_passed": bool(confirmation_enabled and confirmation_passed),
            "risk_modifier": risk_modifier if risk_enabled else 0.0,
        }
    )
    return result


def _result(
    *,
    direction: str,
    state: str,
    driver: str,
    confidence: float,
    evidence: List[str],
    smart_available: bool,
    liquidation_available: bool,
    phase: str,
    display_enabled: bool,
    risk_enabled: bool,
    confirmation_enabled: bool,
    policy_version: str,
    display_states: tuple[str, ...],
) -> Dict[str, Any]:
    return {
        "policy_version": policy_version,
        "phase": phase,
        "direction": direction,
        "state": state,
        "driver": driver,
        "confidence": round(max(0.0, min(100.0, confidence)), 2),
        "evidence": list(evidence),
        "smart_money_available": smart_available,
        "liquidation_v2_available": liquidation_available,
        "data_valid": bool(smart_available or liquidation_available),
        "display_enabled": display_enabled,
        "display_states": list(display_states),
        "display_allowed": bool(display_enabled and state in display_states),
        "risk_enabled": risk_enabled,
        "confirmation_enabled": confirmation_enabled,
        "shadow_confirmation_passed": False,
        "shadow_risk_modifier": 0.0,
        "shadow_risk_strength": 0.0,
        "shadow_risk_components": {
            "liquidation": 0.0,
            "oi_decline": 0.0,
            "price_extension": 0.0,
            "flow_conflict": 0.0,
            "state_floor": 0.0,
        },
        "confirmation_passed": False,
        "risk_modifier": 0.0,
    }


def _shadow_risk_strength(
    *,
    state: str,
    liquidation_available: bool,
    target_liquidation_usdt: float,
    liquidation_volume_ratio: float,
    liquidation_oi_ratio: float,
    oi_change: float,
    abs_change: float,
    market_flow_available: bool,
    market_flow_aligned: bool,
) -> tuple[float, Dict[str, float]]:
    liquidation_intensity = 0.0
    if liquidation_available:
        liquidation_intensity = max(
            _clamp01(target_liquidation_usdt / 100_000.0),
            _clamp01(liquidation_volume_ratio / 0.03),
            _clamp01(liquidation_oi_ratio / 0.001),
        )
    liquidation_score = 35.0 * liquidation_intensity
    oi_decline_score = 25.0 * _clamp01(-oi_change / 0.05)
    price_extension_score = 20.0 * _clamp01((abs_change - 3.0) / 12.0)
    flow_conflict_score = 20.0 if market_flow_available and not market_flow_aligned else 0.0
    state_floor = {
        "crowded": 20.0,
        "active_squeeze": 50.0,
        "position_control": 65.0,
        "exhaustion": 75.0,
    }.get(state, 0.0)
    raw_score = liquidation_score + oi_decline_score + price_extension_score + flow_conflict_score
    score = max(state_floor, min(100.0, raw_score))
    return round(score, 2), {
        "liquidation": round(liquidation_score, 2),
        "oi_decline": round(oi_decline_score, 2),
        "price_extension": round(price_extension_score, 2),
        "flow_conflict": round(flow_conflict_score, 2),
        "state_floor": round(state_floor, 2),
    }


def _market_flow_available(derivatives: Dict[str, Any]) -> bool:
    return any(
        derivatives.get(key) is not None
        for key in (
            "cvd_usdt_1m",
            "cvd_usdt_3m",
            "buy_aggressor_ratio_1m",
            "taker_buy_ratio",
            "buy_aggressor_ratio_3m",
        )
    )


def _display_states(value: Any) -> tuple[str, ...]:
    allowed = {
        "position_control",
        "crowded",
        "pre_squeeze",
        "active_squeeze",
        "exhaustion",
        "organic_continuation",
    }
    if not isinstance(value, (list, tuple, set)):
        return DEFAULT_DISPLAY_STATES
    states = tuple(
        dict.fromkeys(
            str(item).strip().lower()
            for item in value
            if str(item).strip().lower() in allowed
        )
    )
    return states or DEFAULT_DISPLAY_STATES


def _clamp01(value: Any) -> float:
    return max(0.0, min(1.0, _safe_float(value, 0.0)))


def _liquidation_available(payload: Dict[str, Any]) -> bool:
    return bool(payload and payload.get("tracking") is True)


def _first_number(payload: Dict[str, Any], keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


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
