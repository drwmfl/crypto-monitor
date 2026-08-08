from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

try:
    from candidates.candidate_models import Candidate
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate


DEFAULT_TRADE_CONFIRMATION: Dict[str, Any] = {
    "trade_confirmation_enabled": True,
    "trade_confirmation_min_confirmations": 3,
    "trade_confirmation_min_actionable_confirmations": 3,
    "trade_confirmation_min_strong_direct_confirmations": 3,
    "trade_confirmation_min_event_count": 2,
    "trade_confirmation_min_oi_signal_level": "L1",
    "trade_confirmation_min_oi_change_pct": 0.015,
    "trade_confirmation_up_taker_buy_ratio": 0.55,
    "trade_confirmation_down_taker_buy_ratio": 0.45,
    "trade_confirmation_up_buy_aggressor_ratio": 0.55,
    "trade_confirmation_down_buy_aggressor_ratio": 0.45,
    "trade_confirmation_min_liquidation_usdt": 20_000.0,
    "trade_confirmation_orderbook_imbalance": 0.08,
    "trade_confirmation_max_spread_bps": 12.0,
    "trade_confirmation_min_depth_notional": 0.0,
    "position_pressure_confirmation_enabled": False,
}


@dataclass
class ConfirmationCheck:
    key: str
    passed: bool
    value: Any = None
    reason: str = ""


@dataclass
class TradeConfirmation:
    enabled: bool
    direction: str
    passed_count: int
    required_count: int
    actionable_required_count: int
    strong_direct_required_count: int
    stage: str
    checks: List[ConfirmationCheck] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["checks"] = [asdict(item) for item in self.checks]
        return payload

    @property
    def passed(self) -> bool:
        return self.passed_count >= self.required_count

    @property
    def actionable_passed(self) -> bool:
        return self.passed_count >= self.actionable_required_count

    @property
    def strong_direct_passed(self) -> bool:
        return self.passed_count >= self.strong_direct_required_count


def evaluate_trade_confirmation(candidate: Candidate, settings: Dict[str, Any] | None = None) -> TradeConfirmation:
    cfg = dict(DEFAULT_TRADE_CONFIRMATION)
    if settings:
        cfg.update({key: value for key, value in settings.items() if key in cfg})

    enabled = _parse_bool(cfg.get("trade_confirmation_enabled"), True)
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").strip().lower()
    if direction not in {"up", "down"}:
        direction = "unknown"

    required_count = max(1, _to_int(cfg.get("trade_confirmation_min_confirmations"), 3))
    actionable_required_count = max(1, _to_int(cfg.get("trade_confirmation_min_actionable_confirmations"), required_count))
    strong_direct_required_count = max(
        1,
        _to_int(cfg.get("trade_confirmation_min_strong_direct_confirmations"), required_count),
    )

    if not enabled:
        return TradeConfirmation(
            enabled=False,
            direction=direction,
            passed_count=0,
            required_count=required_count,
            actionable_required_count=actionable_required_count,
            strong_direct_required_count=strong_direct_required_count,
            stage="disabled",
            checks=[],
        )

    pressure_enabled = _parse_bool(
        cfg.get("position_pressure_confirmation_enabled"),
        False,
    ) and bool((candidate.position_pressure or {}).get("confirmation_enabled"))
    checks = [
        _check_price_persistence(candidate, cfg),
        _check_oi(candidate, cfg, direction),
        _check_flow(candidate, cfg, direction),
        _check_orderbook(candidate, cfg, direction),
        _check_position_pressure(candidate, direction)
        if pressure_enabled
        else _check_liquidation(candidate, cfg, direction),
    ]
    passed_count = sum(1 for item in checks if item.passed)
    if passed_count >= max(actionable_required_count, strong_direct_required_count):
        stage = "trade_ready"
    elif passed_count >= required_count:
        stage = "confirmed"
    elif passed_count > 0:
        stage = "tracking"
    else:
        stage = "new_pulse"

    return TradeConfirmation(
        enabled=True,
        direction=direction,
        passed_count=passed_count,
        required_count=required_count,
        actionable_required_count=actionable_required_count,
        strong_direct_required_count=strong_direct_required_count,
        stage=stage,
        checks=checks,
    )


def _check_price_persistence(candidate: Candidate, cfg: Dict[str, Any]) -> ConfirmationCheck:
    min_events = max(1, _to_int(cfg.get("trade_confirmation_min_event_count"), 2))
    event_count = int(candidate.event_count or 0)
    window_count = len(candidate.windows or [])
    passed = event_count >= min_events or window_count >= 2
    return ConfirmationCheck(
        key="price_persistence",
        passed=passed,
        value={"event_count": event_count, "windows": list(candidate.windows or [])},
        reason="repeat_or_multi_window" if passed else "single_pulse",
    )


def _check_oi(candidate: Candidate, cfg: Dict[str, Any], direction: str) -> ConfirmationCheck:
    derivatives = candidate.derivatives or {}
    min_rank = _signal_rank(cfg.get("trade_confirmation_min_oi_signal_level"))
    rank = _signal_rank(derivatives.get("oi_signal_level"))
    min_change = _to_float(cfg.get("trade_confirmation_min_oi_change_pct"), 0.015)
    oi_change = max(
        _to_float(derivatives.get("oi_change_pct"), 0.0),
        _to_float(derivatives.get("oi_change_pct_5m"), 0.0),
        _to_float(derivatives.get("oi_change_pct_15m"), 0.0),
        _to_float(derivatives.get("oi_change_pct_1h"), 0.0),
    )
    regime = str(derivatives.get("oi_regime") or "").strip().lower()
    if direction == "up":
        supportive_regime = regime in {"new_longs", "accumulation"}
    elif direction == "down":
        supportive_regime = regime in {"new_shorts", "deleveraging"}
    else:
        supportive_regime = False
    passed = rank >= min_rank or (supportive_regime and abs(oi_change) >= min_change)
    return ConfirmationCheck(
        key="oi",
        passed=passed,
        value={"level": derivatives.get("oi_signal_level"), "regime": regime, "change": oi_change},
        reason="oi_confirmed" if passed else "oi_not_confirmed",
    )


def _check_flow(candidate: Candidate, cfg: Dict[str, Any], direction: str) -> ConfirmationCheck:
    derivatives = candidate.derivatives or {}
    taker_buy_ratio = _to_optional_float(derivatives.get("taker_buy_ratio"))
    buy_ratio_1m = _to_optional_float(derivatives.get("buy_aggressor_ratio_1m"))
    buy_ratio_3m = _to_optional_float(derivatives.get("buy_aggressor_ratio_3m"))
    cvd_1m = _to_float(derivatives.get("cvd_usdt_1m"), 0.0)
    cvd_3m = _to_float(derivatives.get("cvd_usdt_3m"), 0.0)

    if direction == "up":
        taker_passed = taker_buy_ratio is not None and taker_buy_ratio >= _to_float(
            cfg.get("trade_confirmation_up_taker_buy_ratio"),
            0.55,
        )
        micro_passed = (
            buy_ratio_1m is not None
            and buy_ratio_1m >= _to_float(cfg.get("trade_confirmation_up_buy_aggressor_ratio"), 0.55)
            and (cvd_1m > 0 or cvd_3m > 0)
        )
    elif direction == "down":
        taker_passed = taker_buy_ratio is not None and taker_buy_ratio <= _to_float(
            cfg.get("trade_confirmation_down_taker_buy_ratio"),
            0.45,
        )
        micro_passed = (
            buy_ratio_1m is not None
            and buy_ratio_1m <= _to_float(cfg.get("trade_confirmation_down_buy_aggressor_ratio"), 0.45)
            and (cvd_1m < 0 or cvd_3m < 0)
        )
    else:
        taker_passed = False
        micro_passed = False

    passed = taker_passed or micro_passed
    return ConfirmationCheck(
        key="flow",
        passed=passed,
        value={
            "taker_buy_ratio": taker_buy_ratio,
            "buy_aggressor_ratio_1m": buy_ratio_1m,
            "buy_aggressor_ratio_3m": buy_ratio_3m,
            "cvd_usdt_1m": cvd_1m,
            "cvd_usdt_3m": cvd_3m,
        },
        reason="flow_confirmed" if passed else "flow_not_confirmed",
    )


def _check_orderbook(candidate: Candidate, cfg: Dict[str, Any], direction: str) -> ConfirmationCheck:
    orderbook = candidate.orderbook or {}
    imbalance = _to_float(orderbook.get("imbalance"), 0.0)
    spread_bps = _to_float(orderbook.get("spread_bps"), 0.0)
    bid_notional = _to_float(orderbook.get("bid_notional"), 0.0)
    ask_notional = _to_float(orderbook.get("ask_notional"), 0.0)
    total_depth = bid_notional + ask_notional
    min_depth = max(0.0, _to_float(cfg.get("trade_confirmation_min_depth_notional"), 0.0))
    max_spread = max(0.0, _to_float(cfg.get("trade_confirmation_max_spread_bps"), 12.0))
    threshold = abs(_to_float(cfg.get("trade_confirmation_orderbook_imbalance"), 0.08))

    spread_ok = spread_bps > 0 and (max_spread <= 0 or spread_bps <= max_spread)
    depth_ok = total_depth >= min_depth
    if direction == "up":
        side_ok = imbalance >= threshold
    elif direction == "down":
        side_ok = imbalance <= -threshold
    else:
        side_ok = False
    passed = bool(spread_ok and depth_ok and side_ok)
    return ConfirmationCheck(
        key="orderbook",
        passed=passed,
        value={"imbalance": imbalance, "spread_bps": spread_bps, "depth": total_depth},
        reason="orderbook_confirmed" if passed else "orderbook_not_confirmed",
    )


def _check_liquidation(candidate: Candidate, cfg: Dict[str, Any], direction: str) -> ConfirmationCheck:
    liquidation = candidate.liquidation or {}
    derivatives = candidate.derivatives or {}
    long_liq = _to_float(liquidation.get("long_liq_usdt"), 0.0) + _to_float(
        liquidation.get("micro_long_liq_usdt_1m"),
        0.0,
    )
    short_liq = _to_float(liquidation.get("short_liq_usdt"), 0.0) + _to_float(
        liquidation.get("micro_short_liq_usdt_1m"),
        0.0,
    )
    micro_imbalance = _to_float(
        liquidation.get("micro_liq_imbalance_usdt_1m"),
        _to_float(derivatives.get("micro_liq_imbalance_usdt_1m"), 0.0),
    )
    threshold = max(0.0, _to_float(cfg.get("trade_confirmation_min_liquidation_usdt"), 20_000.0))
    if direction == "up":
        passed = (short_liq - long_liq) >= threshold or micro_imbalance >= threshold
    elif direction == "down":
        passed = (long_liq - short_liq) >= threshold or micro_imbalance <= -threshold
    else:
        passed = False
    return ConfirmationCheck(
        key="liquidation",
        passed=passed,
        value={"long_liq_usdt": long_liq, "short_liq_usdt": short_liq, "micro_imbalance": micro_imbalance},
        reason="liquidation_confirmed" if passed else "liquidation_not_confirmed",
    )


def _check_position_pressure(candidate: Candidate, direction: str) -> ConfirmationCheck:
    pressure = candidate.position_pressure or {}
    pressure_direction = str(pressure.get("direction") or "").strip().lower()
    passed = bool(
        pressure.get("confirmation_enabled")
        and pressure.get("confirmation_passed")
        and pressure_direction == direction
    )
    return ConfirmationCheck(
        key="position_pressure",
        passed=passed,
        value={
            "state": pressure.get("state"),
            "driver": pressure.get("driver"),
            "confidence": pressure.get("confidence"),
            "direction": pressure_direction,
        },
        reason="position_pressure_confirmed" if passed else "position_pressure_not_confirmed",
    )


def confirmation_count(candidate: Candidate) -> int:
    confirmation = candidate.confirmation or {}
    return max(0, _to_int(confirmation.get("passed_count"), 0))


def confirmation_stage(candidate: Candidate) -> str:
    confirmation = candidate.confirmation or {}
    return str(confirmation.get("stage") or "unknown")


def _signal_rank(value: Any) -> int:
    text = str(value or "").strip().upper()
    return {"L0": 0, "L1": 1, "L2": 2, "L3": 3}.get(text, -1)


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


def _to_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
