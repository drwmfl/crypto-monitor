from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from alerts.alert_policy import AlertDecision
    from candidates.candidate_models import Candidate
    from instrument_type import instrument_badge
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.alert_policy import AlertDecision
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.instrument_type import instrument_badge


ALERT_TYPE_LABELS = {
    "watchlist_alert": "观察候选",
    "actionable_alert": "高价值候选",
    "risk_alert": "高风险提醒",
    "strong_direct_alert": "急速异动",
    "startup_alert": "启动预警",
}
ALERT_TYPE_ICONS = {
    "watchlist_alert": "👀",
    "actionable_alert": "💎",
    "risk_alert": "🚨",
    "strong_direct_alert": "⚡",
    "startup_alert": "🚀",
}

DETAIL_LEVELS = {"compact", "full", "verbose"}
ACCUMULATION_STATUS_LABELS = {
    "ready": "放量",
    "warming": "升温",
    "dormant": "沉睡",
}

def format_strategy_alert(
    candidate: Candidate,
    decision: AlertDecision,
    detail_level: str = "compact",
) -> str:
    derivatives = candidate.derivatives or {}
    accumulation = candidate.accumulation or {}
    latest = candidate.latest_features or {}
    alert_label = ALERT_TYPE_LABELS.get(decision.alert_type, "观察候选")
    alert_icon = ALERT_TYPE_ICONS.get(decision.alert_type, "👀")
    token_name = str(candidate.base_asset or "").strip().upper() or _token_name(candidate.symbol)
    trigger_count = max(1, int(candidate.event_count or 0))
    direction = _direction_label(str(latest.get("direction") or ""))
    windows = "/".join(candidate.windows) or str(latest.get("window") or "N/A")
    price = _fmt_float(latest.get("price"), digits=8)
    change = _fmt_pct(latest.get("change_pct"))
    badge = instrument_badge(latest.get("instrument_type"))
    title_badge = f" | {badge}" if badge else ""

    lines = [
        f"**{alert_icon} {alert_label} | {token_name}（触发{trigger_count}次）{title_badge}**",
        _market_line(
            direction=direction,
            window=str(latest.get("window") or windows or "N/A"),
            change=change,
            rvol=_fmt_x(latest.get("rvol")),
            price=price,
            alert_type=decision.alert_type,
        ),
    ]
    oi_line = _oi_summary_line(derivatives)
    if oi_line:
        lines.append(oi_line)
    if decision.alert_type == "actionable_alert":
        confirmation_line = _actionable_confirmation_line(candidate)
        if confirmation_line:
            lines.append(confirmation_line)
    if decision.alert_type == "risk_alert":
        risk_line = _risk_reason_line(candidate)
        if risk_line:
            lines.append(risk_line)
    pressure_line = _position_pressure_line(candidate.position_pressure or {})
    if pressure_line:
        lines.append(pressure_line)

    startup_line = _startup_line(latest, decision.alert_type)
    if startup_line:
        lines.append(startup_line)

    accumulation_line = _accumulation_line(accumulation)
    if accumulation_line:
        lines.append(accumulation_line)

    if _normalize_detail_level(detail_level) != "compact":
        source = _source_label(str(latest.get("source") or latest.get("trigger_source") or ""))
        event_name = str(latest.get("event_type") or latest.get("rule_name") or "N/A")
        lines.append(f"🧾 事件：{event_name} | 来源 {source}")
        oi_reason = str(derivatives.get("oi_reason") or "").strip()
        micro_reason = str(derivatives.get("micro_reason") or "").strip()
        if oi_reason:
            lines.append(f"📊 OI解释：{oi_reason}")
        if micro_reason:
            lines.append(f"🔬 微结构解释：{micro_reason}")

    lines.append(f"🔗 https://www.binance.com/futures/{candidate.symbol}")
    return "\n".join(lines)


def _token_name(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)]
    return text or "UNKNOWN"


def _market_line(
    *,
    direction: str,
    window: str,
    change: str,
    rvol: str,
    price: str,
    alert_type: str,
) -> str:
    if alert_type == "strong_direct_alert":
        icon = "⚡"
    else:
        icon = {"上涨": "📈", "下跌": "📉"}.get(direction, "📊")
    return f"{icon} 异动：{window}{direction} {change} | RVOL {rvol} | 现价 {price}"


def _oi_summary_line(derivatives: Dict[str, Any]) -> str:
    if not _has_oi_data(derivatives):
        return ""
    regime = str(derivatives.get("oi_regime") or "unknown").strip().lower()
    if regime in {"unknown", "neutral", "none", ""}:
        return ""
    level = str(derivatives.get("oi_signal_level") or "none").strip().upper()
    strong = level in {"L2", "L3"}
    phrases = {
        "new_longs": "新多明显增仓" if strong else "新多开始增仓",
        "short_cover": "空头快速回补" if strong else "空头回补",
        "new_shorts": "新空明显增仓" if strong else "新空开始增仓",
        "deleveraging": "多头快速去杠杆" if strong else "多头去杠杆",
        "accumulation": "横盘明显增仓" if strong else "横盘增仓",
        "churn": "持仓换手，方向不明",
    }
    phrase = phrases.get(regime)
    return f"📊 OI：{phrase}" if phrase else ""


def _actionable_confirmation_line(candidate: Candidate) -> str:
    confirmation = candidate.confirmation or {}
    checks = confirmation.get("checks") if isinstance(confirmation, dict) else []
    if not isinstance(checks, list):
        return ""
    direction = str((candidate.latest_features or {}).get("direction") or "").strip().lower()
    labels: list[str] = []
    for check in checks:
        if not isinstance(check, dict) or not check.get("passed"):
            continue
        key = str(check.get("key") or "").strip()
        if key == "price_persistence":
            value = check.get("value") if isinstance(check.get("value"), dict) else {}
            windows = value.get("windows") if isinstance(value.get("windows"), list) else []
            labels.append("多周期持续触发" if len(windows) >= 2 else "走势持续触发")
        elif key == "flow":
            labels.append("主动买盘增强" if direction == "up" else "主动卖盘增强")
        elif key == "orderbook":
            labels.append("盘口买方占优" if direction == "up" else "盘口卖方占优")
        elif key == "liquidation":
            labels.append("空头强平配合" if direction == "up" else "多头强平配合")
        elif key == "position_pressure":
            labels.append("仓位压力确认")
        elif key == "oi" and not _oi_summary_line(candidate.derivatives or {}):
            labels.append("OI状态确认")
    labels = list(dict.fromkeys(labels))
    return f"✅ 确认：{' | '.join(labels[:2])}" if labels else ""


def _risk_reason_line(candidate: Candidate) -> str:
    breakdown = candidate.risk_breakdown or {}
    if not isinstance(breakdown, dict):
        return "⚠️ 风险：多项风险条件叠加"
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").strip().lower()
    window = str(latest.get("window") or "短时").strip() or "短时"
    reasons: list[tuple[float, str]] = []

    overheat = _safe_float(breakdown.get("overheat"), 0.0)
    if overheat > 0:
        move = "涨幅过热" if direction == "up" else "跌幅过大" if direction == "down" else "波动过热"
        reasons.append((overheat, f"{window}{move}"))
    component_labels = {
        "direction_conflict": "多周期方向冲突",
        "single_factor": "仅单次/单周期触发",
        "crowding_noise": "短时反复触发过多",
        "open_candle": "K线尚未收盘",
        "liquidation_risk": "短时爆仓金额较大",
        "accumulation_risk": "吸筹标的流动性或区间异常",
        "position_pressure_risk": "仓位压力风险",
    }
    for key, label in component_labels.items():
        weight = _safe_float(breakdown.get(key), 0.0)
        if weight > 0:
            reasons.append((weight, label))

    weak = _safe_float(breakdown.get("weak_confirmation"), 0.0)
    if weak > 0:
        raw = breakdown.get("raw") if isinstance(breakdown.get("raw"), dict) else {}
        label = "量能确认偏弱" if _safe_float(raw.get("rvol"), 0.0) <= 1.2 else "基础确认偏弱"
        reasons.append((weak, label))

    derivatives_risk = _safe_float(breakdown.get("derivatives_risk"), 0.0)
    if derivatives_risk > 0:
        reasons.append((derivatives_risk, _derivatives_risk_label(candidate)))
    orderbook_risk = _safe_float(breakdown.get("orderbook_risk"), 0.0)
    if orderbook_risk > 0:
        reasons.append((orderbook_risk, _orderbook_risk_label(candidate)))

    selected: list[str] = []
    for _, label in sorted(reasons, key=lambda item: item[0], reverse=True):
        if label and label not in selected:
            selected.append(label)
        if len(selected) >= 2:
            break
    return f"⚠️ 风险：{' | '.join(selected)}" if selected else "⚠️ 风险：多项风险条件叠加"


def _derivatives_risk_label(candidate: Candidate) -> str:
    derivatives = candidate.derivatives or {}
    latest = candidate.latest_features or {}
    direction = str(latest.get("direction") or "").strip().lower()
    funding_rate = abs(_safe_float(derivatives.get("funding_rate"), 0.0))
    oi_regime = str(derivatives.get("oi_regime") or "").strip().lower()
    micro_regime = str(derivatives.get("micro_regime") or "").strip().lower()
    taker_buy_ratio = _safe_float(derivatives.get("taker_buy_ratio"), 0.0)
    cvd_1m = _safe_float(derivatives.get("cvd_usdt_1m"), 0.0)
    if funding_rate >= 0.0015:
        return "资金费率过热"
    if direction == "up" and (oi_regime == "short_cover" or micro_regime == "short_cover"):
        return "上涨主要由空头回补推动"
    if direction == "up" and ((0 < taker_buy_ratio < 0.48) or cvd_1m < 0):
        return "上涨与主动买盘背离"
    if direction == "down" and oi_regime == "new_shorts":
        return "新空增仓压力"
    if direction == "down" and oi_regime == "deleveraging":
        return "多头快速去杠杆"
    if oi_regime == "churn" or micro_regime == "churn":
        return "短线换手噪音偏高"
    return "衍生品信号存在冲突"


def _orderbook_risk_label(candidate: Candidate) -> str:
    orderbook = candidate.orderbook or {}
    spread_bps = _safe_float(orderbook.get("spread_bps"), 0.0)
    bid_notional = _safe_float(orderbook.get("bid_notional"), 0.0)
    ask_notional = _safe_float(orderbook.get("ask_notional"), 0.0)
    imbalance = abs(_safe_float(orderbook.get("imbalance"), 0.0))
    if 0 < bid_notional + ask_notional < 100000:
        return "盘口流动性偏低"
    if spread_bps > 15:
        return "买卖价差过大"
    if imbalance >= 0.75:
        return "盘口挂单严重失衡"
    return "盘口条件存在异常"


def _accumulation_line(accumulation: Dict[str, Any]) -> str:
    if not accumulation or not accumulation.get("in_accumulation_pool"):
        return ""
    status = _accumulation_status_label(accumulation.get("status"))
    score = _fmt_float(accumulation.get("score"), 1)
    days = _fmt_int(accumulation.get("sideways_days"))
    range_pct = _fmt_pct_plain(accumulation.get("range_pct"))
    vol_ratio = _fmt_x(accumulation.get("recent_vol_ratio_7d"))
    market_cap = _fmt_compact_usd(accumulation.get("market_cap"))
    return f"🧲 收筹：{status} {score} | 横盘 {days}天 | 区间 {range_pct} | Vol {vol_ratio} | 市值 {market_cap}"


def _position_pressure_line(pressure: Dict[str, Any]) -> str:
    if not pressure or not pressure.get("display_enabled") or not pressure.get("data_valid"):
        return ""
    confidence = _safe_optional_float(pressure.get("confidence"))
    if confidence is None or confidence < 55.0:
        return ""
    state = str(pressure.get("state") or "unknown").strip().lower()
    if state in {"unknown", "neutral", "no_active_pressure"}:
        return ""
    raw_display_states = pressure.get("display_states")
    display_states = (
        {
            str(item).strip().lower()
            for item in raw_display_states
            if str(item).strip()
        }
        if isinstance(raw_display_states, (list, tuple, set))
        else {"active_squeeze", "exhaustion", "position_control"}
    )
    if state not in display_states or pressure.get("display_allowed") is False:
        return ""
    state_labels = {
        "position_control": "持仓方控制",
        "crowded": "仓位拥挤",
        "pre_squeeze": "挤压预热",
        "active_squeeze": "强平进行",
        "exhaustion": "强平尾声",
        "organic_continuation": "新增仓位延续",
    }
    driver_labels = {
        "position_pressure": "持仓压力",
        "short_cover": "空头回补",
        "long_liquidation": "多头强平",
        "new_positions": "新增仓位",
        "profitable_shorts": "盈利空头",
        "profitable_longs": "盈利多头",
        "crowded_shorts": "空头拥挤",
        "crowded_longs": "多头拥挤",
    }
    label = state_labels.get(state, state)
    raw_driver = str(pressure.get("driver") or "")
    driver = driver_labels.get(raw_driver, raw_driver or "未知")
    return f"⚖️ 仓位：{label} | {driver}"


def _startup_line(latest: Dict[str, Any], alert_type: str) -> str:
    if alert_type != "startup_alert":
        return ""
    window = str(latest.get("startup_window") or "").strip() or "N/A"
    change = _fmt_pct(latest.get("startup_change_pct"))
    breakout = _safe_optional_float(latest.get("startup_breakout_distance_pct"))
    if breakout is None:
        return f"🚀 启动：{window}累计 {change}"
    if breakout > 0.005:
        position = f"收盘突破60m高点 +{breakout:.2f}%"
    elif breakout >= -0.005:
        position = "收盘触及60m高点"
    elif breakout >= -0.15:
        position = f"距60m高点还差 {abs(breakout):.2f}%"
    else:
        position = f"盘中突破后回落，现低于60m高点 {abs(breakout):.2f}%"
    return f"🚀 启动：{window}累计 {change} | {position}"


def _accumulation_status_label(status: Any) -> str:
    key = str(status or "unknown").strip().lower()
    return ACCUMULATION_STATUS_LABELS.get(key, key)


def _has_oi_data(derivatives: Dict[str, Any]) -> bool:
    keys = (
        "oi_usdt",
        "oi_change_pct_5m",
        "oi_change_pct_15m",
        "oi_change_pct_1h",
        "oi_change_pct_4h",
    )
    return _has_any_value(derivatives, keys)


def _has_any_value(payload: Dict[str, Any], keys: tuple[str, ...]) -> bool:
    for key in keys:
        if _safe_optional_float(payload.get(key)) is not None:
            return True
    return False


def _direction_label(value: str) -> str:
    text = value.strip().lower()
    if text in {"up", "long", "bullish"}:
        return "上涨"
    if text in {"down", "short", "bearish"}:
        return "下跌"
    return "未知"


def _source_label(value: str) -> str:
    text = value.strip().lower()
    if text == "ws":
        return "实时WS"
    if text == "poll":
        return "轮询"
    return text or "未知"


def _fmt_pct(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None:
        return "N/A"
    return f"{numeric:+.2f}%"


def _fmt_pct_plain(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None:
        return "N/A"
    return f"{numeric:.2f}%"


def _fmt_float(value: Any, digits: int = 2) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None:
        return "N/A"
    return f"{numeric:.{digits}f}"


def _fmt_x(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None or numeric <= 0:
        return "N/A"
    return f"{numeric:.2f}x"


def _fmt_int(value: Any) -> str:
    try:
        if value is None:
            return "0"
        return str(int(float(value)))
    except (TypeError, ValueError):
        return "0"


def _fmt_compact_usd(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None or numeric <= 0:
        return "N/A"
    abs_value = abs(numeric)
    if abs_value >= 1_000_000_000:
        return f"${numeric / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${numeric / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${numeric / 1_000:.2f}K"
    return f"${numeric:.2f}"


def _normalize_detail_level(value: Any) -> str:
    text = str(value or "compact").strip().lower()
    return text if text in DETAIL_LEVELS else "compact"


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
