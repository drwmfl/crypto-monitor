from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from alerts.alert_policy import AlertDecision
    from candidates.candidate_models import Candidate
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.alert_policy import AlertDecision
    from apps.market_monitor.backend.candidates.candidate_models import Candidate


ALERT_TYPE_LABELS = {
    "watchlist_alert": "观察候选",
    "actionable_alert": "高价值候选",
    "risk_alert": "高风险提醒",
}

DETAIL_LEVELS = {"compact", "full", "verbose"}
ACCUMULATION_STATUS_LABELS = {
    "ready": "放量",
    "warming": "升温",
    "dormant": "沉睡",
}

OI_REGIME_SHORT_LABELS = {
    "new_longs": "新多",
    "short_cover": "挤空",
    "new_shorts": "新空",
    "deleveraging": "去杠杆",
    "accumulation": "潜伏",
    "churn": "换手",
    "neutral": "中性",
    "unknown": "未知",
}

MICRO_REGIME_SHORT_LABELS = {
    "breakout_continuation": "延续",
    "new_longs": "新多",
    "absorption_reversal": "吸收",
    "short_cover": "挤空",
    "churn": "换手",
    "unknown": "未知",
}


def format_strategy_alert(
    candidate: Candidate,
    decision: AlertDecision,
    detail_level: str = "compact",
) -> str:
    derivatives = candidate.derivatives or {}
    accumulation = candidate.accumulation or {}
    latest = candidate.latest_features or {}
    alert_label = ALERT_TYPE_LABELS.get(decision.alert_type, "策略提醒")
    token_name = str(candidate.base_asset or "").strip().upper() or _token_name(candidate.symbol)
    trigger_count = max(1, int(candidate.event_count or 0))
    direction = _direction_label(str(latest.get("direction") or ""))
    windows = "/".join(candidate.windows) or str(latest.get("window") or "N/A")
    price = _fmt_float(latest.get("price"), digits=8)
    change = _fmt_pct(latest.get("change_pct"))

    lines = [
        f"**{alert_label} | {token_name}（触发{trigger_count}次）**",
        f"状态：OI {_oi_state_label(derivatives)} | Micro {_micro_state_label(derivatives)} | {direction} {windows}",
        f"价格：{change} | {price} | 评分/风险 {candidate.score:.1f}/{candidate.risk_score:.1f}",
        f"OI：{_oi_matrix(derivatives)}",
        f"微结构：{_micro_matrix(derivatives)}",
        f"资金：主动买 {_fmt_ratio_pct(derivatives.get('taker_buy_ratio'))} | 费率 {_fmt_ratio_pct(derivatives.get('funding_rate'))} | OI {_fmt_compact_usd(derivatives.get('oi_usdt'))}",
    ]

    accumulation_line = _accumulation_line(accumulation)
    if accumulation_line:
        lines.append(accumulation_line)

    if _normalize_detail_level(detail_level) != "compact":
        source = _source_label(str(latest.get("source") or latest.get("trigger_source") or ""))
        event_name = str(latest.get("event_type") or latest.get("rule_name") or "N/A")
        lines.append(f"事件：{event_name} | 来源 {source}")
        oi_reason = str(derivatives.get("oi_reason") or "").strip()
        micro_reason = str(derivatives.get("micro_reason") or "").strip()
        if oi_reason:
            lines.append(f"OI解释：{oi_reason}")
        if micro_reason:
            lines.append(f"微结构解释：{micro_reason}")

    lines.append(f"https://www.binance.com/futures/{candidate.symbol}")
    return "\n".join(lines)


def _token_name(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)]
    return text or "UNKNOWN"


def _oi_state_label(derivatives: Dict[str, Any]) -> str:
    level = str(derivatives.get("oi_signal_level") or "none").upper()
    regime = _oi_regime_short_label(str(derivatives.get("oi_regime") or "unknown"))
    return f"{level} {regime}"


def _micro_state_label(derivatives: Dict[str, Any]) -> str:
    level = str(derivatives.get("micro_signal_level") or "L0").upper()
    regime = _micro_regime_short_label(str(derivatives.get("micro_regime") or "unknown"))
    return f"{level} {regime}"


def _oi_regime_short_label(regime: str) -> str:
    return OI_REGIME_SHORT_LABELS.get(str(regime or "unknown"), str(regime or "unknown"))


def _micro_regime_short_label(regime: str) -> str:
    return MICRO_REGIME_SHORT_LABELS.get(str(regime or "unknown"), str(regime or "unknown"))


def _accumulation_line(accumulation: Dict[str, Any]) -> str:
    if not accumulation or not accumulation.get("in_accumulation_pool"):
        return ""
    status = _accumulation_status_label(accumulation.get("status"))
    score = _fmt_float(accumulation.get("score"), 1)
    days = _fmt_int(accumulation.get("sideways_days"))
    range_pct = _fmt_pct_plain(accumulation.get("range_pct"))
    vol_ratio = _fmt_x(accumulation.get("recent_vol_ratio_7d"))
    market_cap = _fmt_compact_usd(accumulation.get("market_cap"))
    return f"收筹：{status} {score} | 横盘 {days}天 | 区间 {range_pct} | Vol {vol_ratio} | 市值 {market_cap}"


def _accumulation_status_label(status: Any) -> str:
    key = str(status or "unknown").strip().lower()
    return ACCUMULATION_STATUS_LABELS.get(key, key)


def _oi_matrix(derivatives: Dict[str, Any]) -> str:
    if not derivatives:
        return "预热中"
    return (
        f"5m {_fmt_ratio_pct(derivatives.get('oi_change_pct_5m'))} | "
        f"15m {_fmt_ratio_pct(derivatives.get('oi_change_pct_15m'))} | "
        f"1h {_fmt_ratio_pct(derivatives.get('oi_change_pct_1h'))} | "
        f"4h {_fmt_ratio_pct(derivatives.get('oi_change_pct_4h'))}"
    )


def _micro_matrix(derivatives: Dict[str, Any]) -> str:
    if not derivatives:
        return "预热中"

    regime = str(derivatives.get("micro_regime") or "unknown")
    if regime == "absorption_reversal":
        return (
            f"CVD {_fmt_signed_usd(derivatives.get('cvd_usdt_10s'))}/"
            f"{_fmt_signed_usd(derivatives.get('cvd_usdt_30s'))}/"
            f"{_fmt_signed_usd(derivatives.get('cvd_usdt_1m'))} | "
            f"买方 {_fmt_ratio_pct(derivatives.get('buy_aggressor_ratio_10s'))}/"
            f"{_fmt_ratio_pct(derivatives.get('buy_aggressor_ratio_30s'))}/"
            f"{_fmt_ratio_pct(derivatives.get('buy_aggressor_ratio_1m'))} | "
            f"强平差 {_fmt_signed_usd(derivatives.get('micro_liq_imbalance_usdt_1m'))}"
        )

    return (
        f"CVD {_fmt_signed_usd(derivatives.get('cvd_usdt_1m'))}/"
        f"{_fmt_signed_usd(derivatives.get('cvd_usdt_3m'))}/"
        f"{_fmt_signed_usd(derivatives.get('cvd_usdt_5m'))} | "
        f"买方 {_fmt_ratio_pct(derivatives.get('buy_aggressor_ratio_1m'))}/"
        f"{_fmt_ratio_pct(derivatives.get('buy_aggressor_ratio_3m'))}/"
        f"{_fmt_ratio_pct(derivatives.get('buy_aggressor_ratio_5m'))} | "
        f"强平差 {_fmt_signed_usd(derivatives.get('micro_liq_imbalance_usdt_1m'))}"
    )


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


def _fmt_ratio_pct(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None:
        return "N/A"
    return f"{numeric * 100.0:+.2f}%"


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


def _fmt_signed_usd(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None:
        return "N/A"
    abs_value = abs(numeric)
    if abs_value >= 1_000_000:
        return f"{numeric / 1_000_000:+.2f}M"
    if abs_value >= 1_000:
        return f"{numeric / 1_000:+.1f}K"
    return f"{numeric:+.0f}"


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
