from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("/", "")


def base_asset_from_symbol(symbol: str) -> str:
    text = normalize_symbol(symbol)
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)]
    return text


@dataclass
class RawEvent:
    event_id: str
    symbol: str
    base_asset: str
    source: str
    event_type: str
    window: str
    direction: str
    severity: str
    event_time: str
    features: Dict[str, Any] = field(default_factory=dict)
    raw_message: str = ""
    created_at: str = field(default_factory=utc_now_iso)

    @classmethod
    def from_alert_payload(
        cls,
        payload: Dict[str, Any],
        *,
        source: str,
        event_time: Optional[str] = None,
    ) -> "RawEvent":
        symbol = normalize_symbol(str(payload.get("symbol", "")))
        rule_name = str(payload.get("rule_name", "unknown_rule")).strip() or "unknown_rule"
        window = str(payload.get("window", "")).strip()
        direction = str(payload.get("direction", "")).strip().lower()
        severity = str(payload.get("level", "medium")).strip().lower() or "medium"
        raw_event_time = event_time or _coerce_time(payload.get("event_time")) or utc_now_iso()

        features = {
            "rule_name": rule_name,
            "change_pct": _safe_float(payload.get("change_pct")),
            "price": _safe_float(payload.get("price")),
            "confidence": _safe_float(payload.get("confidence")),
            "confidence_band": payload.get("confidence_band"),
            "rvol": _safe_float(payload.get("rvol")),
            "change_1h_pct": _safe_float(payload.get("change_1h_pct")),
            "change_24h_pct": _safe_float(payload.get("change_24h_pct")),
            "mc": _safe_float(payload.get("mc")),
            "fdv": _safe_float(payload.get("fdv")),
            "trigger_source": payload.get("trigger_source"),
            "scan_source": payload.get("scan_source"),
            "candle_state": payload.get("candle_state"),
            "ws_gap_detected": payload.get("ws_gap_detected"),
            "ws_gap_window": payload.get("ws_gap_window"),
            "ws_gap_age_sec": _safe_float(payload.get("ws_gap_age_sec")),
            "ws_gap_reason": payload.get("ws_gap_reason"),
            "reasons": [str(item) for item in (payload.get("reasons") or [])],
        }

        return cls(
            event_id=uuid.uuid4().hex,
            symbol=symbol,
            base_asset=base_asset_from_symbol(symbol),
            source=source,
            event_type=rule_name,
            window=window,
            direction=direction,
            severity=severity if severity in {"low", "medium", "high"} else "medium",
            event_time=raw_event_time,
            features=features,
            raw_message=_build_raw_message(symbol=symbol, window=window, rule_name=rule_name, direction=direction),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "source": self.source,
            "event_type": self.event_type,
            "window": self.window,
            "direction": self.direction,
            "severity": self.severity,
            "event_time": self.event_time,
            "features": self.features,
            "raw_message": self.raw_message,
            "created_at": self.created_at,
        }


def _build_raw_message(*, symbol: str, window: str, rule_name: str, direction: str) -> str:
    return f"{symbol} {window} {direction} {rule_name}".strip()


def _coerce_time(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
