from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

try:
    from candidates.raw_event import RawEvent, utc_now_iso
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.raw_event import RawEvent, utc_now_iso


@dataclass
class Candidate:
    candidate_id: str
    symbol: str
    base_asset: str
    status: str = "tracking"
    first_seen: str = ""
    last_seen: str = ""
    event_count: int = 0
    event_types: List[str] = field(default_factory=list)
    windows: List[str] = field(default_factory=list)
    directions: Dict[str, int] = field(default_factory=dict)
    sources: Dict[str, int] = field(default_factory=dict)
    severities: Dict[str, int] = field(default_factory=dict)
    reason_tags: List[str] = field(default_factory=list)
    latest_features: Dict[str, Any] = field(default_factory=dict)
    max_abs_change_pct: float = 0.0
    max_rvol: float = 0.0
    max_confidence: float = 0.0
    score: float = 0.0
    score_breakdown: Dict[str, Any] = field(default_factory=dict)
    risk_score: float = 0.0
    risk_level: str = "unknown"
    risk_breakdown: Dict[str, Any] = field(default_factory=dict)
    priority: str = "low"
    recent_events: List[Dict[str, Any]] = field(default_factory=list)
    derivatives: Dict[str, Any] = field(default_factory=dict)
    orderbook: Dict[str, Any] = field(default_factory=dict)
    liquidation: Dict[str, Any] = field(default_factory=dict)
    accumulation: Dict[str, Any] = field(default_factory=dict)
    factor_snapshot: Dict[str, Any] = field(default_factory=dict)
    factor_updated_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_raw_event(cls, event: RawEvent) -> "Candidate":
        now = utc_now_iso()
        candidate = cls(
            candidate_id=event.symbol,
            symbol=event.symbol,
            base_asset=event.base_asset,
            first_seen=event.event_time or now,
            last_seen=event.event_time or now,
            updated_at=now,
        )
        candidate.apply_event(event)
        return candidate

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Candidate":
        known = {field_name for field_name in cls.__dataclass_fields__.keys()}
        data = {key: value for key, value in payload.items() if key in known}
        candidate = cls(**data)
        candidate.event_types = _dedupe_str_list(candidate.event_types)
        candidate.windows = _dedupe_str_list(candidate.windows)
        candidate.reason_tags = _dedupe_str_list(candidate.reason_tags)
        candidate.recent_events = candidate.recent_events[-20:]
        return candidate

    def apply_event(self, event: RawEvent) -> None:
        self.event_count += 1
        self.last_seen = event.event_time or utc_now_iso()
        self.updated_at = utc_now_iso()
        self.status = "tracking"

        self.event_types = _append_unique(self.event_types, event.event_type)
        self.windows = _append_unique(self.windows, event.window)
        self.directions[event.direction or "unknown"] = self.directions.get(event.direction or "unknown", 0) + 1
        self.sources[event.source or "unknown"] = self.sources.get(event.source or "unknown", 0) + 1
        self.severities[event.severity or "medium"] = self.severities.get(event.severity or "medium", 0) + 1

        features = dict(event.features or {})
        features.update(
            {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "event_time": event.event_time,
                "source": event.source,
                "window": event.window,
                "direction": event.direction,
                "severity": event.severity,
            }
        )
        self.latest_features = features

        change_pct = _safe_float(features.get("change_pct"), 0.0)
        rvol = _safe_float(features.get("rvol"), 0.0)
        confidence = _safe_float(features.get("confidence"), 0.0)
        self.max_abs_change_pct = max(self.max_abs_change_pct, abs(change_pct))
        self.max_rvol = max(self.max_rvol, rvol)
        self.max_confidence = max(self.max_confidence, confidence)

        tags = [event.event_type, event.window, event.direction, event.severity]
        for reason in features.get("reasons") or []:
            tags.append(str(reason))
        for tag in tags:
            clean = str(tag or "").strip()
            if clean:
                self.reason_tags = _append_unique(self.reason_tags, clean, limit=40)

        self.recent_events.append(
            {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "window": event.window,
                "direction": event.direction,
                "source": event.source,
                "severity": event.severity,
                "event_time": event.event_time,
                "change_pct": change_pct,
                "rvol": rvol,
                "confidence": confidence,
            }
        )
        self.recent_events = self.recent_events[-20:]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _append_unique(items: List[str], value: str, limit: int = 20) -> List[str]:
    clean = str(value or "").strip()
    if not clean:
        return items
    if clean not in items:
        items.append(clean)
    return items[-limit:]


def _dedupe_str_list(items: List[Any]) -> List[str]:
    result: List[str] = []
    for item in items or []:
        clean = str(item or "").strip()
        if clean and clean not in result:
            result.append(clean)
    return result


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
