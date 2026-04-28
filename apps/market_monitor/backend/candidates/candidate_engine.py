from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from candidates.candidate_models import Candidate
    from candidates.candidate_store import CandidateStore
    from candidates.raw_event import RawEvent
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.candidates.candidate_store import CandidateStore
    from apps.market_monitor.backend.candidates.raw_event import RawEvent


class CandidateEngine:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.store = CandidateStore(self.settings)
        self._candidates = self.store.load_all()

    def update_candidate(self, event: RawEvent) -> Candidate:
        key = event.symbol
        candidate = self._candidates.get(key)
        if candidate is None or self._is_stale(candidate, event):
            candidate = Candidate.from_raw_event(event)
        else:
            candidate.apply_event(event)
        self._candidates[key] = candidate
        return candidate

    def save(self) -> None:
        self.store.save_all(self._candidates)

    def all_candidates(self) -> Dict[str, Candidate]:
        return dict(self._candidates)

    def _is_stale(self, candidate: Candidate, event: RawEvent) -> bool:
        ttl_minutes = _safe_int(self.settings.get("candidate_ttl_minutes"), 120)
        if ttl_minutes <= 0:
            return False
        last_seen = _parse_time(candidate.last_seen)
        event_time = _parse_time(event.event_time)
        if last_seen is None or event_time is None:
            return False
        return (event_time - last_seen).total_seconds() > (ttl_minutes * 60)


def _parse_time(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        text = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
