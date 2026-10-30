from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
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
        self.save_interval_sec = max(0.0, _safe_float(self.settings.get("candidate_save_interval_sec"), 60.0))
        self.retention_hours = max(0.0, _safe_float(self.settings.get("candidate_retention_hours"), 24.0))
        self._last_save_ts: Optional[float] = None

    def update_candidate(self, event: RawEvent) -> Candidate:
        key = event.symbol
        candidate = self._candidates.get(key)
        if candidate is None or self._is_stale(candidate, event):
            candidate = Candidate.from_raw_event(event)
        else:
            candidate.apply_event(event)
        self._candidates[key] = candidate
        return candidate

    def save(self, force: bool = False) -> None:
        # The file is only a warm-start cache. Rewriting it on every event blocked the event loop for
        # seconds and starved the realtime websocket, so writes are debounced.
        now = time.monotonic()
        if (
            not force
            and self._last_save_ts is not None
            and self.save_interval_sec > 0
            and now - self._last_save_ts < self.save_interval_sec
        ):
            return
        self._prune_expired()
        self.store.save_all(self._candidates)
        self._last_save_ts = now

    def _prune_expired(self) -> None:
        if self.retention_hours <= 0:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        for key, candidate in list(self._candidates.items()):
            last_seen = _parse_time(candidate.last_seen)
            if last_seen is not None and last_seen < cutoff:
                del self._candidates[key]

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


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
