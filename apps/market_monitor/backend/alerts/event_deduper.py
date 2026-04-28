from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from candidates.candidate_models import Candidate
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


DEFAULT_EVENT_DEDUPE: Dict[str, Any] = {
    "event_dedupe_enabled": True,
    "event_dedupe_state_file": "event_dedupe_state.json",
    "event_dedupe_window_minutes": 30,
    "event_dedupe_min_abs_change_delta_pct": 3.0,
    "event_dedupe_min_relative_change_delta": 0.35,
    "event_dedupe_min_score_delta": 8.0,
    "event_dedupe_min_risk_delta": 15.0,
    "event_dedupe_factor_upgrade_enabled": True,
    "event_dedupe_window_upgrade_enabled": True,
    "event_dedupe_window_upgrade_requires_score_delta": True,
    "event_dedupe_reversal_enabled": True,
}

WINDOW_RANK = {
    "1m": 1,
    "5m": 2,
    "15m": 3,
    "30m": 4,
    "1h": 5,
}


@dataclass
class EventDedupeDecision:
    should_send: bool
    reason: str


class EventDeduper:
    """
    Suppresses repeated pushes from the same symbol move while allowing real upgrades.
    State is keyed by symbol, not by alert type, so strong-direct and strategy alerts
    share one event window.
    """

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = dict(DEFAULT_EVENT_DEDUPE)
        if settings:
            self.settings.update(settings)
        self.runtime_dir = resolve_runtime_dir(self.settings)
        self.state_path = self.runtime_dir / str(
            self.settings.get("event_dedupe_state_file", "event_dedupe_state.json")
        )
        self._state = self._load_state()

    @property
    def enabled(self) -> bool:
        return _parse_bool(self.settings.get("event_dedupe_enabled"), True)

    def decide(self, candidate: Candidate, *, alert_type: str) -> EventDedupeDecision:
        if not self.enabled:
            return EventDedupeDecision(True, "event_dedupe_disabled")

        now_ts = time.time()
        self._prune(now_ts)
        current = self._current_snapshot(candidate=candidate, alert_type=alert_type, now_ts=now_ts)
        if not current["symbol"]:
            return EventDedupeDecision(True, "event_dedupe_no_symbol")

        active = self._active_state(current["symbol"])
        if not active:
            return EventDedupeDecision(True, "new_event")

        if now_ts >= _to_float(active.get("expires_ts"), 0.0):
            return EventDedupeDecision(True, "event_window_expired")

        prev_direction = str(active.get("direction") or "")
        if current["direction"] and prev_direction and current["direction"] != prev_direction:
            if _parse_bool(self.settings.get("event_dedupe_reversal_enabled"), True):
                return EventDedupeDecision(True, "direction_reversal")

        if current["direction"] != prev_direction:
            return EventDedupeDecision(False, "event_dedupe_opposite_direction_suppressed")

        upgrade_reason = self._upgrade_reason(active=active, current=current)
        if upgrade_reason:
            return EventDedupeDecision(True, upgrade_reason)

        return EventDedupeDecision(False, "event_dedupe_no_increment")

    def mark_sent(self, candidate: Candidate, *, alert_type: str) -> None:
        if not self.enabled:
            return
        now_ts = time.time()
        self._prune(now_ts)
        current = self._current_snapshot(candidate=candidate, alert_type=alert_type, now_ts=now_ts)
        if not current["symbol"]:
            return

        window_sec = max(1, _to_int(self.settings.get("event_dedupe_window_minutes"), 30)) * 60.0
        previous = self._active_state(current["symbol"]) or {}
        same_direction = str(previous.get("direction") or "") == current["direction"]
        first_sent_ts = _to_float(previous.get("first_sent_ts"), 0.0) if same_direction else 0.0
        if first_sent_ts <= 0:
            first_sent_ts = now_ts

        active = dict(current)
        active["first_sent_ts"] = first_sent_ts
        active["last_sent_ts"] = now_ts
        active["expires_ts"] = now_ts + window_sec
        active["send_count"] = (_to_int(previous.get("send_count"), 0) + 1) if same_direction else 1
        active["max_abs_change_pct"] = max(
            _to_float(previous.get("max_abs_change_pct"), 0.0) if same_direction else 0.0,
            _to_float(current.get("abs_change_pct"), 0.0),
        )

        self._state.setdefault("symbols", {})[current["symbol"]] = {"active": active}
        self._save_state()

    def _upgrade_reason(self, *, active: Dict[str, Any], current: Dict[str, Any]) -> str:
        prev_max_change = _to_float(active.get("max_abs_change_pct"), 0.0)
        curr_change = _to_float(current.get("abs_change_pct"), 0.0)
        change_delta = curr_change - prev_max_change
        min_abs_delta = max(0.0, _to_float(self.settings.get("event_dedupe_min_abs_change_delta_pct"), 3.0))
        min_relative_delta = max(0.0, _to_float(self.settings.get("event_dedupe_min_relative_change_delta"), 0.35))
        required_delta = max(min_abs_delta, prev_max_change * min_relative_delta)
        if prev_max_change <= 0 or change_delta >= required_delta:
            return f"price_extension:{change_delta:.2f}"

        prev_score = _to_float(active.get("score"), 0.0)
        curr_score = _to_float(current.get("score"), 0.0)
        score_delta = curr_score - prev_score
        min_score_delta = max(0.0, _to_float(self.settings.get("event_dedupe_min_score_delta"), 8.0))

        if _parse_bool(self.settings.get("event_dedupe_factor_upgrade_enabled"), True):
            if score_delta >= min_score_delta:
                return f"score_upgrade:{score_delta:.1f}"

            prev_risk = _to_float(active.get("risk_score"), 0.0)
            curr_risk = _to_float(current.get("risk_score"), 0.0)
            risk_delta = curr_risk - prev_risk
            min_risk_delta = max(0.0, _to_float(self.settings.get("event_dedupe_min_risk_delta"), 15.0))
            if risk_delta >= min_risk_delta:
                return f"risk_upgrade:{risk_delta:.1f}"

            if _to_int(current.get("oi_rank"), -1) > _to_int(active.get("oi_rank"), -1):
                return "oi_upgrade"
            if _to_int(current.get("micro_rank"), -1) > _to_int(active.get("micro_rank"), -1):
                return "micro_upgrade"

        if _parse_bool(self.settings.get("event_dedupe_window_upgrade_enabled"), True):
            window_up = _to_int(current.get("window_rank"), 0) > _to_int(active.get("window_rank"), 0)
            if window_up:
                if not _parse_bool(self.settings.get("event_dedupe_window_upgrade_requires_score_delta"), True):
                    return "window_upgrade"
                if score_delta >= min_score_delta:
                    return f"window_score_upgrade:{score_delta:.1f}"

        return ""

    def _current_snapshot(self, *, candidate: Candidate, alert_type: str, now_ts: float) -> Dict[str, Any]:
        latest = candidate.latest_features or {}
        window = str(latest.get("window") or "").strip()
        direction = str(latest.get("direction") or "").strip().lower()
        derivatives = candidate.derivatives or {}
        return {
            "symbol": str(candidate.symbol or "").strip().upper(),
            "direction": direction,
            "window": window,
            "window_rank": WINDOW_RANK.get(window, 0),
            "alert_type": str(alert_type or "").strip(),
            "abs_change_pct": abs(_to_float(latest.get("change_pct"), 0.0)),
            "score": _to_float(candidate.score, 0.0),
            "risk_score": _to_float(candidate.risk_score, 0.0),
            "oi_rank": _signal_rank(derivatives.get("oi_signal_level")),
            "micro_rank": _signal_rank(derivatives.get("micro_signal_level")),
            "updated_ts": now_ts,
        }

    def _active_state(self, symbol: str) -> Dict[str, Any]:
        state = self._state.get("symbols", {}).get(str(symbol or "").upper(), {})
        active = state.get("active", {})
        return active if isinstance(active, dict) else {}

    def _prune(self, now_ts: float) -> None:
        symbols = self._state.setdefault("symbols", {})
        changed = False
        for symbol in list(symbols.keys()):
            active = symbols.get(symbol, {}).get("active", {})
            expires_ts = _to_float(active.get("expires_ts"), 0.0) if isinstance(active, dict) else 0.0
            if expires_ts > 0 and now_ts <= expires_ts:
                continue
            symbols.pop(symbol, None)
            changed = True
        if changed:
            self._save_state()

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"symbols": {}}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("symbols", {})
                return payload
        except Exception:
            pass
        return {"symbols": {}}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.state_path)


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


def _to_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
