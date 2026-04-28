from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    from candidates.candidate_models import Candidate
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


DEFAULT_ALERT_POLICY: Dict[str, Any] = {
    "enabled": True,
    "direct_tg_enabled": False,
    "min_watch_score": 50.0,
    "min_actionable_score": 75.0,
    "max_watch_risk": 60.0,
    "max_actionable_risk": 45.0,
    "risk_alert_score": 70.0,
    "watch_cooldown_minutes": 30,
    "actionable_cooldown_minutes": 60,
    "risk_cooldown_minutes": 60,
    "global_max_10m": 5,
    "global_max_hour": 20,
    "require_oi_for_actionable": True,
    "min_actionable_oi_signal_level": "L2",
}


@dataclass
class AlertDecision:
    should_send: bool
    alert_type: str = "none"
    reason: str = ""


class AlertPolicy:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = dict(DEFAULT_ALERT_POLICY)
        if settings:
            self.settings.update(settings)
        self.runtime_dir = resolve_runtime_dir(self.settings)
        self.state_path = self.runtime_dir / str(self.settings.get("policy_state_file", "alert_policy_state.json"))
        self._state = self._load_state()

    @property
    def enabled(self) -> bool:
        return _parse_bool(self.settings.get("enabled"), True)

    @property
    def direct_tg_enabled(self) -> bool:
        return _parse_bool(self.settings.get("direct_tg_enabled"), False)

    def decide(self, candidate: Candidate) -> AlertDecision:
        if not self.enabled:
            return AlertDecision(False, reason="policy_disabled")

        alert_type = self._classify(candidate)
        if alert_type == "none":
            return AlertDecision(False, reason="below_threshold")

        now_ts = time.time()
        if not self._global_rate_allowed(now_ts):
            return AlertDecision(False, alert_type=alert_type, reason="global_rate_limited")

        if not self._symbol_type_allowed(candidate.symbol, alert_type, now_ts):
            return AlertDecision(False, alert_type=alert_type, reason="symbol_cooldown")

        return AlertDecision(True, alert_type=alert_type, reason="accepted")

    def mark_sent(self, symbol: str, alert_type: str) -> None:
        self._mark_sent(symbol=symbol, alert_type=alert_type, now_ts=time.time())
        self._save_state()

    def allow_alert_type(
        self,
        symbol: str,
        alert_type: str,
        *,
        cooldown_minutes: Optional[int] = None,
    ) -> AlertDecision:
        if not self.enabled:
            return AlertDecision(False, alert_type=alert_type, reason="policy_disabled")

        now_ts = time.time()
        if not self._global_rate_allowed(now_ts):
            return AlertDecision(False, alert_type=alert_type, reason="global_rate_limited")

        if not self._symbol_type_allowed(symbol, alert_type, now_ts, cooldown_minutes=cooldown_minutes):
            return AlertDecision(False, alert_type=alert_type, reason="symbol_cooldown")

        return AlertDecision(True, alert_type=alert_type, reason="accepted")

    def _classify(self, candidate: Candidate) -> str:
        score = float(candidate.score or 0.0)
        risk = float(candidate.risk_score or 0.0)
        if score >= _to_float(self.settings.get("min_watch_score"), 50.0) and risk >= _to_float(
            self.settings.get("risk_alert_score"),
            70.0,
        ):
            return "risk_alert"
        if score >= _to_float(self.settings.get("min_actionable_score"), 75.0) and risk <= _to_float(
            self.settings.get("max_actionable_risk"),
            45.0,
        ):
            if _parse_bool(self.settings.get("require_oi_for_actionable"), True):
                required_rank = _oi_signal_rank(str(self.settings.get("min_actionable_oi_signal_level") or "L2"))
                if _candidate_oi_signal_rank(candidate) < required_rank:
                    if score >= _to_float(self.settings.get("min_watch_score"), 50.0) and risk <= _to_float(
                        self.settings.get("max_watch_risk"),
                        60.0,
                    ):
                        return "watchlist_alert"
                    return "none"
            return "actionable_alert"
        if score >= _to_float(self.settings.get("min_watch_score"), 50.0) and risk <= _to_float(
            self.settings.get("max_watch_risk"),
            60.0,
        ):
            return "watchlist_alert"
        return "none"

    def _global_rate_allowed(self, now_ts: float) -> bool:
        sent = [
            _to_float(item, 0.0)
            for item in self._state.get("global_sent_ts", [])
            if _to_float(item, 0.0) > now_ts - 3600.0
        ]
        self._state["global_sent_ts"] = sent
        max_hour = max(1, _to_int(self.settings.get("global_max_hour"), 20))
        max_10m = max(1, _to_int(self.settings.get("global_max_10m"), 5))
        if len(sent) >= max_hour:
            return False
        if len([ts for ts in sent if ts > now_ts - 600.0]) >= max_10m:
            return False
        return True

    def _symbol_type_allowed(
        self,
        symbol: str,
        alert_type: str,
        now_ts: float,
        *,
        cooldown_minutes: Optional[int] = None,
    ) -> bool:
        cooldown_minutes = cooldown_minutes if cooldown_minutes is not None else self._cooldown_minutes(alert_type)
        key = f"{symbol.upper()}:{alert_type}"
        last_sent = _to_float(self._state.get("last_sent", {}).get(key), 0.0)
        return (now_ts - last_sent) >= (cooldown_minutes * 60.0)

    def _cooldown_minutes(self, alert_type: str) -> int:
        if alert_type == "actionable_alert":
            return max(1, _to_int(self.settings.get("actionable_cooldown_minutes"), 60))
        if alert_type == "risk_alert":
            return max(1, _to_int(self.settings.get("risk_cooldown_minutes"), 60))
        return max(1, _to_int(self.settings.get("watch_cooldown_minutes"), 30))

    def _mark_sent(self, symbol: str, alert_type: str, now_ts: float) -> None:
        self._state.setdefault("last_sent", {})[f"{symbol.upper()}:{alert_type}"] = now_ts
        self._state.setdefault("global_sent_ts", []).append(now_ts)

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"last_sent": {}, "global_sent_ts": []}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("last_sent", {})
                payload.setdefault("global_sent_ts", [])
                return payload
        except Exception:
            pass
        return {"last_sent": {}, "global_sent_ts": []}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.state_path)


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


def _candidate_oi_signal_rank(candidate: Candidate) -> int:
    derivatives = candidate.derivatives or {}
    return _oi_signal_rank(str(derivatives.get("oi_signal_level") or "none"))


def _oi_signal_rank(level: str) -> int:
    text = str(level or "").strip().upper()
    return {"L0": 0, "L1": 1, "L2": 2, "L3": 3}.get(text, -1)
