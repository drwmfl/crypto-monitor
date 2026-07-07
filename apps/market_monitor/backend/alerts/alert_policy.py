from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

try:
    from alerts.trade_confirmation import confirmation_count
    from candidates.candidate_models import Candidate
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.trade_confirmation import confirmation_count
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
    "watchlist_tg_enabled": False,
    "watch_cooldown_minutes": 30,
    "actionable_cooldown_minutes": 60,
    "risk_cooldown_minutes": 60,
    "startup_cooldown_minutes": 45,
    "global_max_10m": 5,
    "global_max_hour": 20,
    "require_oi_for_actionable": True,
    "min_actionable_oi_signal_level": "L2",
    "actionable_require_factor_completeness": True,
    "actionable_min_factor_completeness_pct": 50.0,
    "actionable_required_factor_groups_any": ["oi", "micro"],
    "actionable_edge_filter_enabled": True,
    "actionable_strong_score": 85.0,
    "actionable_edge_min_confirmations": 4,
    "actionable_edge_min_factor_completeness_pct": 80.0,
    "actionable_edge_required_factor_groups_all": ["oi", "micro"],
    "actionable_edge_min_micro_signal_level": "L1",
    "actionable_edge_reject_micro_regimes": ["churn"],
    "actionable_edge_required_confirmation_keys_any": ["flow", "orderbook"],
    "require_confirmation_for_actionable": True,
    "min_actionable_confirmations": 3,
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
        if alert_type == "watchlist_alert" and not _parse_bool(self.settings.get("watchlist_tg_enabled"), False):
            return AlertDecision(False, alert_type=alert_type, reason="watchlist_tg_disabled")
        if alert_type == "actionable_alert" and not self._actionable_factor_complete(candidate):
            return AlertDecision(False, alert_type=alert_type, reason="actionable_factor_incomplete")

        now_ts = time.time()
        if not self._global_rate_allowed(now_ts):
            return AlertDecision(False, alert_type=alert_type, reason="global_rate_limited")

        if not self._symbol_type_allowed(candidate.symbol, alert_type, now_ts):
            return AlertDecision(False, alert_type=alert_type, reason="symbol_cooldown")

        return AlertDecision(True, alert_type=alert_type, reason="accepted")

    def _actionable_factor_complete(self, candidate: Candidate) -> bool:
        if not _parse_bool(self.settings.get("actionable_require_factor_completeness"), True):
            return True

        completeness = candidate.factor_snapshot.get("factor_completeness") if isinstance(candidate.factor_snapshot, dict) else {}
        if not isinstance(completeness, dict):
            return False
        pct = _to_float(completeness.get("pct"), 0.0)
        min_pct = max(0.0, _to_float(self.settings.get("actionable_min_factor_completeness_pct"), 50.0))
        if pct < min_pct:
            return False

        required_any = self.settings.get("actionable_required_factor_groups_any", ["oi", "micro"])
        if isinstance(required_any, str):
            required_any = [item.strip() for item in required_any.split(",")]
        if not isinstance(required_any, list):
            required_any = ["oi", "micro"]
        groups = completeness.get("groups") if isinstance(completeness.get("groups"), dict) else {}
        return any(bool(groups.get(str(group).strip())) for group in required_any if str(group).strip())

    def mark_sent(self, symbol: str, alert_type: str) -> None:
        self._mark_sent(symbol=symbol, alert_type=alert_type, now_ts=time.time())
        self._save_state()

    def recently_sent_any(self, symbol: str, alert_types: Iterable[str], *, within_minutes: float) -> Optional[str]:
        window_sec = max(0.0, _to_float(within_minutes, 0.0) * 60.0)
        if window_sec <= 0:
            return None

        now_ts = time.time()
        last_sent = self._state.get("last_sent", {})
        if not isinstance(last_sent, dict):
            return None

        symbol_key = str(symbol or "").strip().upper()
        for alert_type in alert_types:
            type_key = str(alert_type or "").strip()
            if not type_key:
                continue
            key = f"{symbol_key}:{type_key}"
            sent_ts = _to_float(last_sent.get(key), 0.0)
            if sent_ts > 0 and now_ts - sent_ts <= window_sec:
                return type_key
        return None

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
            if _parse_bool(self.settings.get("require_confirmation_for_actionable"), True):
                required_confirmations = max(1, _to_int(self.settings.get("min_actionable_confirmations"), 3))
                if confirmation_count(candidate) < required_confirmations:
                    if score >= _to_float(self.settings.get("min_watch_score"), 50.0) and risk <= _to_float(
                        self.settings.get("max_watch_risk"),
                        60.0,
                    ):
                        return "watchlist_alert"
                    return "none"
            if not self._actionable_edge_filter_passed(candidate):
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

    def _actionable_edge_filter_passed(self, candidate: Candidate) -> bool:
        if not _parse_bool(self.settings.get("actionable_edge_filter_enabled"), True):
            return True

        score = float(candidate.score or 0.0)
        strong_score = max(
            _to_float(self.settings.get("min_actionable_score"), 75.0),
            _to_float(self.settings.get("actionable_strong_score"), 85.0),
        )
        if score >= strong_score:
            return True

        min_confirmations = max(1, _to_int(self.settings.get("actionable_edge_min_confirmations"), 4))
        if confirmation_count(candidate) < min_confirmations:
            return False

        if self._factor_completeness_pct(candidate) < _to_float(
            self.settings.get("actionable_edge_min_factor_completeness_pct"),
            80.0,
        ):
            return False

        if not self._factor_groups_all_available(
            candidate,
            self.settings.get("actionable_edge_required_factor_groups_all", ["oi", "micro"]),
        ):
            return False

        min_micro_rank = _micro_signal_rank(str(self.settings.get("actionable_edge_min_micro_signal_level") or "L1"))
        derivatives = candidate.derivatives or {}
        if _micro_signal_rank(str(derivatives.get("micro_signal_level") or "none")) < min_micro_rank:
            return False

        rejected_regimes = self._csv_or_list(self.settings.get("actionable_edge_reject_micro_regimes", ["churn"]))
        micro_regime = str(derivatives.get("micro_regime") or "").strip().lower()
        if micro_regime and micro_regime in {item.lower() for item in rejected_regimes}:
            return False

        required_any = self._csv_or_list(
            self.settings.get("actionable_edge_required_confirmation_keys_any", ["flow", "orderbook"])
        )
        if required_any and not any(self._confirmation_check_passed(candidate, key) for key in required_any):
            return False

        return True

    def _factor_completeness_pct(self, candidate: Candidate) -> float:
        completeness = candidate.factor_snapshot.get("factor_completeness") if isinstance(candidate.factor_snapshot, dict) else {}
        if not isinstance(completeness, dict):
            return 0.0
        return max(0.0, min(100.0, _to_float(completeness.get("pct"), 0.0)))

    def _factor_groups_all_available(self, candidate: Candidate, raw_groups: object) -> bool:
        required_groups = self._csv_or_list(raw_groups)
        if not required_groups:
            return True

        completeness = candidate.factor_snapshot.get("factor_completeness") if isinstance(candidate.factor_snapshot, dict) else {}
        groups = completeness.get("groups") if isinstance(completeness, dict) and isinstance(completeness.get("groups"), dict) else {}
        return all(bool(groups.get(group)) for group in required_groups)

    def _confirmation_check_passed(self, candidate: Candidate, key: str) -> bool:
        confirmation = candidate.confirmation or {}
        checks = confirmation.get("checks") if isinstance(confirmation, dict) else []
        if not isinstance(checks, list):
            return False
        target = str(key or "").strip()
        for check in checks:
            if not isinstance(check, dict):
                continue
            if str(check.get("key") or "").strip() == target and bool(check.get("passed")):
                return True
        return False

    def _csv_or_list(self, value: object) -> list[str]:
        if isinstance(value, str):
            raw_items = value.split(",")
        elif isinstance(value, list):
            raw_items = value
        else:
            raw_items = []
        return [str(item).strip() for item in raw_items if str(item).strip()]

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
        if alert_type == "startup_alert":
            return max(1, _to_int(self.settings.get("startup_cooldown_minutes"), 45))
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


def _micro_signal_rank(level: str) -> int:
    text = str(level or "").strip().upper()
    return {"L0": 0, "L1": 1, "L2": 2, "L3": 3}.get(text, -1)
