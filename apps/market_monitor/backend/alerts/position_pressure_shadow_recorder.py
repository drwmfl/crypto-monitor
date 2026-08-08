from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

try:
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


class PositionPressureShadowRecorder:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("position_pressure_shadow_enabled"), True)
        runtime_dir = resolve_runtime_dir(self.settings)
        self.data_path = runtime_dir / str(
            self.settings.get("position_pressure_shadow_file") or "position_pressure_shadow.jsonl"
        )
        self.summary_path = runtime_dir / str(
            self.settings.get("position_pressure_summary_file") or "position_pressure_readiness.json"
        )
        self.state_path = runtime_dir / str(
            self.settings.get("position_pressure_state_file") or "position_pressure_state.json"
        )
        self.policy_version = str(
            self.settings.get("position_pressure_policy_version") or "position-pressure-v1-shadow"
        )
        self.min_days = max(
            0.0,
            _safe_float(self.settings.get("position_pressure_review_min_days"), 7.0),
        )
        self.min_samples = max(
            1,
            _safe_int(self.settings.get("position_pressure_review_min_first_push_samples"), 100),
        )
        self.min_smart_coverage = max(
            0.0,
            min(
                100.0,
                _safe_float(
                    self.settings.get("position_pressure_review_min_smart_money_coverage_pct"),
                    50.0,
                ),
            ),
        )
        self.min_liquidation_coverage = max(
            0.0,
            min(
                100.0,
                _safe_float(
                    self.settings.get("position_pressure_review_min_liquidation_coverage_pct"),
                    90.0,
                ),
            ),
        )
        timezone_name = str(
            self.settings.get("position_pressure_review_timezone") or "Asia/Shanghai"
        ).strip()
        try:
            self.local_timezone = ZoneInfo(timezone_name)
        except Exception:
            self.local_timezone = timezone.utc
        self._lock = threading.Lock()
        self._state = self._load_state()
        if self.enabled:
            now = datetime.now(timezone.utc)
            self._state.setdefault("started_at", now.isoformat())
            self._state["updated_at"] = now.isoformat()
            self._save_state()
            _write_json_atomic(self.summary_path, self._build_summary(now))

    def record(self, row: Dict[str, Any], *, alert_sent: bool) -> Dict[str, Any]:
        if not self.enabled:
            return {}
        now = datetime.now(timezone.utc)
        payload = dict(row or {})
        payload.setdefault("created_at", now.isoformat())
        payload.setdefault("policy_version", self.policy_version)
        with self._lock:
            self.data_path.parent.mkdir(parents=True, exist_ok=True)
            with self.data_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
            self._update_state(payload, alert_sent=alert_sent, now=now)
            self._save_state()
            summary = self._build_summary(now)
            _write_json_atomic(self.summary_path, summary)
            return summary

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            return self._build_summary(datetime.now(timezone.utc))

    def _update_state(self, row: Dict[str, Any], *, alert_sent: bool, now: datetime) -> None:
        self._state["shadow_rows"] = _safe_int(self._state.get("shadow_rows"), 0) + 1
        self._state.setdefault("started_at", now.isoformat())
        self._state["updated_at"] = now.isoformat()

        pressure = row.get("position_pressure") if isinstance(row.get("position_pressure"), dict) else {}
        state = str(pressure.get("state") or "unknown")
        state_counts = self._state.setdefault("state_counts", {})
        state_counts[state] = _safe_int(state_counts.get(state), 0) + 1

        if not alert_sent:
            return
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            return
        local_day = now.astimezone(self.local_timezone).date().isoformat()
        sample_key = f"{local_day}:{symbol}"
        sample_keys = self._state.setdefault("sample_keys", [])
        if sample_key in sample_keys:
            return
        sample_keys.append(sample_key)
        self._state["first_push_samples"] = _safe_int(
            self._state.get("first_push_samples"),
            0,
        ) + 1

        alert_type = str(row.get("alert_type") or "unknown")
        alert_counts = self._state.setdefault("alert_type_counts", {})
        alert_counts[alert_type] = _safe_int(alert_counts.get(alert_type), 0) + 1

        if not pressure.get("data_valid"):
            return
        self._state["valid_first_push_samples"] = _safe_int(
            self._state.get("valid_first_push_samples"),
            0,
        ) + 1

        coverage = self._state.setdefault(
            "coverage_counts",
            {"smart_money": 0, "liquidation_v2": 0, "both": 0, "classified": 0},
        )
        smart_available = bool(pressure.get("smart_money_available"))
        liquidation_available = bool(pressure.get("liquidation_v2_available"))
        if smart_available:
            coverage["smart_money"] = _safe_int(coverage.get("smart_money"), 0) + 1
        if liquidation_available:
            coverage["liquidation_v2"] = _safe_int(coverage.get("liquidation_v2"), 0) + 1
        if smart_available and liquidation_available:
            coverage["both"] = _safe_int(coverage.get("both"), 0) + 1
        if state not in {"unknown", "neutral"}:
            coverage["classified"] = _safe_int(coverage.get("classified"), 0) + 1

    def _build_summary(self, now: datetime) -> Dict[str, Any]:
        started = _parse_time(self._state.get("started_at"))
        elapsed_days = max(0.0, (now - started).total_seconds() / 86400.0) if started else 0.0
        first_push_samples = _safe_int(self._state.get("first_push_samples"), 0)
        valid_samples = _safe_int(self._state.get("valid_first_push_samples"), 0)
        coverage_counts = self._state.get("coverage_counts")
        if not isinstance(coverage_counts, dict):
            coverage_counts = {"smart_money": 0, "liquidation_v2": 0, "both": 0, "classified": 0}
        coverage_pct = {
            key: round(_safe_int(coverage_counts.get(key), 0) / first_push_samples * 100.0, 2)
            if first_push_samples
            else 0.0
            for key in ("smart_money", "liquidation_v2", "both", "classified")
        }
        days_reached = elapsed_days >= self.min_days
        samples_reached = valid_samples >= self.min_samples
        smart_reached = coverage_pct["smart_money"] >= self.min_smart_coverage
        liquidation_reached = coverage_pct["liquidation_v2"] >= self.min_liquidation_coverage
        ready = days_reached and samples_reached and smart_reached and liquidation_reached
        return {
            "version": 1,
            "policy_version": self.policy_version,
            "started_at": self._state.get("started_at"),
            "updated_at": self._state.get("updated_at"),
            "elapsed_days": round(elapsed_days, 2),
            "shadow_rows": _safe_int(self._state.get("shadow_rows"), 0),
            "first_push_samples": first_push_samples,
            "valid_first_push_samples": valid_samples,
            "coverage_counts": {key: _safe_int(coverage_counts.get(key), 0) for key in coverage_pct},
            "coverage_pct": coverage_pct,
            "state_counts": dict(self._state.get("state_counts") or {}),
            "alert_type_counts": dict(self._state.get("alert_type_counts") or {}),
            "review_gate": {
                "min_days": self.min_days,
                "min_first_push_samples": self.min_samples,
                "min_smart_money_coverage_pct": self.min_smart_coverage,
                "min_liquidation_coverage_pct": self.min_liquidation_coverage,
                "days_reached": days_reached,
                "samples_reached": samples_reached,
                "smart_money_coverage_reached": smart_reached,
                "liquidation_coverage_reached": liquidation_reached,
                "ready": ready,
                "status": "READY_FOR_REVIEW" if ready else "COLLECTING",
            },
        }

    def _load_state(self) -> Dict[str, Any]:
        fallback = {
            "version": 1,
            "sample_keys": [],
            "first_push_samples": 0,
            "valid_first_push_samples": 0,
            "shadow_rows": 0,
            "coverage_counts": {"smart_money": 0, "liquidation_v2": 0, "both": 0, "classified": 0},
            "state_counts": {},
            "alert_type_counts": {},
        }
        if not self.state_path.exists():
            return fallback
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                had_first_push_samples = "first_push_samples" in payload
                for key, value in fallback.items():
                    payload.setdefault(key, value)
                if not had_first_push_samples:
                    payload["first_push_samples"] = len(payload.get("sample_keys") or [])
                return payload
        except Exception:
            pass
        return fallback

    def _save_state(self) -> None:
        _write_json_atomic(self.state_path, self._state)


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _parse_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default
