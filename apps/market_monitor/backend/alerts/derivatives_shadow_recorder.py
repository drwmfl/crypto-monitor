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


class DerivativesShadowRecorder:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("derivatives_shadow_enabled"), True)
        runtime_dir = resolve_runtime_dir(self.settings)
        self.data_path = runtime_dir / str(
            self.settings.get("derivatives_shadow_file") or "derivatives_factor_shadow.jsonl"
        )
        self.summary_path = runtime_dir / str(
            self.settings.get("derivatives_shadow_summary_file") or "derivatives_shadow_readiness.json"
        )
        self.state_path = runtime_dir / str(
            self.settings.get("derivatives_shadow_state_file") or "derivatives_shadow_state.json"
        )
        self.policy_version = str(
            self.settings.get("derivatives_shadow_policy_version") or "derivatives-v1-shadow"
        )
        self.min_days = max(0.0, _safe_float(self.settings.get("derivatives_shadow_review_min_days"), 7.0))
        self.min_samples = max(
            1,
            _safe_int(self.settings.get("derivatives_shadow_review_min_first_push_samples"), 100),
        )
        timezone_name = str(
            self.settings.get("derivatives_shadow_review_timezone") or "Asia/Shanghai"
        ).strip()
        try:
            self.local_timezone = ZoneInfo(timezone_name)
        except Exception:
            self.local_timezone = timezone.utc
        self._lock = threading.Lock()
        self._state = self._load_state()
        if self.enabled:
            now = datetime.now(timezone.utc)
            if not self._state.get("started_at"):
                self._state["started_at"] = now.isoformat()
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
        if not self._state.get("started_at"):
            self._state["started_at"] = now.isoformat()
        self._state["updated_at"] = now.isoformat()
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
        self._state["valid_first_push_samples"] = _safe_int(
            self._state.get("valid_first_push_samples"),
            0,
        ) + 1

        coverage = self._state.setdefault("coverage_counts", {"basis": 0, "oi": 0, "funding": 0})
        factors = row.get("factor_state") if isinstance(row.get("factor_state"), dict) else {}
        if _has_number(factors.get("basis_bps_now")):
            coverage["basis"] = _safe_int(coverage.get("basis"), 0) + 1
        if _has_number(factors.get("oi_amount_change_pct_5m")) or _has_number(
            factors.get("oi_amount_change_pct_15m")
        ):
            coverage["oi"] = _safe_int(coverage.get("oi"), 0) + 1
        if str(factors.get("funding_shadow_status") or "") == "ready":
            coverage["funding"] = _safe_int(coverage.get("funding"), 0) + 1

    def _build_summary(self, now: datetime) -> Dict[str, Any]:
        started = _parse_time(self._state.get("started_at"))
        elapsed_days = max(0.0, (now - started).total_seconds() / 86400.0) if started else 0.0
        samples = _safe_int(self._state.get("valid_first_push_samples"), 0)
        coverage_counts = self._state.get("coverage_counts")
        if not isinstance(coverage_counts, dict):
            coverage_counts = {"basis": 0, "oi": 0, "funding": 0}
        coverage_pct = {
            key: round(_safe_int(coverage_counts.get(key), 0) / samples * 100.0, 2) if samples else 0.0
            for key in ("basis", "oi", "funding")
        }
        ready = elapsed_days >= self.min_days and samples >= self.min_samples
        return {
            "version": 1,
            "policy_version": self.policy_version,
            "started_at": self._state.get("started_at"),
            "updated_at": self._state.get("updated_at"),
            "elapsed_days": round(elapsed_days, 2),
            "shadow_rows": _safe_int(self._state.get("shadow_rows"), 0),
            "valid_first_push_samples": samples,
            "coverage_counts": {key: _safe_int(coverage_counts.get(key), 0) for key in coverage_pct},
            "coverage_pct": coverage_pct,
            "review_gate": {
                "min_days": self.min_days,
                "min_first_push_samples": self.min_samples,
                "days_reached": elapsed_days >= self.min_days,
                "samples_reached": samples >= self.min_samples,
                "ready": ready,
                "status": "READY_FOR_REVIEW" if ready else "COLLECTING",
            },
        }

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {
                "version": 1,
                "sample_keys": [],
                "valid_first_push_samples": 0,
                "shadow_rows": 0,
                "coverage_counts": {"basis": 0, "oi": 0, "funding": 0},
            }
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("version", 1)
                payload.setdefault("sample_keys", [])
                payload.setdefault("valid_first_push_samples", len(payload["sample_keys"]))
                payload.setdefault("shadow_rows", 0)
                payload.setdefault("coverage_counts", {"basis": 0, "oi": 0, "funding": 0})
                return payload
        except Exception:
            pass
        return {
            "version": 1,
            "sample_keys": [],
            "valid_first_push_samples": 0,
            "shadow_rows": 0,
            "coverage_counts": {"basis": 0, "oi": 0, "funding": 0},
        }

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


def _has_number(value: Any) -> bool:
    try:
        float(value)
        return value is not None
    except (TypeError, ValueError):
        return False


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
