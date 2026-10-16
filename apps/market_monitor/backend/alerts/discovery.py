from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir

logger = logging.getLogger(__name__)

DEFAULT_DISCOVERY: Dict[str, Any] = {
    "enabled": False,
    "mode": "shadow",
    "t1_change_pct": 4.0,
    "t1_min_rvol": 1.2,
    "t2_runup_pct": 8.0,
    "thread_hours": 4.0,
    "upgrade_pct": 15.0,
    "daily_cap": 3,
    "directions": ["up", "down"],
    "timezone_offset_hours": 8,
    "shadow_file": "discovery_shadow.jsonl",
    "state_file": "discovery_state.json",
}


class DiscoveryShadow:
    """Two triggers plus a per-symbol move thread, recorded instead of pushed.

    T1 fast spike : the current 1m candle has moved >= t1_change_pct with rvol >= t1_min_rvol.
    T2 steady run : price is >= t2_runup_pct away from the recent 1m extreme (low for up, high for down).
    Thread        : the first trigger opens a thread; inside thread_hours only a further move of
                    upgrade_pct beyond the last alert price records again; daily_cap per symbol and direction.
    """

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = dict(DEFAULT_DISCOVERY)
        if settings:
            self.settings.update(settings)
        self.enabled = _parse_bool(self.settings.get("enabled"), False)
        self.t1_change_pct = max(0.0, _to_float(self.settings.get("t1_change_pct"), 4.0))
        self.t1_min_rvol = max(0.0, _to_float(self.settings.get("t1_min_rvol"), 1.2))
        self.t2_runup_pct = max(0.0, _to_float(self.settings.get("t2_runup_pct"), 8.0))
        self.thread_ms = int(max(0.0, _to_float(self.settings.get("thread_hours"), 4.0)) * 3600 * 1000)
        self.upgrade_pct = max(0.0, _to_float(self.settings.get("upgrade_pct"), 15.0))
        self.daily_cap = max(1, int(_to_float(self.settings.get("daily_cap"), 3)))
        directions = self.settings.get("directions") or ["up", "down"]
        if isinstance(directions, str):
            directions = [item.strip() for item in directions.split(",")]
        self.directions = {str(item).strip().lower() for item in directions if str(item).strip().lower() in {"up", "down"}}
        self.tz = timezone(timedelta(hours=_to_float(self.settings.get("timezone_offset_hours"), 8.0)))
        self.runtime_dir: Optional[Path] = None
        self.shadow_path: Optional[Path] = None
        self.state_path: Optional[Path] = None
        if self.enabled:
            self.runtime_dir = resolve_runtime_dir(self.settings)
            self.shadow_path = self.runtime_dir / str(self.settings.get("shadow_file") or "discovery_shadow.jsonl")
            self.state_path = self.runtime_dir / str(self.settings.get("state_file") or "discovery_state.json")
        self._threads: Dict[str, Dict[str, float]] = {}
        self._day_counts: Dict[str, int] = {}
        self._load_state()
        if self.enabled and str(self.settings.get("mode") or "shadow").strip().lower() != "shadow":
            logger.warning("Discovery mode %r is not implemented yet; recording in shadow mode only.", self.settings.get("mode"))

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "DiscoveryShadow":
        raw = config.get("discovery", {}) if isinstance(config, dict) else {}
        return cls(raw if isinstance(raw, dict) else {})

    def observe(
        self,
        *,
        symbol: str,
        now_ms: int,
        price: float,
        change_1m_pct: Optional[float],
        rvol: Optional[float],
        runup_up_pct: Optional[float],
        runup_down_pct: Optional[float],
        runup_minutes: int,
    ) -> List[Dict[str, Any]]:
        if not self.enabled or price <= 0:
            return []
        recorded: List[Dict[str, Any]] = []
        for direction in ("up", "down"):
            if direction not in self.directions:
                continue
            triggers = self._triggers(
                direction,
                change_1m_pct=change_1m_pct,
                rvol=rvol,
                runup_pct=runup_up_pct if direction == "up" else runup_down_pct,
            )
            if not triggers:
                continue
            kind = self._thread_decision(symbol, direction, now_ms, price)
            if kind is None:
                continue
            row = {
                "created_at": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
                "symbol": symbol,
                "direction": direction,
                "triggers": triggers,
                "kind": kind,
                "price": price,
                "change_1m_pct": _round(change_1m_pct),
                "rvol": _round(rvol),
                "runup_pct": _round(runup_up_pct if direction == "up" else runup_down_pct),
                "runup_minutes": runup_minutes,
                "day_count": self._day_counts.get(self._day_key(symbol, direction, now_ms), 0),
                "mode": "shadow",
            }
            recorded.append(row)
            logger.info(
                "Discovery shadow alert: symbol=%s dir=%s triggers=%s kind=%s price=%s change_1m=%s runup=%s/%sm",
                symbol,
                direction,
                "+".join(triggers),
                kind,
                price,
                row["change_1m_pct"],
                row["runup_pct"],
                runup_minutes,
            )
        if recorded:
            self._append(recorded)
            self._save_state(now_ms)
        return recorded

    def _triggers(
        self,
        direction: str,
        *,
        change_1m_pct: Optional[float],
        rvol: Optional[float],
        runup_pct: Optional[float],
    ) -> List[str]:
        sign = 1.0 if direction == "up" else -1.0
        triggers: List[str] = []
        if (
            change_1m_pct is not None
            and sign * change_1m_pct >= self.t1_change_pct
            and (self.t1_min_rvol <= 0 or (rvol is not None and rvol >= self.t1_min_rvol))
        ):
            triggers.append("T1")
        if runup_pct is not None and sign * runup_pct >= self.t2_runup_pct:
            triggers.append("T2")
        return triggers

    def _thread_decision(self, symbol: str, direction: str, now_ms: int, price: float) -> Optional[str]:
        day_key = self._day_key(symbol, direction, now_ms)
        if self._day_counts.get(day_key, 0) >= self.daily_cap:
            return None
        thread_key = f"{symbol}:{direction}"
        thread = self._threads.get(thread_key)
        if thread and now_ms - thread["ts"] < self.thread_ms:
            last = thread["price"]
            threshold = last * (1 + self.upgrade_pct / 100) if direction == "up" else last * (1 - self.upgrade_pct / 100)
            extended = price >= threshold if direction == "up" else price <= threshold
            if not extended:
                return None
            kind = "upgrade"
        else:
            kind = "new"
        self._threads[thread_key] = {"ts": now_ms, "price": price}
        self._day_counts[day_key] = self._day_counts.get(day_key, 0) + 1
        return kind

    def _day_key(self, symbol: str, direction: str, now_ms: int) -> str:
        day = datetime.fromtimestamp(now_ms / 1000, self.tz).date().isoformat()
        return f"{day}:{symbol}:{direction}"

    def _append(self, rows: List[Dict[str, Any]]) -> None:
        if self.shadow_path is None:
            return
        try:
            with self.shadow_path.open("a", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.warning("Discovery shadow write failed: %s", exc)

    def _load_state(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Discovery state load failed, starting empty: %s", exc)
            return
        threads = payload.get("threads") if isinstance(payload, dict) else None
        counts = payload.get("day_counts") if isinstance(payload, dict) else None
        if isinstance(threads, dict):
            self._threads = {
                str(k): {"ts": float(v.get("ts", 0)), "price": float(v.get("price", 0))}
                for k, v in threads.items()
                if isinstance(v, dict)
            }
        if isinstance(counts, dict):
            self._day_counts = {str(k): int(v) for k, v in counts.items()}

    def _save_state(self, now_ms: int) -> None:
        if self.state_path is None:
            return
        today = datetime.fromtimestamp(now_ms / 1000, self.tz).date().isoformat()
        self._threads = {k: v for k, v in self._threads.items() if now_ms - v["ts"] < self.thread_ms}
        self._day_counts = {k: v for k, v in self._day_counts.items() if k.startswith(today)}
        try:
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps({"saved_at": time.time(), "threads": self._threads, "day_counts": self._day_counts}),
                encoding="utf-8",
            )
            tmp.replace(self.state_path)
        except Exception as exc:
            logger.warning("Discovery state save failed: %s", exc)


def _round(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(float(value), 4)


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
