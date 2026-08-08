from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

try:
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


LiquidationTuple = Tuple[int, float, float]


class LiquidationHistoryStore:
    """Persist the recent all-market liquidation stream without affecting decisions."""

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        runtime_dir = resolve_runtime_dir(self.settings)
        self.state_path = runtime_dir / str(
            self.settings.get("state_file") or "liquidation_v2_state.json"
        )
        self.event_path = runtime_dir / str(
            self.settings.get("event_file") or "liquidation_v2_events.jsonl"
        )
        self.retention_sec = max(3600, _safe_int(self.settings.get("retention_sec"), 7200))
        self.max_events_per_symbol = max(
            100,
            _safe_int(self.settings.get("max_events_per_symbol"), 2000),
        )
        self.save_interval_sec = max(
            1.0,
            _safe_float(self.settings.get("save_interval_sec"), 5.0),
        )
        self.restore_tail_bytes = max(
            64 * 1024,
            _safe_int(self.settings.get("restore_tail_bytes"), 8 * 1024 * 1024),
        )
        self._events: Dict[str, Deque[Dict[str, Any]]] = {}
        self._dedupe_keys: Dict[str, set[str]] = {}
        self._last_save_ts = 0.0
        self._lock = threading.RLock()
        self._load()

    def add_event(
        self,
        *,
        symbol: str,
        event_ts_ms: int,
        short_liq_usdt: float,
        long_liq_usdt: float,
        side: str,
        price: float,
        qty: float,
        source: str = "binance_all_force_order_ws",
    ) -> bool:
        if not self.enabled:
            return False
        symbol_key = str(symbol or "").upper().strip()
        event_ts_ms = _safe_int(event_ts_ms, 0)
        short_value = max(0.0, _safe_float(short_liq_usdt, 0.0))
        long_value = max(0.0, _safe_float(long_liq_usdt, 0.0))
        if not symbol_key or event_ts_ms <= 0 or short_value + long_value <= 0:
            return False

        row = {
            "symbol": symbol_key,
            "event_ts_ms": event_ts_ms,
            "short_liq_usdt": short_value,
            "long_liq_usdt": long_value,
            "side": str(side or "").upper().strip(),
            "price": max(0.0, _safe_float(price, 0.0)),
            "qty": max(0.0, _safe_float(qty, 0.0)),
            "source": str(source or "binance_all_force_order_ws"),
            "recorded_at_ms": int(time.time() * 1000),
        }
        key = _event_key(row)
        with self._lock:
            self._prune_locked(now_ms=max(event_ts_ms, int(time.time() * 1000)))
            keys = self._dedupe_keys.setdefault(symbol_key, set())
            if key in keys:
                return False
            buffer = self._events.setdefault(
                symbol_key,
                deque(maxlen=self.max_events_per_symbol),
            )
            if len(buffer) == buffer.maxlen and buffer:
                keys.discard(_event_key(buffer[0]))
            buffer.append(row)
            keys.add(key)
            self._append_event_locked(row)
            self._save_locked(force=False)
        return True

    def events(self, symbol: str, *, now_ms: Optional[int] = None) -> List[LiquidationTuple]:
        symbol_key = str(symbol or "").upper().strip()
        if not symbol_key:
            return []
        with self._lock:
            self._prune_locked(now_ms=now_ms or int(time.time() * 1000))
            return [
                (
                    _safe_int(row.get("event_ts_ms"), 0),
                    _safe_float(row.get("short_liq_usdt"), 0.0),
                    _safe_float(row.get("long_liq_usdt"), 0.0),
                )
                for row in self._events.get(symbol_key, ())
            ]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            self._prune_locked(now_ms=int(time.time() * 1000))
            latest_ts = max(
                (
                    _safe_int(row.get("event_ts_ms"), 0)
                    for rows in self._events.values()
                    for row in rows
                ),
                default=0,
            )
            return {
                "enabled": self.enabled,
                "symbol_count": len(self._events),
                "event_count": sum(len(rows) for rows in self._events.values()),
                "latest_event_ts_ms": latest_ts,
                "state_file": str(self.state_path),
                "event_file": str(self.event_path),
            }

    def save(self, *, force: bool = True) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._save_locked(force=force)

    def _append_event_locked(self, row: Dict[str, Any]) -> None:
        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        with self.event_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

    def _prune_locked(self, *, now_ms: int) -> None:
        cutoff_ms = int(now_ms) - self.retention_sec * 1000
        stale_symbols: List[str] = []
        for symbol, rows in self._events.items():
            keys = self._dedupe_keys.setdefault(symbol, set())
            while rows and _safe_int(rows[0].get("event_ts_ms"), 0) < cutoff_ms:
                keys.discard(_event_key(rows.popleft()))
            if not rows:
                stale_symbols.append(symbol)
        for symbol in stale_symbols:
            self._events.pop(symbol, None)
            self._dedupe_keys.pop(symbol, None)

    def _load(self) -> None:
        if not self.enabled:
            return
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - self.retention_sec * 1000
        if self.state_path.exists():
            try:
                payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception:
                payload = None
            symbols = payload.get("symbols") if isinstance(payload, dict) else None
            if isinstance(symbols, dict):
                for raw_rows in symbols.values():
                    if not isinstance(raw_rows, list):
                        continue
                    for raw in raw_rows[-self.max_events_per_symbol :]:
                        self._restore_row(raw, cutoff_ms=cutoff_ms)
        for raw in self._read_event_tail():
            self._restore_row(raw, cutoff_ms=cutoff_ms)
        for symbol, rows in list(self._events.items()):
            ordered = sorted(rows, key=lambda row: _safe_int(row.get("event_ts_ms"), 0))
            if len(ordered) > self.max_events_per_symbol:
                ordered = ordered[-self.max_events_per_symbol :]
            self._events[symbol] = deque(ordered, maxlen=self.max_events_per_symbol)
            self._dedupe_keys[symbol] = {_event_key(row) for row in ordered}

    def _read_event_tail(self) -> Iterable[Dict[str, Any]]:
        if not self.event_path.exists():
            return []
        try:
            with self.event_path.open("rb") as handle:
                handle.seek(0, 2)
                size = handle.tell()
                start = max(0, size - self.restore_tail_bytes)
                handle.seek(start)
                if start > 0:
                    handle.readline()
                raw_lines = handle.readlines()
        except OSError:
            return []
        rows: List[Dict[str, Any]] = []
        for raw_line in raw_lines:
            try:
                value = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                rows.append(value)
        return rows

    def _restore_row(self, raw: Any, *, cutoff_ms: int) -> None:
        if not isinstance(raw, dict):
            return
        symbol_key = str(raw.get("symbol") or "").upper().strip()
        event_ts_ms = _safe_int(raw.get("event_ts_ms"), 0)
        if not symbol_key or event_ts_ms < cutoff_ms:
            return
        row = dict(raw)
        row["symbol"] = symbol_key
        key = _event_key(row)
        keys = self._dedupe_keys.setdefault(symbol_key, set())
        if key in keys:
            return
        rows = self._events.setdefault(
            symbol_key,
            deque(maxlen=self.max_events_per_symbol),
        )
        if len(rows) == rows.maxlen and rows:
            keys.discard(_event_key(rows[0]))
        rows.append(row)
        keys.add(key)

    def _save_locked(self, *, force: bool) -> None:
        now = time.time()
        if not force and now - self._last_save_ts < self.save_interval_sec:
            return
        self._last_save_ts = now
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "updated_at_ms": int(now * 1000),
            "retention_sec": self.retention_sec,
            "symbols": {
                symbol: list(rows)
                for symbol, rows in self._events.items()
                if rows
            },
        }
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(self.state_path)


def _event_key(row: Dict[str, Any]) -> str:
    return ":".join(
        (
            str(row.get("symbol") or ""),
            str(_safe_int(row.get("event_ts_ms"), 0)),
            str(row.get("side") or ""),
            f"{_safe_float(row.get('price'), 0.0):.12g}",
            f"{_safe_float(row.get('qty'), 0.0):.12g}",
        )
    )


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
