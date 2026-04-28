from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from candidates.raw_event import RawEvent
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.raw_event import RawEvent
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


class RawEventStore:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.runtime_dir = resolve_runtime_dir(settings)
        self.path = self.runtime_dir / str((settings or {}).get("raw_event_file", "raw_events.jsonl"))
        self._lock = threading.Lock()

    def append(self, event: RawEvent) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(event.to_dict(), ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line)

    def tail(self, limit: int = 100) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows = self.path.read_text(encoding="utf-8").splitlines()[-max(1, int(limit)) :]
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                result.append(parsed)
        return result
