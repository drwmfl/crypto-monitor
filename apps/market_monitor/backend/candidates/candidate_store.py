from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from candidates.candidate_models import Candidate
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


class CandidateStore:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.runtime_dir = resolve_runtime_dir(settings)
        self.path = self.runtime_dir / str((settings or {}).get("candidate_file", "candidates.json"))

    def load_all(self) -> Dict[str, Candidate]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        rows = payload.get("candidates", payload) if isinstance(payload, dict) else {}
        if not isinstance(rows, dict):
            return {}

        candidates: Dict[str, Candidate] = {}
        for key, value in rows.items():
            if not isinstance(value, dict):
                continue
            candidate = Candidate.from_dict(value)
            candidates[str(key or candidate.candidate_id)] = candidate
        return candidates

    def save_all(self, candidates: Dict[str, Candidate]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "candidates": {
                key: candidate.to_dict()
                for key, candidate in sorted(candidates.items(), key=lambda item: item[0])
            }
        }
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.path)
