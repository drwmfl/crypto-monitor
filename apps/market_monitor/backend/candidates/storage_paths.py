from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional


def resolve_runtime_dir(settings: Optional[Dict[str, Any]] = None) -> Path:
    settings = settings or {}
    configured = (
        os.getenv("ALERT_STRATEGY_RUNTIME_DIR")
        or str(settings.get("runtime_dir") or "").strip()
    )
    if configured:
        path = Path(configured)
    elif Path("/runtime").exists():
        path = Path("/runtime")
    else:
        repo_root = Path(__file__).resolve().parents[4]
        path = repo_root / "data" / "runtime" / "market_monitor"

    path.mkdir(parents=True, exist_ok=True)
    return path
