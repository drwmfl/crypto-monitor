from __future__ import annotations

from pathlib import Path

FILE_DIR = Path(__file__).resolve().parent
APP_ROOT = FILE_DIR.parent if (FILE_DIR.parent / "config").exists() else FILE_DIR
REPO_ROOT = APP_ROOT.parent.parent if APP_ROOT.parent.name == "apps" else APP_ROOT


def default_config_path() -> str:
    candidates = [
        APP_ROOT / "config" / "config.json",
        REPO_ROOT / "apps" / "market_monitor" / "config" / "config.json",
        REPO_ROOT / "config.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate.resolve())
    return "config.json"


def default_reports_dir() -> str:
    candidates = [
        REPO_ROOT / "data" / "reports" / "market_monitor",
        APP_ROOT / "reports",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate.resolve())
    return "reports"
