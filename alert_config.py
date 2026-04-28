from __future__ import annotations

from typing import Any, Dict, Optional

from apps.market_monitor.backend.alert_config import AlertConfig, load_alert_config


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Root-level compatibility loader (dict output).
    """
    return load_alert_config(config_path=config_path).raw


__all__ = ["AlertConfig", "load_alert_config", "load_config"]
