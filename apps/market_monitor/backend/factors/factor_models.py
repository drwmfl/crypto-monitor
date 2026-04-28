from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class FactorSnapshot:
    symbol: str
    base_asset: str
    updated_at: str = field(default_factory=utc_now_iso)
    derivatives: Dict[str, Any] = field(default_factory=dict)
    orderbook: Dict[str, Any] = field(default_factory=dict)
    liquidation: Dict[str, Any] = field(default_factory=dict)
    accumulation: Dict[str, Any] = field(default_factory=dict)
    source_health: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def empty(cls, symbol: str, base_asset: str) -> "FactorSnapshot":
        return cls(symbol=symbol, base_asset=base_asset)


def merge_factor_snapshot(existing: Optional[Dict[str, Any]], incoming: FactorSnapshot) -> Dict[str, Any]:
    result: Dict[str, Any] = dict(existing or {})
    payload = incoming.to_dict()
    result["symbol"] = incoming.symbol
    result["base_asset"] = incoming.base_asset
    result["updated_at"] = incoming.updated_at
    for key in ("derivatives", "orderbook", "liquidation", "accumulation", "source_health"):
        current = result.get(key)
        if not isinstance(current, dict):
            current = {}
        value = payload.get(key)
        if isinstance(value, dict) and value:
            current.update(value)
        result[key] = current
    return result


def recent_health_entry(source: str, ok: bool, message: str = "") -> Dict[str, Any]:
    return {
        "source": source,
        "ok": bool(ok),
        "message": str(message or ""),
        "checked_at": utc_now_iso(),
    }
