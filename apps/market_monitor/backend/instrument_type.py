from __future__ import annotations

from typing import Any, Dict


STOCK_CONTRACT_BADGE = "📊 股票合约"


def binance_instrument_metadata(market: Dict[str, Any]) -> Dict[str, Any]:
    info = market.get("info") if isinstance(market.get("info"), dict) else {}
    contract_type = _normalized_value(info.get("contractType") or market.get("contractType"))
    underlying_type = _normalized_value(info.get("underlyingType") or market.get("underlyingType"))
    raw_subtypes = info.get("underlyingSubType") or market.get("underlyingSubType") or []
    if isinstance(raw_subtypes, str):
        underlying_subtypes = [raw_subtypes.strip()]
    elif isinstance(raw_subtypes, list):
        underlying_subtypes = [str(item).strip() for item in raw_subtypes if str(item).strip()]
    else:
        underlying_subtypes = []

    if underlying_type == "EQUITY" or underlying_type.endswith("_EQUITY"):
        instrument_type = "stock"
    elif underlying_type == "COMMODITY":
        instrument_type = "commodity"
    elif underlying_type == "INDEX":
        instrument_type = "index"
    elif underlying_type == "PREMARKET":
        instrument_type = "premarket"
    elif contract_type == "TRADIFI_PERPETUAL":
        instrument_type = "tradifi"
    else:
        instrument_type = "crypto"

    return {
        "instrument_type": instrument_type,
        "instrument_contract_type": contract_type,
        "instrument_underlying_type": underlying_type,
        "instrument_underlying_subtypes": underlying_subtypes,
    }


def instrument_badge(instrument_type: Any) -> str:
    return STOCK_CONTRACT_BADGE if str(instrument_type or "").strip().lower() == "stock" else ""


def _normalized_value(value: Any) -> str:
    return str(value or "").strip().upper()
