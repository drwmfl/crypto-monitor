from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import orjson
except Exception:  # pragma: no cover - orjson is optional, stdlib json is the fallback
    orjson = None


def dumps_compact(payload: Any) -> bytes:
    """Compact UTF-8 JSON. orjson is several times faster than json on the large state caches, which are
    written on the event loop and stalled the realtime websocket while they serialized."""
    if orjson is not None:
        return orjson.dumps(payload, option=orjson.OPT_NON_STR_KEYS, default=_fallback)
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=_fallback).encode("utf-8")


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(dumps_compact(payload))
    tmp_path.replace(path)


def _fallback(value: Any) -> Any:
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    return str(value)
