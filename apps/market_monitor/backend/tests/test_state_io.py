from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

try:
    import state_io
except ModuleNotFoundError:
    from apps.market_monitor.backend import state_io


class StateIoTests(unittest.TestCase):
    def test_round_trip_is_compact_utf8_json(self) -> None:
        payload = {"symbols": {"龙虾USDT": [{"t": 1, "v": 1.5}]}, "set": {1}, "tuple": (1, 2), 3: "int key"}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "state.json"
            state_io.write_json_atomic(path, payload)
            text = path.read_text(encoding="utf-8")
            loaded = json.loads(text)
        self.assertNotIn("\n", text)
        self.assertIn("龙虾USDT", text)
        self.assertEqual(loaded["symbols"]["龙虾USDT"][0]["v"], 1.5)
        self.assertEqual(loaded["set"], [1])
        self.assertEqual(loaded["tuple"], [1, 2])
        self.assertEqual(loaded["3"], "int key")
        self.assertFalse((path.parent / "state.json.tmp").exists())

    def test_stdlib_fallback_matches(self) -> None:
        payload = {"a": [1, 2.5, None, "x"], "b": {"c": True}}
        saved = state_io.orjson
        try:
            state_io.orjson = None
            fallback = json.loads(state_io.dumps_compact(payload))
        finally:
            state_io.orjson = saved
        self.assertEqual(fallback, json.loads(state_io.dumps_compact(payload)))


if __name__ == "__main__":
    unittest.main()
