from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from alerts.discovery import DiscoveryShadow
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.discovery import DiscoveryShadow

MINUTE = 60_000
T0 = 1_792_000_000_000  # 2026-10-14 (UTC+8 afternoon)


def _observe(shadow: DiscoveryShadow, **overrides: object) -> list:
    kwargs = {
        "symbol": "CTKUSDT",
        "now_ms": T0,
        "price": 0.1369,
        "change_1m_pct": 0.5,
        "rvol": 1.0,
        "runup_up_pct": 2.0,
        "runup_down_pct": -1.0,
        "runup_minutes": 60,
    }
    kwargs.update(overrides)
    return shadow.observe(**kwargs)


class DiscoveryShadowTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.runtime_dir = self._tmp.name
        self._env = patch.dict("os.environ", {"ALERT_STRATEGY_RUNTIME_DIR": self.runtime_dir})
        self._env.start()

    def tearDown(self) -> None:
        self._env.stop()
        self._tmp.cleanup()

    def _shadow(self, **settings: object) -> DiscoveryShadow:
        return DiscoveryShadow({"enabled": True, "runtime_dir": self.runtime_dir, **settings})

    def test_disabled_records_nothing(self) -> None:
        shadow = DiscoveryShadow({"enabled": False})

        self.assertEqual(_observe(shadow, change_1m_pct=13.4, rvol=8.2), [])
        self.assertIsNone(shadow.shadow_path)

    def test_t1_needs_change_and_volume(self) -> None:
        shadow = self._shadow()

        self.assertEqual(_observe(shadow, change_1m_pct=4.5, rvol=1.0), [])
        rows = _observe(shadow, change_1m_pct=4.5, rvol=3.0)

        self.assertEqual([(r["direction"], r["triggers"], r["kind"]) for r in rows], [("up", ["T1"], "new")])

    def test_t2_steady_run_and_down_direction(self) -> None:
        shadow = self._shadow()

        up = _observe(shadow, runup_up_pct=8.2)
        down = _observe(shadow, symbol="ZEREBROUSDT", runup_up_pct=1.0, runup_down_pct=-9.0)

        self.assertEqual(up[0]["triggers"], ["T2"])
        self.assertEqual((down[0]["direction"], down[0]["triggers"]), ("down", ["T2"]))

    def test_thread_records_only_new_and_upgrades(self) -> None:
        shadow = self._shadow()

        first = _observe(shadow, runup_up_pct=9.0, price=1.00)
        same_move = _observe(shadow, runup_up_pct=12.0, price=1.10, now_ms=T0 + 10 * MINUTE)
        extended = _observe(shadow, runup_up_pct=20.0, price=1.16, now_ms=T0 + 20 * MINUTE)
        after_thread = _observe(shadow, runup_up_pct=9.0, price=1.05, now_ms=T0 + 5 * 60 * MINUTE)

        self.assertEqual(first[0]["kind"], "new")
        self.assertEqual(same_move, [])
        self.assertEqual(extended[0]["kind"], "upgrade")
        self.assertEqual(after_thread[0]["kind"], "new")

    def test_daily_cap_per_symbol_and_direction(self) -> None:
        shadow = self._shadow(daily_cap=2, thread_hours=0)

        kinds = [len(_observe(shadow, runup_up_pct=9.0, now_ms=T0 + i * MINUTE)) for i in range(4)]
        down = _observe(shadow, runup_up_pct=0.0, runup_down_pct=-9.0, now_ms=T0 + 5 * MINUTE)

        self.assertEqual(kinds, [1, 1, 0, 0])
        self.assertEqual(len(down), 1)

    def test_rows_and_state_survive_restart(self) -> None:
        shadow = self._shadow()
        _observe(shadow, change_1m_pct=13.4, rvol=8.2, runup_up_pct=21.6)

        restarted = self._shadow()
        repeat = _observe(restarted, change_1m_pct=6.0, rvol=4.0, now_ms=T0 + 2 * MINUTE)

        self.assertEqual(repeat, [])
        lines = Path(self.runtime_dir, "discovery_shadow.jsonl").read_text(encoding="utf-8").splitlines()
        row = json.loads(lines[0])
        self.assertEqual(len(lines), 1)
        self.assertEqual(row["triggers"], ["T1", "T2"])
        self.assertEqual(row["mode"], "shadow")
        self.assertAlmostEqual(row["runup_pct"], 21.6)


if __name__ == "__main__":
    unittest.main()
