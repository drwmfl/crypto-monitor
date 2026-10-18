from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    from alerts.discovery import DiscoveryPusher, DiscoveryShadow, format_discovery_alert
    from notifier import AlertNotifier
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.discovery import DiscoveryPusher, DiscoveryShadow, format_discovery_alert
    from apps.market_monitor.backend.notifier import AlertNotifier

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

    def test_t2_close_confirmation_ignores_intra_minute_wicks(self) -> None:
        shadow = self._shadow(t2_confirm_on_close=True)

        intra = _observe(shadow, runup_up_pct=9.0, is_closed=False)
        spike = _observe(shadow, symbol="TIAUSDT", change_1m_pct=4.3, rvol=12.7, runup_up_pct=9.0, is_closed=False)
        closed = _observe(shadow, runup_up_pct=8.4, is_closed=True)

        self.assertEqual(intra, [])
        self.assertEqual(spike[0]["triggers"], ["T1"])
        self.assertEqual(closed[0]["triggers"], ["T2"])

    def test_instrument_filter_skips_stock_contracts(self) -> None:
        shadow = self._shadow(instrument_types=["crypto"])

        stock = _observe(shadow, symbol="MUUUSDT", runup_down_pct=-9.0, instrument_type="stock")
        crypto = _observe(shadow, runup_up_pct=9.0, instrument_type="crypto")
        unknown = _observe(shadow, symbol="NEWUSDT", runup_up_pct=9.0, instrument_type=None)

        self.assertEqual(stock, [])
        self.assertEqual(len(crypto), 1)
        self.assertEqual(len(unknown), 1)

    def test_push_directions_mark_live_rows_and_upgrade_keeps_previous_price(self) -> None:
        shadow = self._shadow(push_directions=["up"])

        up = _observe(shadow, runup_up_pct=9.0, price=1.0)
        down = _observe(shadow, symbol="ZEREBROUSDT", runup_up_pct=0.0, runup_down_pct=-9.0)
        upgrade = _observe(shadow, runup_up_pct=26.0, price=1.2, now_ms=T0 + 30 * MINUTE)

        self.assertEqual((up[0]["mode"], up[0]["prev_price"]), ("live", None))
        self.assertEqual(down[0]["mode"], "shadow")
        self.assertEqual((upgrade[0]["kind"], upgrade[0]["prev_price"]), ("upgrade", 1.0))


class DiscoveryMessageTests(unittest.TestCase):
    def test_new_alert_message(self) -> None:
        row = {
            "symbol": "CTKUSDT", "direction": "up", "kind": "new", "triggers": ["T1", "T2"], "price": 0.1369,
            "prev_price": None, "change_1m_pct": 13.42, "rvol": 8.2, "runup_pct": 21.58, "runup_minutes": 61,
        }

        message = format_discovery_alert(row, followup_minutes=15)

        self.assertIn("**🔎 异动发现 | CTK（触发1次）**", message)
        self.assertIn("📈 近61分钟 +21.58% | 1分钟 +13.42% | RVOL 8.20x", message)
        self.assertIn("🧭 触发：1分钟急拉 + 持续拉升", message)
        self.assertIn("💵 现价 0.1369", message)
        self.assertIn("⏱ 15分钟后跟进", message)
        self.assertNotIn("可操作", message)
        titled = AlertNotifier._apply_daily_push_title(message, 2)
        self.assertIn("**🔎 异动发现 | CTK（今日第2次推送）**", titled)

    def test_upgrade_message_warns_about_overheating(self) -> None:
        row = {
            "symbol": "CTKUSDT", "direction": "up", "kind": "upgrade", "triggers": ["T2"], "price": 0.16,
            "prev_price": 0.1369, "change_1m_pct": 1.0, "rvol": 2.0, "runup_pct": 42.1, "runup_minutes": 61,
        }

        message = format_discovery_alert(row)

        self.assertIn("**🔥 异动加速 | CTK（触发1次）**", message)
        self.assertIn("较上次提醒 0.1369 → 0.16（+16.87%） | 近61分钟 +42.10%", message)
        self.assertIn("⚠️ 加速阶段，注意过热回落", message)
        self.assertNotIn("分钟后跟进", message)


class DiscoveryPusherTests(unittest.IsolatedAsyncioTestCase):
    ROW = {
        "symbol": "CTKUSDT", "direction": "up", "kind": "new", "triggers": ["T2"], "price": 100.0,
        "prev_price": None, "change_1m_pct": 1.0, "rvol": 2.0, "runup_pct": 8.5, "runup_minutes": 61,
    }

    def _pusher(self, sent: bool = True) -> DiscoveryPusher:
        notifier = SimpleNamespace(notify_text=AsyncMock(return_value=sent))
        client = SimpleNamespace(fetch_1m_bars=AsyncMock(return_value=[[0, 100.0, 109.0, 99.0, 106.0]]))
        pusher = DiscoveryPusher({"followup_minutes": 15}, notifier, client=client)
        pusher._followup_delay_sec = lambda: 0.0
        return pusher

    async def test_push_sends_alert_then_followup(self) -> None:
        pusher = self._pusher()

        await pusher.push(dict(self.ROW))

        calls = pusher.notifier.notify_text.await_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].kwargs["symbol"], "CTKUSDT")
        self.assertEqual(calls[0].kwargs["rule_name"], "discovery_alert")
        self.assertIn("**⏱ 异动跟进 | CTK（15分钟后）**", calls[1].args[0])
        self.assertIn("✅ 状态：上涨延续", calls[1].args[0])
        self.assertNotIn("symbol", calls[1].kwargs)

    async def test_no_followup_when_alert_not_sent(self) -> None:
        pusher = self._pusher(sent=False)

        await pusher.push(dict(self.ROW))

        self.assertEqual(pusher.notifier.notify_text.await_count, 1)
        pusher.client.fetch_1m_bars.assert_not_awaited()

    async def test_close_cancels_pending_followups(self) -> None:
        pusher = self._pusher()
        pusher._followup_delay_sec = lambda: 3600.0

        task = pusher.push(dict(self.ROW))
        await asyncio.sleep(0)
        await pusher.close()

        self.assertTrue(task.cancelled())
        pusher.client.fetch_1m_bars.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
