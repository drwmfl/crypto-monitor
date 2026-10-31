from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

try:
    from alert_config import load_alert_config
    from alerts.discovery import DiscoveryPusher
    from alerts.push_router import PushRouter
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_alert_config
    from apps.market_monitor.backend.alerts.discovery import DiscoveryPusher
    from apps.market_monitor.backend.alerts.push_router import PushRouter

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.json"


def _row(symbol="AAAUSDT", direction="up", triggers=("T2",), runup=8.5, kind="new"):
    return {"symbol": symbol, "direction": direction, "triggers": list(triggers), "runup_pct": runup,
            "kind": kind, "price": 1.0, "change_1m_pct": 1.0, "rvol": 2.0, "runup_minutes": 60}


class PushRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.router = PushRouter({"enabled": True, "runtime_dir": self.tmp.name})
        self.features = {}
        self.router.feature_provider = lambda symbol, direction: dict(self.features)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_disabled_by_default(self) -> None:
        self.assertFalse(PushRouter({}).enabled)

    def test_legacy_up_types_and_windows(self) -> None:
        r = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="5m")
        self.assertTrue(r.send)
        self.assertFalse(r.silent)
        r = self.router.route_legacy(symbol="BBBUSDT", direction="up", alert_type="risk_alert", window="1m")
        self.assertTrue(r.send)
        self.assertTrue(r.silent)
        r = self.router.route_legacy(symbol="CCCUSDT", direction="up", alert_type="actionable_alert", window="15m")
        self.assertFalse(r.send)
        self.assertEqual(r.reason, "legacy_window_filtered")

    def test_legacy_down_keeps_only_strong_direct(self) -> None:
        r = self.router.route_legacy(symbol="AAAUSDT", direction="down", alert_type="risk_alert", window="1m")
        self.assertFalse(r.send)
        r = self.router.route_legacy(symbol="AAAUSDT", direction="down", alert_type="strong_direct_alert", window="1m")
        self.assertTrue(r.send)
        self.assertFalse(r.silent)

    def test_discovery_t2_low_needs_volume_and_rings_on_strong_volume(self) -> None:
        self.features = {"vol5x": 1.2}
        self.assertEqual(self.router.route_discovery(_row("A1USDT")).reason, "t2_low_no_volume")
        self.features = {"vol5x": 2.5}
        r = self.router.route_discovery(_row("A2USDT"))
        self.assertTrue(r.send and r.silent)
        self.features = {"vol5x": 4.0}
        r = self.router.route_discovery(_row("A3USDT"))
        self.assertTrue(r.send)
        self.assertFalse(r.silent)

    def test_discovery_t2_low_without_history_is_sent_silently(self) -> None:
        self.features = {}
        r = self.router.route_discovery(_row())
        self.assertTrue(r.send and r.silent)

    def test_discovery_t1_up_requires_fast_move(self) -> None:
        self.features = {"speed_min": 45}
        self.assertEqual(self.router.route_discovery(_row("A1USDT", triggers=("T1",))).reason, "t1_slow_move")
        self.features = {"speed_min": 12}
        r = self.router.route_discovery(_row("A2USDT", triggers=("T1",)))
        self.assertTrue(r.send)
        self.assertFalse(r.silent)

    def test_discovery_down_rules(self) -> None:
        self.features = {"vol5x": 9.0, "qv24": 1e6}
        self.assertEqual(self.router.route_discovery(_row("D1USDT", "down", runup=8.5)).reason, "t2_low_disabled")
        self.assertEqual(self.router.route_discovery(_row("D2USDT", "down", runup=12.0)).reason, "t2_high_low_liquidity")
        self.features = {"qv24": 2e7}
        r = self.router.route_discovery(_row("D3USDT", "down", runup=12.0))
        self.assertTrue(r.send and r.silent)
        self.features = {"speed_min": 55}
        r = self.router.route_discovery(_row("D4USDT", "down", triggers=("T1",)))
        self.assertTrue(r.send)

    def test_shared_thread_blocks_second_push_across_strategies(self) -> None:
        self.features = {"vol5x": 4.0}
        first = self.router.route_discovery(_row("AAAUSDT"))
        self.assertTrue(first.send)
        second = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="strong_direct_alert", window="1m")
        self.assertFalse(second.send)
        self.assertTrue(second.reason.startswith("thread_dedupe"))
        other_dir = self.router.route_legacy(symbol="AAAUSDT", direction="down", alert_type="strong_direct_alert", window="1m")
        self.assertTrue(other_dir.send)

    def test_thread_expires(self) -> None:
        router = PushRouter({"enabled": True, "runtime_dir": self.tmp.name, "thread_minutes": 60})
        with patch("time.time", return_value=1000.0):
            self.assertTrue(router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m").send)
        with patch("time.time", return_value=1000.0 + 3599):
            self.assertFalse(router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m").send)
        with patch("time.time", return_value=1000.0 + 3601):
            self.assertTrue(router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m").send)

    def test_release_frees_the_thread(self) -> None:
        r = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m")
        self.router.release(r)
        self.assertTrue(self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m").send)

    def test_decorate_inserts_context_before_link(self) -> None:
        self.features = {"vol5x": 1.0}
        self.router.route_discovery(_row("AAAUSDT"))  # filtered, but noted as a co-signal
        self.features = {"speed_min": 7, "vol5x": 3.4, "qv24": 2.5e8}
        r = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="risk_alert", window="1m")
        text = self.router.decorate("title\n💵 现价 1\n🔗 https://x", r)
        lines = text.split("\n")
        self.assertEqual(lines[-1], "🔗 https://x")
        self.assertIn("📊 起涨7分钟 | 5分钟量 3.4倍 | 24h额 2.50亿", lines)
        self.assertIn("🤝 30分钟内同时命中：异动持续", lines)
        self.assertIn("🔕 次要信号（静音）", lines)

    def test_every_verdict_is_recorded(self) -> None:
        self.router.route_legacy(symbol="AAAUSDT", direction="down", alert_type="risk_alert", window="1m")
        rows = [json.loads(line) for line in (Path(self.tmp.name) / "push_router_shadow.jsonl").read_text("utf-8").splitlines()]
        self.assertEqual(rows[-1]["reason"], "legacy_type_filtered")
        self.assertFalse(rows[-1]["send"])


class DiscoveryPusherRoutingTests(unittest.TestCase):
    def test_routed_push_is_silent_and_filtered_push_is_dropped(self) -> None:
        async def run():
            with tempfile.TemporaryDirectory() as tmp:
                notifier = AsyncMock()
                notifier.notify_text = AsyncMock(return_value=True)
                pusher = DiscoveryPusher({"followup_enabled": False}, notifier)
                pusher.router = PushRouter({"enabled": True, "runtime_dir": tmp})
                features = {"vol5x": 2.5}
                pusher.router.feature_provider = lambda s, d: dict(features)
                task = pusher.push(_row("AAAUSDT"))
                await task
                self.assertTrue(notifier.notify_text.await_args.kwargs.get("silent"))
                features["vol5x"] = 0.5
                self.assertIsNone(pusher.push(_row("BBBUSDT")))
                self.assertEqual(notifier.notify_text.await_count, 1)

        asyncio.run(run())

    def test_unrouted_pusher_sends_without_silent_flag(self) -> None:
        async def run():
            notifier = AsyncMock()
            notifier.notify_text = AsyncMock(return_value=True)
            pusher = DiscoveryPusher({"followup_enabled": False}, notifier)
            await pusher.push(_row())
            self.assertNotIn("silent", notifier.notify_text.await_args.kwargs)

        asyncio.run(run())


class LegacyPipelineRoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _pipeline(self):
        try:
            from strategy_pipeline import AlertStrategyPipeline
        except ModuleNotFoundError:
            from apps.market_monitor.backend.strategy_pipeline import AlertStrategyPipeline
        pipeline = AlertStrategyPipeline.__new__(AlertStrategyPipeline)
        pipeline.settings = {"telegram_detail_level": "compact", "legacy_push_directions": ["down"]}
        pipeline._record_factor_quality = Mock()
        pipeline.push_router = PushRouter({"enabled": True, "runtime_dir": self.tmp.name})
        return pipeline

    @staticmethod
    def _candidate(direction: str, window: str = "1m"):
        try:
            from candidates.candidate_models import Candidate
        except ModuleNotFoundError:
            from apps.market_monitor.backend.candidates.candidate_models import Candidate
        return Candidate(
            candidate_id="METUSDT", symbol="METUSDT", base_asset="MET", event_count=1,
            latest_features={"event_type": "short_breakout", "direction": direction, "window": window,
                             "change_pct": 5.0 if direction == "up" else -5.0, "price": 1.0, "rvol": 3.0},
        )

    async def test_router_replaces_direction_mute_and_drops_risk_down(self) -> None:
        try:
            from alerts.alert_policy import AlertDecision
        except ModuleNotFoundError:
            from apps.market_monitor.backend.alerts.alert_policy import AlertDecision
        pipeline = self._pipeline()
        notifier = SimpleNamespace(notify_text=AsyncMock(return_value=True))
        # Up is muted by legacy_push_directions, but the router takes over and lets startup ring.
        sent = await pipeline._send_decision(
            notifier=notifier, candidate=self._candidate("up"),
            decision=AlertDecision(True, alert_type="startup_alert", reason="accepted"),
        )
        self.assertTrue(sent)
        self.assertNotIn("silent", notifier.notify_text.await_args.kwargs)
        sent = await pipeline._send_decision(
            notifier=notifier, candidate=self._candidate("down"),
            decision=AlertDecision(True, alert_type="risk_alert", reason="accepted"),
        )
        self.assertFalse(sent)
        self.assertEqual(notifier.notify_text.await_count, 1)

    async def test_failed_send_releases_thread(self) -> None:
        pipeline = self._pipeline()
        failing = SimpleNamespace(notify_text=AsyncMock(return_value=False))
        self.assertFalse(await pipeline._send_strong_direct(notifier=failing, candidate=self._candidate("down")))
        working = SimpleNamespace(notify_text=AsyncMock(return_value=True))
        self.assertTrue(await pipeline._send_strong_direct(notifier=working, candidate=self._candidate("down")))


class ConfigTests(unittest.TestCase):
    def test_production_config_enables_router_with_strategy_a(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"ALERT_STRATEGY_RUNTIME_DIR": tmp}):
                config = load_alert_config(str(CONFIG_PATH))
                router = PushRouter.from_config(config.raw)
        self.assertTrue(router.enabled)
        self.assertEqual(router.thread_sec, 3600)
        self.assertEqual(router.discovery["up"]["t2_low_min_vol5x"], 2.0)


if __name__ == "__main__":
    unittest.main()
