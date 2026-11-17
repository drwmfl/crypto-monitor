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

    def test_dropped_families_can_be_readmitted_silently(self) -> None:
        router = PushRouter({"enabled": True, "runtime_dir": self.tmp.name, "discovery": {
            "up": {"t1_slow_mode": "silent", "t2_low_weak_mode": "silent"}, "down": {"t2_low_enabled": True}}})
        features = {"vol5x": 1.2, "speed_min": 45}
        router.feature_provider = lambda symbol, direction: dict(features)
        weak = router.route_discovery(_row("A1USDT"))
        self.assertTrue(weak.send and weak.silent)
        self.assertEqual(weak.reason, "t2_low_weak_silent")
        slow = router.route_discovery(_row("A2USDT", triggers=("T1",)))
        self.assertTrue(slow.send and slow.silent)
        self.assertEqual(slow.reason, "t1_slow_silent")
        down = router.route_discovery(_row("D1USDT", "down", runup=8.5))
        self.assertTrue(down.send and down.silent)

    def test_ring_takes_over_thread_held_by_silent_push(self) -> None:
        first = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="risk_alert", window="1m")
        self.assertTrue(first.send and first.silent)
        ring = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m")
        self.assertTrue(ring.send)
        self.assertFalse(ring.silent)
        again = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m")
        self.assertFalse(again.send)
        silent_again = self.router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="risk_alert", window="1m")
        self.assertFalse(silent_again.send)

    def test_blocked_instrument_types_are_dropped(self) -> None:
        router = PushRouter({"enabled": True, "runtime_dir": self.tmp.name, "blocked_instrument_types": ["stock"]})
        r = router.route_legacy(symbol="TSLAUSDT", direction="up", alert_type="startup_alert", window="1m", instrument_type="stock")
        self.assertFalse(r.send)
        self.assertEqual(r.reason, "instrument_blocked")
        r = router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m", instrument_type="crypto")
        self.assertTrue(r.send)
        r = router.route_legacy(symbol="BBBUSDT", direction="up", alert_type="startup_alert", window="1m")
        self.assertTrue(r.send)

    def _escalating_router(self, **escalation) -> PushRouter:
        settings = {"enabled": True, "min_move_pct": 15.0, "directions": ["up"]}
        settings.update(escalation)
        return PushRouter({"enabled": True, "runtime_dir": self.tmp.name, "escalation": settings})

    def test_escalation_rings_again_when_price_runs_past_last_push(self) -> None:
        router = self._escalating_router()
        legacy = dict(symbol="AAAUSDT", direction="up", window="1m")
        self.assertTrue(router.route_legacy(alert_type="startup_alert", price=1.0, **legacy).send)
        # +10% inside the thread: still deduped.
        self.assertFalse(router.route_legacy(alert_type="strong_direct_alert", price=1.10, **legacy).send)
        # +16%: rings again, even for a type that would otherwise be silent or filtered.
        r = router.route_legacy(alert_type="risk_alert", price=1.16, **legacy)
        self.assertTrue(r.send)
        self.assertFalse(r.silent)
        self.assertEqual(r.reason, "escalation_ring")
        self.assertIn("🚀 行情加速：较上次推送再涨 16.0%", router.decorate("title\n🔗 https://x", r).split("\n"))
        # The next escalation is measured from the escalated push, not from the first one.
        self.assertFalse(router.route_legacy(alert_type="startup_alert", price=1.30, **legacy).send)
        filtered = router.route_legacy(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="15m", price=1.34)
        self.assertEqual(filtered.reason, "escalation_ring")
        rows = [json.loads(line) for line in (Path(self.tmp.name) / "push_router_shadow.jsonl").read_text("utf-8").splitlines()]
        self.assertEqual(rows[-1]["base_reason"], "legacy_window_filtered")
        self.assertEqual(rows[-1]["features"]["price"], 1.34)

    def test_escalation_applies_to_discovery_rows_and_only_inside_a_thread(self) -> None:
        router = self._escalating_router()
        router.feature_provider = lambda symbol, direction: {"vol5x": 4.0}
        # No earlier push: a filtered signal stays filtered whatever its price.
        self.assertFalse(router.route_legacy(symbol="BBBUSDT", direction="up", alert_type="startup_alert", window="15m", price=9.0).send)
        self.assertTrue(router.route_discovery(dict(_row("AAAUSDT"), price=1.0)).send)
        upgrade = router.route_discovery(dict(_row("AAAUSDT", kind="upgrade"), price=1.2))
        self.assertEqual(upgrade.reason, "escalation_ring")
        self.assertFalse(upgrade.silent)

    def test_escalation_respects_direction_and_missing_price(self) -> None:
        router = self._escalating_router()
        down = dict(symbol="AAAUSDT", direction="down", alert_type="strong_direct_alert", window="1m")
        self.assertTrue(router.route_legacy(price=1.0, **down).send)
        self.assertFalse(router.route_legacy(price=0.7, **down).send)
        up = dict(symbol="BBBUSDT", direction="up", alert_type="startup_alert", window="1m")
        self.assertTrue(router.route_legacy(**up).send)
        self.assertFalse(router.route_legacy(price=5.0, **up).send)
        blocked = PushRouter({"enabled": True, "runtime_dir": self.tmp.name, "blocked_instrument_types": ["stock"],
                              "escalation": {"enabled": True, "directions": ["up"]}})
        stock = dict(symbol="TSLAUSDT", direction="up", alert_type="startup_alert", window="1m")
        blocked._threads[("TSLAUSDT", "up")] = (__import__("time").time(), False, 1.0)
        self.assertFalse(blocked.route_legacy(price=2.0, instrument_type="stock", **stock).send)

    def test_escalation_is_off_by_default(self) -> None:
        legacy = dict(symbol="AAAUSDT", direction="up", alert_type="startup_alert", window="1m")
        self.assertTrue(self.router.route_legacy(price=1.0, **legacy).send)
        self.assertFalse(self.router.route_legacy(price=2.0, **legacy).send)

    def test_every_verdict_is_recorded(self) -> None:
        self.router.route_legacy(symbol="AAAUSDT", direction="down", alert_type="risk_alert", window="1m")
        rows = [json.loads(line) for line in (Path(self.tmp.name) / "push_router_shadow.jsonl").read_text("utf-8").splitlines()]
        self.assertEqual(rows[-1]["reason"], "legacy_type_filtered")
        self.assertFalse(rows[-1]["send"])


class BurstDigestTests(unittest.IsolatedAsyncioTestCase):
    def _router(self, tmp: str, notifier) -> PushRouter:
        router = PushRouter({"enabled": True, "runtime_dir": tmp, "burst": {
            "enabled": True, "min_coins": 3, "window_sec": 60, "quiet_sec": 0.05, "max_hold_sec": 0.2}})
        router.notifier = notifier
        router.feature_provider = lambda symbol, direction: {"qv24": 1e7}
        return router

    async def test_market_wide_move_is_merged_into_one_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            notifier = SimpleNamespace(notify_text=AsyncMock(return_value=True))
            router = self._router(tmp, notifier)
            routes = [router.route_discovery(_row(f"C{i}USDT", "down", triggers=("T1",), runup=-6.0)) for i in range(6)]
            # The first two still go out on their own; from the third coin on they are held for the digest.
            self.assertEqual([r.send for r in routes], [True, True, False, False, False, False])
            self.assertEqual(routes[2].reason, "market_burst:t1_ring")
            await asyncio.sleep(0.4)
            self.assertEqual(notifier.notify_text.await_count, 1)
            message = notifier.notify_text.await_args.args[0]
            self.assertIn("市场集体急跌 | 4 个币同时异动", message)
            self.assertIn("• C5 | 异动1分钟急变 | -6.0% | 24h额 1000万", message)
            self.assertEqual(notifier.notify_text.await_args.kwargs["rule_name"], "market_burst")
            # Absorbed coins keep their thread, and the other direction is unaffected.
            self.assertFalse(router.route_discovery(_row("C5USDT", "down", triggers=("T1",))).send)
            self.assertTrue(router.route_discovery(_row("C5USDT", "up", triggers=("T1",))).send)

    async def test_same_coin_repeats_do_not_trigger_burst(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            notifier = SimpleNamespace(notify_text=AsyncMock(return_value=True))
            router = self._router(tmp, notifier)
            for _ in range(5):
                router.route_discovery(_row("AAAUSDT", "down", triggers=("T1",)))
            self.assertTrue(router.route_discovery(_row("BBBUSDT", "down", triggers=("T1",))).send)
            await asyncio.sleep(0.3)
            self.assertEqual(notifier.notify_text.await_count, 0)

    def test_burst_is_off_without_notifier(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            router = self._router(tmp, None)
            routes = [router.route_discovery(_row(f"C{i}USDT", "down", triggers=("T1",))) for i in range(6)]
            self.assertTrue(all(r.send for r in routes))


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
        self.assertEqual(router.discovery["up"]["t2_low_weak_mode"], "silent")
        self.assertTrue(router.discovery["down"]["t2_low_enabled"])
        self.assertEqual(router.blocked_instruments, {"stock"})
        self.assertTrue(router.burst_enabled)
        self.assertEqual(router.burst_min_coins, 6)
        self.assertEqual(router.discovery["up"]["t1_max_speed_minutes"], 0)
        self.assertTrue(router.escalation_enabled)
        self.assertEqual(router.escalation_min_pct, 15.0)
        self.assertEqual(router.escalation_directions, {"up"})


if __name__ == "__main__":
    unittest.main()
