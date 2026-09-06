from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

try:
    import strategy_pipeline as strategy_pipeline_module
    from alerts.alert_policy import AlertDecision
    from candidates.candidate_models import Candidate
    from strategy_pipeline import AlertStrategyPipeline, StrategyProcessResult
except ModuleNotFoundError:
    from apps.market_monitor.backend import strategy_pipeline as strategy_pipeline_module
    from apps.market_monitor.backend.alerts.alert_policy import AlertDecision
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.strategy_pipeline import AlertStrategyPipeline, StrategyProcessResult


class _PolicyStub:
    def __init__(self, decision: AlertDecision) -> None:
        self.decision = decision
        self.sent: list[tuple[str, str]] = []

    def decide(self, candidate: Candidate) -> AlertDecision:
        return self.decision

    def allow_alert_type(self, symbol: str, alert_type: str, **_: object) -> AlertDecision:
        if any(item == (symbol, alert_type) for item in self.sent):
            return AlertDecision(False, alert_type=alert_type, reason="symbol_cooldown")
        return AlertDecision(True, alert_type=alert_type, reason="accepted")

    def recently_sent_any(self, symbol: str, alert_types: object, **_: object) -> None:
        return None

    def mark_sent(self, symbol: str, alert_type: str) -> None:
        self.sent.append((symbol, alert_type))


class _DedupeStub:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def decide(self, candidate: Candidate, *, alert_type: str) -> SimpleNamespace:
        return SimpleNamespace(should_send=True, reason="new_event")

    def mark_sent(self, candidate: Candidate, *, alert_type: str) -> None:
        self.sent.append((candidate.symbol, alert_type))


class StrategyPipelineConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _bare_pipeline() -> AlertStrategyPipeline:
        pipeline = AlertStrategyPipeline.__new__(AlertStrategyPipeline)
        pipeline._symbol_locks = {}
        return pipeline

    async def test_same_symbol_is_serialized(self) -> None:
        pipeline = self._bare_pipeline()
        active = 0
        max_active = 0

        async def worker(payload: dict, *, raw_event: object, notifier: object) -> StrategyProcessResult:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.02)
            active -= 1
            return StrategyProcessResult("METUSDT", False, "none", "test", 0.0, 0.0)

        pipeline._process_event_serialized = worker
        payload = {"symbol": "METUSDT", "rule_name": "early_start", "direction": "up"}

        await asyncio.gather(
            pipeline.process_event(payload, source="ws", notifier=None),
            pipeline.process_event(payload, source="poll", notifier=None),
        )

        self.assertEqual(max_active, 1)

    async def test_different_symbols_remain_concurrent(self) -> None:
        pipeline = self._bare_pipeline()
        entered = 0
        both_entered = asyncio.Event()
        release = asyncio.Event()

        async def worker(payload: dict, *, raw_event: object, notifier: object) -> StrategyProcessResult:
            nonlocal entered
            entered += 1
            if entered == 2:
                both_entered.set()
            await release.wait()
            return StrategyProcessResult(str(payload["symbol"]), False, "none", "test", 0.0, 0.0)

        pipeline._process_event_serialized = worker
        tasks = [
            asyncio.create_task(
                pipeline.process_event(
                    {"symbol": symbol, "rule_name": "early_start", "direction": "up"},
                    source="ws",
                    notifier=None,
                )
            )
            for symbol in ("METUSDT", "TAKEUSDT")
        ]

        await asyncio.wait_for(both_entered.wait(), timeout=1.0)
        release.set()
        await asyncio.gather(*tasks)

        self.assertEqual(entered, 2)


class StrategyPipelinePriorityTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _candidate() -> Candidate:
        return Candidate(
            candidate_id="METUSDT",
            symbol="METUSDT",
            base_asset="MET",
            event_count=1,
            windows=["15m"],
            latest_features={
                "event_type": "early_start",
                "direction": "up",
                "window": "15m",
                "change_pct": 6.68,
                "price": 0.51,
                "rvol": 10.42,
                "startup_window": "15m",
                "startup_change_pct": 6.68,
                "startup_breakout_distance_pct": 0.64,
            },
            confirmation={
                "checks": [
                    {
                        "key": "price_persistence",
                        "passed": True,
                        "value": {"windows": ["5m", "15m"]},
                    }
                ]
            },
        )

    @staticmethod
    def _pipeline(candidate: Candidate, decision: AlertDecision) -> AlertStrategyPipeline:
        pipeline = AlertStrategyPipeline.__new__(AlertStrategyPipeline)
        pipeline.settings = {"factor_retry_enabled": False, "telegram_detail_level": "compact"}
        pipeline._symbol_locks = {}
        pipeline.event_store = SimpleNamespace(append=Mock())
        pipeline.candidate_engine = SimpleNamespace(update_candidate=Mock(return_value=candidate), save=Mock())
        pipeline.factor_enricher = SimpleNamespace(should_enrich=Mock(return_value=False))
        pipeline.policy = _PolicyStub(decision)
        pipeline.event_deduper = _DedupeStub()
        pipeline._schedule_hot_pool_prewarm = Mock()
        pipeline._apply_trade_confirmation = Mock()
        pipeline._record_actionable_policy_shadow = Mock()
        pipeline._record_derivatives_shadow = Mock()
        pipeline._record_position_pressure_shadow = Mock()
        pipeline._record_factor_quality = Mock()
        return pipeline

    async def test_early_start_promotes_to_one_actionable_alert(self) -> None:
        candidate = self._candidate()
        pipeline = self._pipeline(
            candidate,
            AlertDecision(True, alert_type="actionable_alert", reason="accepted"),
        )
        notifier = SimpleNamespace(notify_text=AsyncMock(return_value=True))

        with patch.object(strategy_pipeline_module, "score_candidate", return_value=(100.0, {}, "high")), patch.object(
            strategy_pipeline_module,
            "score_risk",
            return_value=(27.0, "low", {}),
        ):
            result = await pipeline.process_event(
                {
                    "symbol": "METUSDT",
                    "rule_name": "early_start",
                    "direction": "up",
                    "window": "15m",
                    "change_pct": 6.68,
                },
                source="ws",
                notifier=notifier,
            )

        self.assertTrue(result.alert_sent)
        self.assertEqual(result.alert_type, "actionable_alert")
        notifier.notify_text.assert_awaited_once()
        message = notifier.notify_text.await_args.args[0]
        self.assertIn("**💎 高价值候选 | MET", message)
        self.assertIn("✅ 确认：多周期持续触发", message)
        self.assertIn("🚀 启动：15m累计 +6.68%", message)
        self.assertEqual(pipeline.policy.sent, [("METUSDT", "actionable_alert")])
        self.assertEqual(pipeline.event_deduper.sent, [("METUSDT", "actionable_alert")])

    async def test_early_start_falls_back_to_startup_when_not_primary(self) -> None:
        candidate = self._candidate()
        pipeline = self._pipeline(
            candidate,
            AlertDecision(False, alert_type="watchlist_alert", reason="watchlist_tg_disabled"),
        )
        notifier = SimpleNamespace(notify_text=AsyncMock(return_value=True))

        with patch.object(strategy_pipeline_module, "score_candidate", return_value=(60.0, {}, "medium")), patch.object(
            strategy_pipeline_module,
            "score_risk",
            return_value=(30.0, "low", {}),
        ):
            result = await pipeline.process_event(
                {
                    "symbol": "METUSDT",
                    "rule_name": "early_start",
                    "direction": "up",
                    "window": "15m",
                    "change_pct": 6.68,
                },
                source="ws",
                notifier=notifier,
            )

        self.assertTrue(result.alert_sent)
        self.assertEqual(result.alert_type, "startup_alert")
        notifier.notify_text.assert_awaited_once()
        self.assertIn("**🚀 启动预警 | MET", notifier.notify_text.await_args.args[0])

    async def test_concurrent_same_symbol_startup_sends_only_once(self) -> None:
        candidate = self._candidate()
        pipeline = self._pipeline(
            candidate,
            AlertDecision(False, alert_type="watchlist_alert", reason="watchlist_tg_disabled"),
        )
        notifier = SimpleNamespace(notify_text=AsyncMock(return_value=True))
        payload = {
            "symbol": "METUSDT",
            "rule_name": "early_start",
            "direction": "up",
            "window": "15m",
            "change_pct": 6.68,
        }

        with patch.object(strategy_pipeline_module, "score_candidate", return_value=(60.0, {}, "medium")), patch.object(
            strategy_pipeline_module,
            "score_risk",
            return_value=(30.0, "low", {}),
        ):
            results = await asyncio.gather(
                pipeline.process_event(payload, source="ws", notifier=notifier),
                pipeline.process_event(payload, source="poll", notifier=notifier),
            )

        self.assertEqual(sum(result.alert_sent for result in results), 1)
        self.assertEqual(notifier.notify_text.await_count, 1)
        self.assertEqual(pipeline.policy.sent, [("METUSDT", "startup_alert")])


if __name__ == "__main__":
    unittest.main()
