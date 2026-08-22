from __future__ import annotations

import tempfile
import unittest
import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

try:
    from alert_config import load_config
    from alerts.derivatives_shadow_recorder import DerivativesShadowRecorder
    from candidates.candidate_models import Candidate
    from factors.derivatives_history import DerivativesHistoryStore, classify_derivatives_shadow
    from factors.binance_factors import BinanceFactorProvider
    from factors.oi_history import OIHistoryStore, classify_oi_regime_shadow
    from scoring.candidate_score import score_candidate
    from scoring.risk_score import score_risk
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_config
    from apps.market_monitor.backend.alerts.derivatives_shadow_recorder import DerivativesShadowRecorder
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.factors.derivatives_history import (
        DerivativesHistoryStore,
        classify_derivatives_shadow,
    )
    from apps.market_monitor.backend.factors.binance_factors import BinanceFactorProvider
    from apps.market_monitor.backend.factors.oi_history import OIHistoryStore, classify_oi_regime_shadow
    from apps.market_monitor.backend.scoring.candidate_score import score_candidate
    from apps.market_monitor.backend.scoring.risk_score import score_risk


class OIShadowTests(unittest.TestCase):
    def test_amount_change_is_not_polluted_by_price_notional(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            store = OIHistoryStore(
                {
                    "runtime_dir": runtime_dir,
                    "save_interval_sec": 0,
                    "min_zscore_samples": 8,
                }
            )
            store.merge_historical_rows(
                "TESTUSDT",
                [
                    {"timestamp": 1_000_000, "sumOpenInterest": "100", "sumOpenInterestValue": "100"},
                    {"timestamp": 1_300_000, "sumOpenInterest": "100", "sumOpenInterestValue": "200"},
                ],
            )
            metrics = store.metrics("TESTUSDT")
            self.assertAlmostEqual(metrics["oi_change_pct_5m"], 1.0)
            self.assertAlmostEqual(metrics["oi_amount_change_pct_5m"], 0.0)

    def test_turn_up_and_aligned_regime_are_detected(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            store = OIHistoryStore(
                {
                    "runtime_dir": runtime_dir,
                    "save_interval_sec": 0,
                    "min_zscore_samples": 8,
                    "turn_min_5m_pct": 0.015,
                }
            )
            rows = [
                {"timestamp": 1_000_000, "sumOpenInterest": "100", "sumOpenInterestValue": "100"},
                {"timestamp": 1_300_000, "sumOpenInterest": "99", "sumOpenInterestValue": "99"},
                {"timestamp": 1_600_000, "sumOpenInterest": "98", "sumOpenInterestValue": "98"},
                {"timestamp": 1_900_000, "sumOpenInterest": "101", "sumOpenInterestValue": "101"},
            ]
            store.merge_historical_rows("TURNUSDT", rows)
            metrics = store.metrics("TURNUSDT")
            self.assertEqual(metrics["oi_turn_direction"], "up")
            self.assertEqual(metrics["oi_turn_stage"], "early")
            classified = classify_oi_regime_shadow(
                metrics,
                context={"window": "5m", "direction": "up", "change_pct": 2.5},
            )
            self.assertEqual(classified["oi_shadow_regime"], "new_longs")
            self.assertEqual(classified["oi_shadow_primary_window"], "5m")


class DerivativesHistoryTests(unittest.TestCase):
    def test_basis_bootstrap_reservation_prevents_duplicate_requests(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir:
            store = DerivativesHistoryStore(
                {
                    "runtime_dir": runtime_dir,
                    "save_interval_sec": 0,
                    "basis_bootstrap_retry_sec": 1800,
                }
            )

            self.assertTrue(store.reserve_basis_bootstrap("TESTUSDT"))
            self.assertFalse(store.reserve_basis_bootstrap("TESTUSDT"))

    def test_basis_and_funding_windows_are_computed(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            store = DerivativesHistoryStore(
                {
                    "runtime_dir": runtime_dir,
                    "save_interval_sec": 0,
                    "min_sample_interval_sec": 0,
                    "min_stats_samples": 8,
                }
            )
            start = 1_000_000
            rows = []
            for index in range(30):
                ts = start + index * 300_000
                rows.append(
                    {
                        "timestamp": ts,
                        "basisRate": str(index / 10000.0),
                        "basis": str(index),
                        "futuresPrice": str(100 + index / 100.0),
                        "indexPrice": "100",
                    }
                )
                store.record_current(
                    "TESTUSDT",
                    market_basis_bps=None,
                    mark_basis_bps=None,
                    funding_rate=index * 0.000001,
                    funding_interval_hours=8,
                    timestamp_ms=ts,
                )
            store.merge_basis_rows("TESTUSDT", rows)
            latest_ts = start + 30 * 300_000
            store.record_current(
                "TESTUSDT",
                market_basis_bps=35.0,
                mark_basis_bps=34.0,
                funding_rate=0.00004,
                funding_interval_hours=4,
                index_price=100.0,
                mark_price=100.34,
                market_mid_price=100.35,
                timestamp_ms=latest_ts,
            )
            metrics = store.metrics("TESTUSDT")
            self.assertAlmostEqual(metrics["basis_bps_now"], 35.0)
            self.assertAlmostEqual(metrics["basis_delta_5m_bps"], 6.0)
            self.assertEqual(metrics["basis_shadow_status"], "ready")
            self.assertAlmostEqual(metrics["funding_rate_8h"], 0.00008)
            self.assertIn("funding_delta_15m_bps", metrics)

            derivatives = dict(metrics)
            derivatives.update(
                {
                    "oi_shadow_regime": "new_longs",
                    "oi_turn_direction": "none",
                }
            )
            state = classify_derivatives_shadow(derivatives, context={"direction": "up"})
            self.assertEqual(state["derivatives_shadow_policy_version"], "derivatives-v1-shadow")
            self.assertGreaterEqual(state["derivatives_shadow_opportunity_modifier"], 2.0)


class BinanceBasisPrewarmTests(unittest.IsolatedAsyncioTestCase):
    def _provider(self, runtime_dir: str) -> BinanceFactorProvider:
        return BinanceFactorProvider(
            {
                "runtime_dir": runtime_dir,
                "fetch_basis_history": True,
                "fetch_funding": False,
                "basis_request_spacing_sec": 0,
                "basis_rate_limit_cooldown_sec": 900,
                "derivatives_history": {
                    "runtime_dir": runtime_dir,
                    "save_interval_sec": 0,
                    "basis_bootstrap_retry_sec": 1800,
                },
            }
        )

    async def test_tradfi_contract_skips_unsupported_basis_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir:
            provider = self._provider(runtime_dir)
            provider._get_json = AsyncMock(return_value=[])

            result = await provider._prewarm_basis_history(
                None,
                symbol="HOODUSDT",
                context={
                    "instrument_type": "stock",
                    "instrument_contract_type": "TRADIFI_PERPETUAL",
                },
                diagnostics=[],
            )

            self.assertFalse(result)
            provider._get_json.assert_not_awaited()

    async def test_successful_basis_bootstrap_is_not_repeated(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir:
            provider = self._provider(runtime_dir)
            provider._get_json = AsyncMock(return_value=[])

            first = await provider._prewarm_basis_history(
                None,
                symbol="BTCUSDT",
                context={"instrument_type": "crypto"},
                diagnostics=[],
            )
            second = await provider._prewarm_basis_history(
                None,
                symbol="BTCUSDT",
                context={"instrument_type": "crypto"},
                diagnostics=[],
            )

            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual(provider._get_json.await_count, 1)

    async def test_rate_limit_pauses_followup_basis_requests(self) -> None:
        class RateLimitError(RuntimeError):
            status = 429

        with tempfile.TemporaryDirectory() as runtime_dir:
            provider = self._provider(runtime_dir)
            provider._get_json = AsyncMock(side_effect=RateLimitError("rate limited"))

            first = await provider._prewarm_basis_history(
                None,
                symbol="BTCUSDT",
                context={"instrument_type": "crypto"},
                diagnostics=[],
            )
            second = await provider._prewarm_basis_history(
                None,
                symbol="ETHUSDT",
                context={"instrument_type": "crypto"},
                diagnostics=[],
            )

            self.assertFalse(first)
            self.assertFalse(second)
            self.assertEqual(provider._get_json.await_count, 1)


class RecorderAndCompatibilityTests(unittest.TestCase):
    def test_recorder_marks_review_ready_without_changing_scores(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            recorder = DerivativesShadowRecorder(
                {
                    "runtime_dir": runtime_dir,
                    "derivatives_shadow_review_min_days": 0,
                    "derivatives_shadow_review_min_first_push_samples": 2,
                }
            )
            initial = recorder.summary()
            self.assertEqual(initial["valid_first_push_samples"], 0)
            self.assertEqual(initial["review_gate"]["status"], "COLLECTING")
            self.assertTrue(Path(runtime_dir, "derivatives_shadow_readiness.json").exists())
            row = {
                "factor_state": {
                    "basis_bps_now": 10.0,
                    "oi_amount_change_pct_5m": 0.02,
                    "funding_shadow_status": "ready",
                },
                "actual_decision": {"alert_sent": True},
            }
            recorder.record({**row, "symbol": "AAAUSDT"}, alert_sent=True)
            summary = recorder.record({**row, "symbol": "BBBUSDT"}, alert_sent=True)
            self.assertEqual(summary["valid_first_push_samples"], 2)
            self.assertEqual(summary["review_gate"]["status"], "READY_FOR_REVIEW")

            candidate = Candidate(candidate_id="AAAUSDT", symbol="AAAUSDT", base_asset="AAA")
            candidate.latest_features = {
                "direction": "up",
                "change_pct": 3.0,
                "rvol": 2.0,
                "confidence": 70.0,
            }
            before_score = score_candidate(candidate)
            before_risk = score_risk(candidate)
            candidate.derivatives.update(
                {
                    "basis_bps_now": 120.0,
                    "basis_shadow_state": "positive_expansion",
                    "oi_amount_change_pct_5m": 0.08,
                    "oi_shadow_regime": "new_longs",
                    "funding_delta_15m_bps": 0.5,
                    "derivatives_shadow_opportunity_modifier": 4.0,
                    "derivatives_shadow_risk_modifier": 10.0,
                }
            )
            self.assertEqual(before_score, score_candidate(candidate))
            self.assertEqual(before_risk, score_risk(candidate))

    def test_production_config_enables_shadow_only(self) -> None:
        backend_root = Path(__file__).resolve().parents[1]
        path = backend_root / "config" / "config.json"
        if not path.exists():
            path = backend_root.parent / "config" / "config.json"
        config = load_config(str(path))
        strategy = config["alert_strategy"]
        data_feed = config["data_feed"]
        self.assertTrue(strategy["derivatives_shadow_enabled"])
        self.assertTrue(strategy["confirmation_factors"]["binance"]["fetch_basis_history"])
        self.assertEqual(strategy["actionable_policy_version"], "edge3-shadow-v1")
        self.assertEqual(data_feed["ws_realtime_ping_timeout_sec"], 0.0)
        self.assertEqual(data_feed["ws_realtime_processing_concurrency"], 8)
        self.assertEqual(data_feed["ws_realtime_max_pending_evaluations"], 128)
        position_pressure = strategy["confirmation_factors"]["position_pressure"]
        self.assertFalse(position_pressure["display_enabled"])
        self.assertFalse(position_pressure["risk_enabled"])
        self.assertFalse(position_pressure["confirmation_enabled"])


if __name__ == "__main__":
    unittest.main()
