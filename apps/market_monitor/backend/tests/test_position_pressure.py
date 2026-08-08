from __future__ import annotations

import asyncio
import os
import tempfile
import time
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import patch

try:
    from alert_config import load_config
    from alerts.position_pressure_shadow_recorder import PositionPressureShadowRecorder
    from alerts.tg_formatter import _position_pressure_line
    from alerts.trade_confirmation import evaluate_trade_confirmation
    from candidates.candidate_models import Candidate
    from factors.liquidation_history import LiquidationHistoryStore
    from factors.factor_enricher import FactorEnricher
    from factors.microstructure import MicrostructureProvider
    from factors.position_pressure import classify_position_pressure
    from factors.smart_money import SmartMoneyProvider, build_smart_money_metrics
    from scoring.candidate_score import score_candidate
    from scoring.risk_score import score_risk
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_config
    from apps.market_monitor.backend.alerts.position_pressure_shadow_recorder import (
        PositionPressureShadowRecorder,
    )
    from apps.market_monitor.backend.alerts.tg_formatter import _position_pressure_line
    from apps.market_monitor.backend.alerts.trade_confirmation import evaluate_trade_confirmation
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.factors.liquidation_history import LiquidationHistoryStore
    from apps.market_monitor.backend.factors.factor_enricher import FactorEnricher
    from apps.market_monitor.backend.factors.microstructure import MicrostructureProvider
    from apps.market_monitor.backend.factors.position_pressure import classify_position_pressure
    from apps.market_monitor.backend.factors.smart_money import (
        SmartMoneyProvider,
        build_smart_money_metrics,
    )
    from apps.market_monitor.backend.scoring.candidate_score import score_candidate
    from apps.market_monitor.backend.scoring.risk_score import score_risk


def _smart_pressure(**overrides):
    payload = {
        "data_available": True,
        "is_fresh": True,
        "trader_short_share": 0.80,
        "whale_short_share": 0.84,
        "trader_short_profit_ratio": 0.20,
        "trader_short_entry_gap_pct": 0.05,
        "trader_short_qty_change_pct_5m": 0.0,
        "trader_long_share": 0.20,
        "whale_long_share": 0.16,
        "trader_long_profit_ratio": 0.75,
        "trader_long_entry_gap_pct": -0.03,
        "trader_long_qty_change_pct_5m": 0.0,
        "flow_imbalance_30m": 0.30,
        "whale_flow_imbalance_30m": 0.20,
    }
    payload.update(overrides)
    return payload


def _derivatives_up(**overrides):
    payload = {
        "oi_amount_change_pct_15m": 0.02,
        "cvd_usdt_1m": 50_000.0,
        "cvd_usdt_3m": 120_000.0,
        "buy_aggressor_ratio_1m": 0.61,
        "trade_notional_usdt_1m": 500_000.0,
        "oi_usdt": 20_000_000.0,
    }
    payload.update(overrides)
    return payload


class SmartMoneyMetricTests(unittest.IsolatedAsyncioTestCase):
    async def test_background_collectors_share_runtime_dir_and_close_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            enricher = FactorEnricher(
                {
                    "runtime_dir": runtime_dir,
                    "binance": {
                        "enabled": False,
                        "microstructure": {
                            "enabled": True,
                            "global_liquidation_enabled": True,
                        },
                    },
                    "smart_money": {"enabled": True},
                }
            )
            enricher.start_background_tasks()
            self.assertEqual(enricher.smart_money_provider.state_path.parent, Path(runtime_dir))
            self.assertEqual(
                enricher.microstructure_provider.liquidation_history.state_path.parent,
                Path(runtime_dir),
            )
            self.assertTrue(Path(runtime_dir, "smart_money_state.json").exists())
            self.assertTrue(Path(runtime_dir, "liquidation_v2_state.json").exists())
            await enricher.close()

    def test_overview_fields_and_history_changes_are_normalized(self) -> None:
        now_ms = 2_000_000
        old_sample = {
            "symbol": "CELOUSDT",
            "observed_at_ms": now_ms - 5 * 60 * 1000,
            "overview": {
                "updateTime": 123,
                "totalTraders": 30,
                "longTraders": 10,
                "longTradersQty": 80,
                "longTradersAvgEntryPrice": 12,
                "shortTraders": 20,
                "shortTradersQty": 300,
                "shortTradersAvgEntryPrice": 11,
                "longWhales": 4,
                "longWhalesQty": 20,
                "longWhalesAvgEntryPrice": 12,
                "shortWhales": 8,
                "shortWhalesQty": 120,
                "shortWhalesAvgEntryPrice": 11,
                "longProfitTraders": 2,
                "shortProfitTraders": 15,
                "longProfitWhales": 1,
                "shortProfitWhales": 7,
            },
            "stats": {},
        }
        sample = {
            "symbol": "CELOUSDT",
            "observed_at_ms": now_ms,
            "overview": {
                **old_sample["overview"],
                "totalPositions": "400",
                "longShortRatio": "0.5",
                "longTradersQty": "100",
            },
            "stats": {
                "30m": {
                    "longPositions": "200",
                    "shortPositions": "600",
                    "longWhalePositions": "50",
                    "shortWhalePositions": "150",
                    "longTraders": 3,
                    "shortTraders": 7,
                }
            },
            "stats_requested": ["30m"],
            "source": "binance_smart_money_bapi",
        }
        metrics = build_smart_money_metrics(sample, price=10.0, history=[old_sample])
        self.assertTrue(metrics["data_available"])
        self.assertAlmostEqual(metrics["trader_long_share"], 0.25)
        self.assertAlmostEqual(metrics["trader_short_unrealized_pnl_usdt"], 300.0)
        self.assertAlmostEqual(metrics["trader_short_profit_ratio"], 0.75)
        self.assertAlmostEqual(metrics["flow_imbalance_30m"], -0.5)
        self.assertAlmostEqual(metrics["trader_long_qty_change_pct_5m"], 0.25)
        self.assertTrue(metrics["stats_complete"])

    async def test_cached_state_returns_independent_copies(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            provider = SmartMoneyProvider({"runtime_dir": runtime_dir})
            provider._cache["AAAUSDT"] = {"overview": {"totalTraders": 1}}
            provider._history["AAAUSDT"] = deque(
                [{"observed_at_ms": 1, "overview": {"longTradersQty": 1}}],
                maxlen=provider.max_history_samples,
            )
            cached, history = await provider._cached_state("AAAUSDT")
            cached["overview"]["totalTraders"] = 99
            history[0]["overview"]["longTradersQty"] = 99
            cached_again, history_again = await provider._cached_state("AAAUSDT")
            self.assertEqual(cached_again["overview"]["totalTraders"], 1)
            self.assertEqual(history_again[0]["overview"]["longTradersQty"], 1)


class PositionPressureClassifierTests(unittest.TestCase):
    def test_disconnected_liquidation_stream_is_not_treated_as_complete(self) -> None:
        pressure = classify_position_pressure(
            smart_money={},
            liquidation_v2={
                "tracking": False,
                "micro_short_liq_usdt_1m": 500_000.0,
            },
            derivatives=_derivatives_up(),
            latest={"direction": "up", "change_pct": 4.0},
            settings={"phase": "shadow"},
        )
        self.assertEqual(pressure["state"], "unknown")
        self.assertFalse(pressure["liquidation_v2_available"])
        self.assertFalse(pressure["data_valid"])

    def test_profitable_celo_shorts_are_position_control_not_squeeze(self) -> None:
        pressure = classify_position_pressure(
            smart_money=_smart_pressure(
                trader_short_share=0.77,
                whale_short_share=0.81,
                trader_short_profit_ratio=0.8367,
                trader_short_entry_gap_pct=-0.12,
                flow_imbalance_30m=-0.30,
            ),
            liquidation_v2={"tracking": True},
            derivatives=_derivatives_up(),
            latest={"direction": "up", "change_pct": 1.0},
            settings={"phase": "shadow"},
        )
        self.assertEqual(pressure["state"], "position_control")
        self.assertEqual(pressure["driver"], "profitable_shorts")
        self.assertFalse(pressure["confirmation_passed"])
        self.assertEqual(pressure["risk_modifier"], 0.0)

    def test_squeeze_lifecycle_and_downside_symmetry(self) -> None:
        pre_squeeze = classify_position_pressure(
            smart_money=_smart_pressure(),
            liquidation_v2={"tracking": True},
            derivatives=_derivatives_up(),
            latest={"direction": "up", "change_pct": 2.0},
            settings={"phase": "shadow"},
        )
        self.assertEqual(pre_squeeze["state"], "pre_squeeze")
        self.assertTrue(pre_squeeze["shadow_confirmation_passed"])
        self.assertFalse(pre_squeeze["confirmation_passed"])

        active = classify_position_pressure(
            smart_money=_smart_pressure(),
            liquidation_v2={
                "tracking": True,
                "micro_short_liq_usdt_1m": 80_000.0,
                "micro_long_liq_usdt_1m": 5_000.0,
            },
            derivatives=_derivatives_up(oi_amount_change_pct_15m=0.0),
            latest={"direction": "up", "change_pct": 3.0},
            settings={"phase": "shadow"},
        )
        self.assertEqual(active["state"], "active_squeeze")
        self.assertEqual(active["driver"], "short_cover")

        exhaustion = classify_position_pressure(
            smart_money=_smart_pressure(),
            liquidation_v2={
                "tracking": True,
                "micro_short_liq_usdt_1m": 80_000.0,
                "micro_long_liq_usdt_1m": 5_000.0,
            },
            derivatives=_derivatives_up(
                oi_amount_change_pct_15m=-0.02,
                cvd_usdt_1m=-10_000.0,
                cvd_usdt_3m=-20_000.0,
                buy_aggressor_ratio_1m=0.45,
            ),
            latest={"direction": "up", "change_pct": 7.0},
            settings={"phase": "shadow"},
        )
        self.assertEqual(exhaustion["state"], "exhaustion")

        downside = classify_position_pressure(
            smart_money=_smart_pressure(
                trader_long_share=0.76,
                whale_long_share=0.83,
                trader_long_profit_ratio=0.20,
                trader_long_entry_gap_pct=0.04,
            ),
            liquidation_v2={
                "tracking": True,
                "micro_short_liq_usdt_1m": 2_000.0,
                "micro_long_liq_usdt_1m": 90_000.0,
            },
            derivatives=_derivatives_up(
                oi_amount_change_pct_15m=0.0,
                cvd_usdt_1m=-50_000.0,
                cvd_usdt_3m=-100_000.0,
                buy_aggressor_ratio_1m=0.39,
            ),
            latest={"direction": "down", "change_pct": -3.0},
            settings={"phase": "shadow"},
        )
        self.assertEqual(downside["state"], "active_squeeze")
        self.assertEqual(downside["driver"], "long_liquidation")


class LiquidationCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_um_filter_dedupe_and_stream_status(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            provider = MicrostructureProvider(
                {
                    "runtime_dir": runtime_dir,
                    "liquidation_history": {
                        "runtime_dir": runtime_dir,
                        "save_interval_sec": 999,
                    },
                }
            )
            now_ms = int(time.time() * 1000)
            coin_m = {
                "_stream_symbol_type": 2,
                "s": "CELOUSD_PERP",
                "T": now_ms,
                "ap": "10",
                "z": "5",
                "S": "BUY",
            }
            usd_m = {
                "_stream_symbol_type": 1,
                "s": "CELOUSDT",
                "T": now_ms,
                "ap": "10",
                "z": "5",
                "S": "BUY",
            }
            await provider._touch("CELOUSDT")
            await provider._on_force_order(coin_m)
            await provider._on_force_order(usd_m)
            await provider._on_force_order(usd_m)
            self.assertEqual(provider.liquidation_history.summary()["event_count"], 1)
            provider._liquidation_stream_connected = True
            derivatives, sections = await provider._build_symbol_snapshot("CELOUSDT", context={})
            self.assertTrue(sections["v2"]["tracking"])
            self.assertEqual(sections["v2"]["micro_short_liq_usdt_1m"], 50.0)
            self.assertEqual(sections["legacy"]["micro_short_liq_usdt_1m"], 50.0)
            self.assertEqual(derivatives["micro_short_liq_usdt_1m"], 50.0)

    async def test_event_journal_recovers_rows_newer_than_state_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            settings = {
                "runtime_dir": runtime_dir,
                "save_interval_sec": 999,
                "retention_sec": 3600,
            }
            store = LiquidationHistoryStore(settings)
            now_ms = int(time.time() * 1000)
            self.assertTrue(
                store.add_event(
                    symbol="AAAUSDT",
                    event_ts_ms=now_ms,
                    short_liq_usdt=100,
                    long_liq_usdt=0,
                    side="BUY",
                    price=10,
                    qty=10,
                )
            )
            self.assertTrue(
                store.add_event(
                    symbol="AAAUSDT",
                    event_ts_ms=now_ms + 1,
                    short_liq_usdt=200,
                    long_liq_usdt=0,
                    side="BUY",
                    price=10,
                    qty=20,
                )
            )
            restored = LiquidationHistoryStore(settings)
            self.assertEqual(len(restored.events("AAAUSDT", now_ms=now_ms + 2)), 2)
            self.assertFalse(
                restored.add_event(
                    symbol="AAAUSDT",
                    event_ts_ms=now_ms + 1,
                    short_liq_usdt=200,
                    long_liq_usdt=0,
                    side="BUY",
                    price=10,
                    qty=20,
                )
            )


class ShadowCompatibilityTests(unittest.TestCase):
    def test_shadow_fields_do_not_change_score_risk_confirmation_or_tg(self) -> None:
        candidate = Candidate(candidate_id="AAAUSDT", symbol="AAAUSDT", base_asset="AAA")
        candidate.event_count = 2
        candidate.windows = ["1m", "5m"]
        candidate.latest_features = {
            "direction": "up",
            "change_pct": 3.0,
            "rvol": 2.0,
            "confidence": 70.0,
            "price": 10.0,
        }
        candidate.derivatives = {
            "oi_signal_level": "L2",
            "oi_change_pct_15m": 0.03,
            "taker_buy_ratio": 0.60,
            "buy_aggressor_ratio_1m": 0.62,
        }
        candidate.orderbook = {
            "imbalance": 0.10,
            "spread_bps": 5.0,
            "bid_notional": 100_000.0,
            "ask_notional": 90_000.0,
        }
        candidate.liquidation = {"short_liq_usdt": 30_000.0}
        score_before = score_candidate(candidate)
        risk_before = score_risk(candidate, {})
        confirmation_before = evaluate_trade_confirmation(candidate, {}).to_dict()

        candidate.smart_money = _smart_pressure()
        candidate.liquidation_v2 = {
            "tracking": True,
            "micro_short_liq_usdt_1m": 500_000.0,
        }
        candidate.position_pressure = classify_position_pressure(
            smart_money=candidate.smart_money,
            liquidation_v2=candidate.liquidation_v2,
            derivatives=_derivatives_up(),
            latest=candidate.latest_features,
            settings={
                "phase": "shadow",
                "display_enabled": False,
                "risk_enabled": False,
                "confirmation_enabled": False,
            },
        )
        self.assertEqual(score_before, score_candidate(candidate))
        self.assertEqual(risk_before, score_risk(candidate, {}))
        self.assertEqual(confirmation_before, evaluate_trade_confirmation(candidate, {}).to_dict())
        self.assertEqual(_position_pressure_line(candidate.position_pressure), "")

    def test_readiness_requires_time_samples_and_source_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            recorder = PositionPressureShadowRecorder(
                {
                    "runtime_dir": runtime_dir,
                    "position_pressure_review_min_days": 0,
                    "position_pressure_review_min_first_push_samples": 2,
                    "position_pressure_review_min_smart_money_coverage_pct": 50,
                    "position_pressure_review_min_liquidation_coverage_pct": 100,
                }
            )
            base = {
                "alert_type": "actionable_alert",
                "position_pressure": {
                    "state": "crowded",
                    "data_valid": True,
                    "smart_money_available": True,
                    "liquidation_v2_available": True,
                },
            }
            recorder.record({**base, "symbol": "AAAUSDT"}, alert_sent=True)
            recorder.record({**base, "symbol": "AAAUSDT"}, alert_sent=True)
            second = {
                **base,
                "symbol": "BBBUSDT",
                "position_pressure": {
                    **base["position_pressure"],
                    "smart_money_available": False,
                },
            }
            summary = recorder.record(second, alert_sent=True)
            self.assertEqual(summary["valid_first_push_samples"], 2)
            self.assertEqual(summary["coverage_pct"]["smart_money"], 50.0)
            self.assertEqual(summary["coverage_pct"]["liquidation_v2"], 100.0)
            self.assertEqual(summary["review_gate"]["status"], "READY_FOR_REVIEW")
            self.assertTrue(Path(runtime_dir, "position_pressure_readiness.json").exists())

    def test_missing_first_push_stays_in_coverage_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            os.environ,
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            recorder = PositionPressureShadowRecorder(
                {
                    "runtime_dir": runtime_dir,
                    "position_pressure_review_min_days": 0,
                    "position_pressure_review_min_first_push_samples": 1,
                    "position_pressure_review_min_smart_money_coverage_pct": 50,
                    "position_pressure_review_min_liquidation_coverage_pct": 50,
                }
            )
            missing = {
                "symbol": "AAAUSDT",
                "alert_type": "startup_alert",
                "position_pressure": {
                    "state": "unknown",
                    "data_valid": False,
                    "smart_money_available": False,
                    "liquidation_v2_available": False,
                },
            }
            recorder.record(missing, alert_sent=True)
            recorder.record(
                {
                    **missing,
                    "position_pressure": {
                        "state": "crowded",
                        "data_valid": True,
                        "smart_money_available": True,
                        "liquidation_v2_available": True,
                    },
                },
                alert_sent=True,
            )
            summary = recorder.record(
                {
                    **missing,
                    "symbol": "BBBUSDT",
                    "position_pressure": {
                        "state": "crowded",
                        "data_valid": True,
                        "smart_money_available": True,
                        "liquidation_v2_available": True,
                    },
                },
                alert_sent=True,
            )
            self.assertEqual(summary["first_push_samples"], 2)
            self.assertEqual(summary["valid_first_push_samples"], 1)
            self.assertEqual(summary["coverage_pct"]["smart_money"], 50.0)
            self.assertEqual(summary["coverage_pct"]["liquidation_v2"], 50.0)
            self.assertEqual(summary["review_gate"]["status"], "READY_FOR_REVIEW")

    def test_production_config_is_shadow_only(self) -> None:
        backend_root = Path(__file__).resolve().parents[1]
        config_path = backend_root / "config" / "config.json"
        if not config_path.exists():
            config_path = backend_root.parent / "config" / "config.json"
        config = load_config(str(config_path))
        strategy = config["alert_strategy"]
        pressure = strategy["confirmation_factors"]["position_pressure"]
        micro = strategy["confirmation_factors"]["binance"]["microstructure"]
        self.assertTrue(strategy["position_pressure_shadow_enabled"])
        self.assertFalse(strategy["position_pressure_risk_enabled"])
        self.assertFalse(strategy["position_pressure_confirmation_enabled"])
        self.assertEqual(pressure["phase"], "shadow")
        self.assertFalse(pressure["display_enabled"])
        self.assertFalse(pressure["risk_enabled"])
        self.assertFalse(pressure["confirmation_enabled"])
        self.assertTrue(micro["global_liquidation_enabled"])
        self.assertFalse(micro["liquidation_decision_enabled"])
        self.assertFalse(strategy["confirmation_factors"]["binance"]["fetch_liquidations"])


if __name__ == "__main__":
    unittest.main()
