from __future__ import annotations

import unittest

try:
    from alerts.alert_policy import AlertDecision
    from alerts.tg_formatter import format_strategy_alert
    from candidates.candidate_models import Candidate
    from candidates.raw_event import RawEvent
    from instrument_type import binance_instrument_metadata
    from notifier import AlertNotifier
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.alert_policy import AlertDecision
    from apps.market_monitor.backend.alerts.tg_formatter import format_strategy_alert
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.candidates.raw_event import RawEvent
    from apps.market_monitor.backend.instrument_type import binance_instrument_metadata
    from apps.market_monitor.backend.notifier import AlertNotifier


class BinanceInstrumentTypeTests(unittest.TestCase):
    def test_equity_variants_are_classified_as_stock(self) -> None:
        for underlying_type in ("EQUITY", "HK_EQUITY", "KR_EQUITY"):
            with self.subTest(underlying_type=underlying_type):
                metadata = binance_instrument_metadata(
                    {
                        "info": {
                            "contractType": "TRADIFI_PERPETUAL",
                            "underlyingType": underlying_type,
                            "underlyingSubType": ["TradFi"],
                        }
                    }
                )
                self.assertEqual(metadata["instrument_type"], "stock")

    def test_crypto_and_commodity_are_not_classified_as_stock(self) -> None:
        crypto = binance_instrument_metadata(
            {"info": {"contractType": "PERPETUAL", "underlyingType": "COIN"}}
        )
        commodity = binance_instrument_metadata(
            {"info": {"contractType": "TRADIFI_PERPETUAL", "underlyingType": "COMMODITY"}}
        )

        self.assertEqual(crypto["instrument_type"], "crypto")
        self.assertEqual(commodity["instrument_type"], "commodity")

    def test_stock_metadata_reaches_strategy_title(self) -> None:
        event = RawEvent.from_alert_payload(
            {
                "symbol": "SOXSUSDT",
                "rule_name": "early_start",
                "window": "1m",
                "direction": "up",
                "level": "medium",
                "change_pct": 2.0,
                "price": 25.0,
                "rvol": 2.0,
                "instrument_type": "stock",
                "instrument_contract_type": "TRADIFI_PERPETUAL",
                "instrument_underlying_type": "EQUITY",
                "instrument_underlying_subtypes": ["TradFi", "ETF"],
            },
            source="ws",
        )
        candidate = Candidate.from_raw_event(event)
        candidate.score = 70.0
        candidate.risk_score = 30.0

        message = format_strategy_alert(
            candidate,
            AlertDecision(True, alert_type="startup_alert", reason="accepted"),
        )

        self.assertEqual(
            message.splitlines()[0],
            "**🚀 启动预警 | SOXS（触发1次） | 📊 股票合约**",
        )
        final_message = AlertNotifier._apply_daily_push_title(message, 1)
        self.assertEqual(
            final_message.splitlines()[0],
            "**🚀 启动预警 | SOXS（今日第1次推送）⚠️ | 📊 股票合约**",
        )


if __name__ == "__main__":
    unittest.main()
