from __future__ import annotations

import json
import unittest
from pathlib import Path

try:
    from apps.market_monitor.backend.alert_config import DEFAULT_CONFIG
    from apps.market_monitor.backend.binance_ws_routes import (
        FUTURES_MARKET_STREAM_URL,
        FUTURES_TICKER_MARK_PRICE_STREAM_URL,
        combined_market_stream_url,
        normalize_market_stream_url,
    )
    from apps.market_monitor.backend.stream_collector import BINANCE_WS_URL
except ModuleNotFoundError:
    from alert_config import DEFAULT_CONFIG
    from binance_ws_routes import (
        FUTURES_MARKET_STREAM_URL,
        FUTURES_TICKER_MARK_PRICE_STREAM_URL,
        combined_market_stream_url,
        normalize_market_stream_url,
    )
    from stream_collector import BINANCE_WS_URL


class BinanceWebSocketRouteTests(unittest.TestCase):
    def test_realtime_defaults_use_routed_market_endpoint(self) -> None:
        self.assertEqual(
            DEFAULT_CONFIG["data_feed"]["ws_realtime_url"],
            FUTURES_MARKET_STREAM_URL,
        )

        config_path = Path(__file__).resolve().parents[2] / "config" / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(config["data_feed"]["ws_realtime_url"], FUTURES_MARKET_STREAM_URL)

    def test_stream_collector_uses_routed_market_combined_stream(self) -> None:
        self.assertEqual(
            FUTURES_TICKER_MARK_PRICE_STREAM_URL,
            combined_market_stream_url(("!ticker@arr", "!markPrice@arr")),
        )
        self.assertEqual(BINANCE_WS_URL, FUTURES_TICKER_MARK_PRICE_STREAM_URL)
        self.assertTrue(
            FUTURES_TICKER_MARK_PRICE_STREAM_URL.startswith(
                f"{FUTURES_MARKET_STREAM_URL}?streams="
            )
        )

    def test_empty_combined_stream_falls_back_to_market_endpoint(self) -> None:
        self.assertEqual(combined_market_stream_url([]), FUTURES_MARKET_STREAM_URL)

    def test_legacy_market_routes_are_normalized(self) -> None:
        self.assertEqual(
            normalize_market_stream_url("wss://fstream.binance.com/ws"),
            FUTURES_MARKET_STREAM_URL,
        )
        self.assertEqual(
            normalize_market_stream_url(
                "wss://fstream.binance.com/stream?streams=!ticker@arr"
            ),
            "wss://fstream.binance.com/market/stream?streams=!ticker@arr",
        )
        self.assertEqual(
            normalize_market_stream_url(
                "wss://fstream.binance.com/ws/btcusdt@kline_1m"
            ),
            "wss://fstream.binance.com/market/ws/btcusdt@kline_1m",
        )


if __name__ == "__main__":
    unittest.main()
