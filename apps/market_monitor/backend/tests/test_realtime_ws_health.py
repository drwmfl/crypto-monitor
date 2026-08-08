from __future__ import annotations

import json
import time
import unittest

_IMPORT_ERROR = ""
try:
    from apps.market_monitor.backend.realtime_ws import (
        RealtimeKlineWatcher,
        RealtimeSubscriptionError,
    )
except ModuleNotFoundError as app_import_error:
    try:
        from realtime_ws import RealtimeKlineWatcher, RealtimeSubscriptionError
    except ModuleNotFoundError:
        RealtimeKlineWatcher = None
        RealtimeSubscriptionError = RuntimeError
        _IMPORT_ERROR = str(app_import_error)


class _FakeFeed:
    def __init__(self) -> None:
        self.symbol_mapping = {
            "BTCUSDT": "BTC/USDT:USDT",
            "ETHUSDT": "ETH/USDT:USDT",
        }
        self.marked = []

    async def refresh_universe_if_due(self, force: bool = False) -> None:
        return None

    def mark_ws_kline(self, symbol, window, *, candle_start_ms, is_closed) -> None:
        self.marked.append((symbol, window, candle_start_ms, is_closed))


def _watcher() -> RealtimeKlineWatcher:
    watcher = RealtimeKlineWatcher.__new__(RealtimeKlineWatcher)
    watcher.enabled = True
    watcher.windows = ["1m"]
    watcher.no_message_reconnect_sec = 45.0
    watcher.sub_chunk_size = 1
    watcher.symbol_refresh_sec = 60
    watcher.skip_poll_windows = True
    watcher.local_agg_enabled = False
    watcher.local_agg_windows = []
    watcher.local_agg_skip_poll_windows = True
    watcher.prefilter_pct_by_window = {"1m": 999.0}
    watcher.feed = _FakeFeed()
    watcher._request_id = 1
    watcher._last_sub_refresh_ts = 0.0
    watcher._last_data_msg_ts = 0.0
    watcher._last_valid_kline_msg_ts = 0.0
    watcher._connection_message_count = 0
    watcher._connection_kline_count = 0
    watcher._requested_subscriptions = set()
    watcher._confirmed_subscriptions = set()
    watcher._pending_subscription_requests = {}
    watcher._last_subscription_error = ""
    watcher._check_state = {}
    watcher._agg_bootstrap_ready = set()
    return watcher


@unittest.skipIf(RealtimeKlineWatcher is None, f"production dependencies unavailable: {_IMPORT_ERROR}")
class RealtimeWebSocketHealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_subscription_batches_are_tracked_and_acknowledged(self) -> None:
        watcher = _watcher()
        sent = []

        async def send_payload(payload):
            sent.append(payload)

        requested = await watcher._sync_subscriptions(send_payload, set(), force=True)

        self.assertEqual(len(requested), 2)
        self.assertEqual(len(sent), 2)
        self.assertEqual(len(watcher._pending_subscription_requests), 2)
        for payload in sent:
            await watcher._handle_message(json.dumps({"result": None, "id": payload["id"]}))

        self.assertEqual(watcher._confirmed_subscriptions, requested)
        self.assertEqual(watcher._pending_subscription_requests, {})

    async def test_subscription_rejection_forces_reconnect(self) -> None:
        watcher = _watcher()
        request = watcher._subscription_request("SUBSCRIBE", ["btcusdt@kline_1m"])

        with self.assertRaises(RealtimeSubscriptionError):
            await watcher._handle_message(
                json.dumps({"code": 2, "msg": "invalid stream", "id": request["id"]})
            )

        self.assertIn("invalid stream", watcher._last_subscription_error)

    def test_subscription_ack_timeout_forces_reconnect(self) -> None:
        watcher = _watcher()
        request = watcher._subscription_request("SUBSCRIBE", ["btcusdt@kline_1m"])
        watcher._pending_subscription_requests[request["id"]]["sent_at"] = time.time() - 30

        with self.assertRaises(RealtimeSubscriptionError):
            watcher._raise_if_subscription_ack_stalled()

    async def test_valid_combined_kline_marks_connection_healthy(self) -> None:
        watcher = _watcher()
        raw = json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "s": "BTCUSDT",
                    "k": {
                        "s": "BTCUSDT",
                        "i": "1m",
                        "t": 1_786_176_000_000,
                        "T": 1_786_176_059_999,
                        "o": "100.0",
                        "h": "101.0",
                        "l": "99.0",
                        "c": "100.5",
                        "v": "12.0",
                        "x": False,
                    },
                },
            }
        )

        await watcher._handle_message(raw)

        health = watcher.data_health_snapshot()
        self.assertTrue(health["healthy"])
        self.assertEqual(health["received_messages"], 1)
        self.assertEqual(health["received_klines"], 1)
        self.assertEqual(len(watcher.feed.marked), 1)

    async def test_new_connection_resets_health_until_first_kline(self) -> None:
        watcher = _watcher()
        watcher._last_valid_kline_msg_ts = time.time()
        watcher._connection_kline_count = 10
        watcher._confirmed_subscriptions.add("btcusdt@kline_1m")

        watcher._reset_connection_state()

        health = watcher.data_health_snapshot()
        self.assertFalse(health["healthy"])
        self.assertEqual(health["received_klines"], 0)
        self.assertEqual(health["confirmed_subscriptions"], 0)

    def test_local_aggregation_keeps_polling_until_each_window_is_ready(self) -> None:
        watcher = _watcher()
        watcher.local_agg_enabled = True
        watcher.local_agg_windows = ["15m", "30m"]
        watcher._last_valid_kline_msg_ts = time.time()
        watcher._agg_bootstrap_ready = {
            ("BTCUSDT", "15m"),
            ("ETHUSDT", "15m"),
            ("BTCUSDT", "30m"),
        }

        skipped = watcher.poll_windows_to_skip(require_healthy=True)

        self.assertEqual(skipped, {"1m", "15m"})
        health = watcher.data_health_snapshot()
        self.assertEqual(health["local_agg_target_count"], 2)
        self.assertEqual(health["local_agg_ready"], {"15m": 2, "30m": 1})


if __name__ == "__main__":
    unittest.main()
