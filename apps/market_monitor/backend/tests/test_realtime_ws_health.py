from __future__ import annotations

import asyncio
import json
import time
import unittest
from types import SimpleNamespace

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
    watcher.processing_concurrency = 1
    watcher.max_pending_evaluations = 2
    watcher._evaluation_semaphore = asyncio.Semaphore(1)
    watcher._evaluation_tasks = {}
    watcher._dropped_evaluations = 0
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

    async def test_slow_evaluation_does_not_block_receive_loop_and_is_deduplicated(self) -> None:
        watcher = _watcher()
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_evaluation(**_kwargs) -> None:
            started.set()
            await release.wait()

        watcher._evaluate_realtime_kline = slow_evaluation
        kwargs = {
            "symbol": "BTCUSDT",
            "window": "1m",
            "candle_start_ms": 1,
            "candle_close_ms": 2,
            "is_closed": False,
            "open_price": 100.0,
            "close_price": 101.0,
            "volume": 1.0,
        }

        self.assertTrue(watcher._schedule_realtime_evaluation(**kwargs))
        await asyncio.wait_for(started.wait(), timeout=1.0)
        self.assertFalse(watcher._schedule_realtime_evaluation(**kwargs))
        self.assertEqual(len(watcher._evaluation_tasks), 1)

        release.set()
        await asyncio.gather(*list(watcher._evaluation_tasks.values()))
        await asyncio.sleep(0)
        self.assertEqual(watcher._evaluation_tasks, {})

    async def test_evaluation_backlog_is_bounded(self) -> None:
        watcher = _watcher()
        watcher.max_pending_evaluations = 1
        release = asyncio.Event()

        async def slow_evaluation(**_kwargs) -> None:
            await release.wait()

        watcher._evaluate_realtime_kline = slow_evaluation
        base = {
            "window": "1m",
            "candle_start_ms": 1,
            "candle_close_ms": 2,
            "is_closed": False,
            "open_price": 100.0,
            "close_price": 101.0,
            "volume": 1.0,
        }
        self.assertTrue(watcher._schedule_realtime_evaluation(symbol="BTCUSDT", **base))
        self.assertFalse(watcher._schedule_realtime_evaluation(symbol="ETHUSDT", **base))
        self.assertEqual(watcher._dropped_evaluations, 1)

        release.set()
        await asyncio.gather(*list(watcher._evaluation_tasks.values()))

    async def test_local_aggregation_runs_outside_ws_message_handler(self) -> None:
        watcher = _watcher()
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_aggregation(**_kwargs) -> None:
            started.set()
            await release.wait()

        watcher._on_closed_1m_candle = slow_aggregation
        watcher._schedule_closed_1m_aggregation(
            symbol="BTCUSDT",
            candle_start_ms=1,
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            volume=1.0,
        )

        await asyncio.wait_for(started.wait(), timeout=1.0)
        self.assertEqual(len(watcher._local_agg_tasks), 1)
        release.set()
        await asyncio.gather(*list(watcher._local_agg_tasks))
        self.assertFalse(watcher._local_agg_tasks)

    async def test_one_minute_extreme_fast_path_uses_ws_without_rest_snapshot(self) -> None:
        watcher = _watcher()
        watcher.prefilter_pct_by_window = {"1m": 999.0}
        watcher._realtime_1m_volume_history = {"BTCUSDT": [10.0] * 20}
        watcher._realtime_1m_last_closed_start = {}
        watcher._fast_check_state = {}
        watcher._fast_evaluation_tasks = {}
        received = []

        class _Strategy:
            enabled = True
            direct_tg_enabled = False

            @staticmethod
            def _is_extreme_fast_event(payload):
                return abs(float(payload.get("change_pct", 0.0))) >= 8.0 and float(payload.get("rvol") or 0.0) >= 4.0

            async def process_event(self, payload, *, source, notifier):
                received.append((payload, source, notifier))

        watcher.strategy = _Strategy()
        watcher.notifier = object()
        await watcher._handle_message(json.dumps({
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "i": "1m",
                "t": 1,
                "T": 60000,
                "o": "100",
                "h": "109",
                "l": "99",
                "c": "109",
                "v": "50",
                "x": True,
            },
        }))
        await asyncio.gather(*list(watcher._fast_evaluation_tasks.values()))

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0][0]["window"], "1m")
        self.assertEqual(received[0][1], "ws")

    async def test_one_minute_fast_payload_carries_runup_from_recent_low(self) -> None:
        watcher = _watcher()
        watcher._realtime_1m_volume_history = {"BTCUSDT": [10.0] * 20}
        watcher._realtime_1m_range_history = {"BTCUSDT": [(-120000, 95.0, 90.0), (-60000, 101.0, 94.0)]}
        watcher._realtime_1m_last_closed_start = {}
        watcher._fast_check_state = {}
        watcher._fast_evaluation_tasks = {}
        received = []

        class _Strategy:
            enabled = True
            direct_tg_enabled = False

            @staticmethod
            def _is_extreme_fast_event(payload):
                return True

            async def process_event(self, payload, *, source, notifier):
                received.append(payload)

        watcher.strategy = _Strategy()
        watcher.notifier = object()
        await watcher._handle_message(json.dumps({
            "e": "kline",
            "s": "BTCUSDT",
            "k": {"i": "1m", "t": 1, "T": 60000, "o": "100", "h": "109", "l": "99", "c": "108", "v": "50", "x": True},
        }))
        await asyncio.gather(*list(watcher._fast_evaluation_tasks.values()))

        self.assertAlmostEqual(received[0]["runup_pct"], 20.0)
        self.assertEqual(received[0]["runup_minutes"], 3)
        self.assertEqual(watcher._realtime_1m_range_history["BTCUSDT"][-1], (1, 109.0, 99.0))

    async def test_one_minute_message_feeds_discovery_shadow(self) -> None:
        watcher = _watcher()
        watcher._realtime_1m_volume_history = {"BTCUSDT": [10.0] * 20}
        watcher._realtime_1m_range_history = {"BTCUSDT": [(-60000, 101.0, 90.0)]}
        watcher._realtime_1m_last_closed_start = {}
        watcher._fast_check_state = {}
        watcher._fast_evaluation_tasks = {}
        watcher.strategy = None
        observed = []

        pushed = []

        class _Discovery:
            enabled = True

            @staticmethod
            def observe(**kwargs):
                observed.append(kwargs)
                return [{"symbol": "BTCUSDT", "mode": "live"}, {"symbol": "BTCUSDT", "mode": "shadow"}]

        watcher.discovery = _Discovery()
        watcher.discovery_pusher = SimpleNamespace(push=pushed.append)
        watcher.feed.symbol_instrument_metadata = {"BTCUSDT": {"instrument_type": "crypto"}}
        await watcher._handle_message(json.dumps({
            "e": "kline",
            "s": "BTCUSDT",
            "k": {"i": "1m", "t": 1, "T": 60000, "o": "100", "h": "104", "l": "99", "c": "103", "v": "30", "x": False},
        }))

        self.assertEqual(len(observed), 1)
        self.assertAlmostEqual(observed[0]["change_1m_pct"], 3.0)
        self.assertAlmostEqual(observed[0]["runup_up_pct"], (103 / 90 - 1) * 100)
        self.assertAlmostEqual(observed[0]["runup_down_pct"], (103 / 104 - 1) * 100)
        self.assertAlmostEqual(observed[0]["rvol"], 3.0)
        self.assertEqual(observed[0]["runup_minutes"], 2)
        self.assertFalse(observed[0]["is_closed"])
        self.assertEqual(observed[0]["instrument_type"], "crypto")
        self.assertEqual(pushed, [{"symbol": "BTCUSDT", "mode": "live"}])

    def test_runup_for_down_move_uses_recent_high(self) -> None:
        watcher = _watcher()
        watcher._realtime_1m_range_history = {"BTCUSDT": [(1, 120.0, 110.0)]}

        runup, minutes = watcher._realtime_runup("BTCUSDT", direction="down", price=90.0, current_high=100.0)

        self.assertAlmostEqual(runup, -25.0)
        self.assertEqual(minutes, 2)

    async def test_closed_one_minute_candle_is_aggregated_before_change_prefilter(self) -> None:
        watcher = _watcher()
        watcher.local_agg_enabled = True
        watcher.local_agg_windows = ["5m"]
        watcher.prefilter_pct_by_window = {"1m": 999.0}
        started = asyncio.Event()

        async def record_aggregation(**_kwargs) -> None:
            started.set()

        watcher._on_closed_1m_candle = record_aggregation
        await watcher._handle_message(json.dumps({
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "i": "1m",
                "t": 1,
                "T": 60000,
                "o": "100",
                "h": "101",
                "l": "99",
                "c": "100.5",
                "v": "10",
                "x": True,
            },
        }))

        await asyncio.wait_for(started.wait(), timeout=1.0)

    def test_five_minute_local_bucket_closes_on_fifth_one_minute_candle(self) -> None:
        watcher = _watcher()
        watcher._agg_state = {}
        rows = []
        for minute in range(5):
            row = [minute * 60000, 100.0, 101.0 + minute, 99.0, 100.0 + minute, 10.0]
            closed = watcher._update_local_agg_bucket("BTCUSDT", "5m", row)
            rows.append(closed)

        self.assertEqual(sum(item is not None for item in rows), 1)
        self.assertEqual(rows[-1][0], 0)
        self.assertEqual(rows[-1][4], 104.0)
        self.assertEqual(rows[-1][5], 50.0)

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
