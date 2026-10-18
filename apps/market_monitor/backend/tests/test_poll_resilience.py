from __future__ import annotations

import asyncio
import logging
import time
import unittest

try:
    from alert_monitor import _poll_batch_size, _setup_logging
    from data_feed import BinanceKlineDataFeed
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_monitor import _poll_batch_size, _setup_logging
    from apps.market_monitor.backend.data_feed import BinanceKlineDataFeed


class PollBatchSizeTests(unittest.TestCase):
    KW = {
        "full_scan_windows": {"1m", "5m"},
        "batch_size_by_window": {"15m": 60, "30m": 40},
        "default_batch_size": 25,
        "fallback_batch_size": 60,
    }

    def test_fallback_window_rotates_in_batches_instead_of_full_scan(self) -> None:
        self.assertEqual(_poll_batch_size("1m", fallback_windows={"1m", "5m"}, **self.KW), 60)

    def test_normal_windows_keep_their_configured_batch(self) -> None:
        self.assertEqual(_poll_batch_size("1m", fallback_windows=set(), **self.KW), 0)
        self.assertEqual(_poll_batch_size("30m", fallback_windows=set(), **self.KW), 40)
        self.assertEqual(_poll_batch_size("1h", fallback_windows=set(), **self.KW), 25)

    def test_zero_fallback_batch_keeps_old_behaviour(self) -> None:
        kw = dict(self.KW, fallback_batch_size=0)
        self.assertEqual(_poll_batch_size("1m", fallback_windows={"1m"}, **kw), 0)


class SnapshotTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_stuck_request_times_out_instead_of_blocking_the_cycle(self) -> None:
        feed = BinanceKlineDataFeed.__new__(BinanceKlineDataFeed)
        feed._semaphore = asyncio.Semaphore(2)
        feed.snapshot_timeout_sec = 0.2
        feed._is_temporarily_banned = lambda: False
        feed.mark_scan_attempt = lambda *a, **k: None
        results = []
        feed.mark_scan_result = lambda *a, **k: results.append(k)
        feed._fetch_snapshot_sync = lambda *a, **k: time.sleep(2)

        started = time.monotonic()
        with self.assertLogs("data_feed", level="WARNING") as logs:
            snapshot = await feed.fetch_snapshot("XUSDT", "X/USDT:USDT", "1m")

        self.assertIsNone(snapshot)
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertEqual(results[-1]["error"], "TimeoutError")
        self.assertTrue(any("XUSDT" in line for line in logs.output))


class LoggingTests(unittest.TestCase):
    def test_httpx_request_urls_are_not_logged_at_info(self) -> None:
        _setup_logging("INFO")

        self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertEqual(logging.getLogger("httpcore").level, logging.WARNING)


if __name__ == "__main__":
    unittest.main()
