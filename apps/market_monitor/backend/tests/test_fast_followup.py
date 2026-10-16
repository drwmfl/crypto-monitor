from __future__ import annotations

import unittest

try:
    from alerts.fast_followup import format_fast_followup, summarize_followup
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.fast_followup import format_fast_followup, summarize_followup


BARS_UP_THEN_FADE = [
    [0, 100.0, 110.0, 99.5, 108.0],
    [60000, 108.0, 109.0, 96.0, 97.0],
]


class SummarizeFollowUpTests(unittest.TestCase):
    def test_up_alert_that_faded_is_reversed(self) -> None:
        summary = summarize_followup(BARS_UP_THEN_FADE, entry_price=100.0, direction="up")

        self.assertEqual(summary["status"], "reversed")
        self.assertAlmostEqual(summary["change_pct"], -3.0)
        self.assertAlmostEqual(summary["high_pct"], 10.0)
        self.assertAlmostEqual(summary["low_pct"], -4.0)

    def test_down_alert_that_kept_falling_is_continued(self) -> None:
        summary = summarize_followup(BARS_UP_THEN_FADE, entry_price=100.0, direction="down")

        self.assertEqual(summary["status"], "continued")

    def test_small_move_is_flat(self) -> None:
        summary = summarize_followup([[0, 100.0, 101.0, 99.0, 100.5]], entry_price=100.0, direction="up", hold_pct=1.0)

        self.assertEqual(summary["status"], "flat")

    def test_no_bars_returns_none(self) -> None:
        self.assertIsNone(summarize_followup([], entry_price=100.0, direction="up"))

    def test_spike_that_gave_back_most_gains_is_faded(self) -> None:
        # CTK 2026-10-12: alerted at 0.1369, ran to 0.1846, back to 0.1384 after 15 minutes.
        bars = [[0, 0.1371, 0.1650, 0.1366, 0.1567], [60000, 0.1717, 0.1846, 0.1344, 0.1384]]
        summary = summarize_followup(bars, entry_price=0.1369, direction="up")
        message = format_fast_followup(token="CTK", symbol="CTKUSDT", direction="up", minutes=15, summary=summary)

        self.assertEqual(summary["status"], "faded")
        self.assertIn("**⏱ 急速异动跟进 | CTK（15分钟后）**", message)
        self.assertIn("推送价 0.1369 → 现价 0.1384（+1.10%）", message)
        self.assertIn("⚠️ 状态：冲高回落（期间最多涨 34.84%，已回吐过半）", message)
        self.assertIn("https://www.binance.com/futures/CTKUSDT", message)


if __name__ == "__main__":
    unittest.main()
