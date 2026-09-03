from __future__ import annotations

import unittest

try:
    from notifier import AlertNotifier
except ModuleNotFoundError:
    from apps.market_monitor.backend.notifier import AlertNotifier


class DailyPushTitleTests(unittest.TestCase):
    def test_first_push_adds_first_marker_after_daily_count(self) -> None:
        message = "**🚀 启动预警 | SOXS（触发1次）**\n正文"

        updated = AlertNotifier._apply_daily_push_title(message, 1)

        self.assertEqual(updated, "**🚀 启动预警 | SOXS（今日第1次推送）1️⃣**\n正文")

    def test_later_push_does_not_add_warning(self) -> None:
        message = "**💎 高价值候选 | SOXS（触发3次）**\n正文"

        updated = AlertNotifier._apply_daily_push_title(message, 2)

        self.assertEqual(updated, "**💎 高价值候选 | SOXS（今日第2次推送）**\n正文")

    def test_reformatting_does_not_duplicate_or_retain_first_marker(self) -> None:
        first = "**🚀 启动预警 | SOXS（今日第1次推送）1️⃣**\n正文"

        repeated_first = AlertNotifier._apply_daily_push_title(first, 1)
        second = AlertNotifier._apply_daily_push_title(first, 2)

        self.assertEqual(repeated_first.count("1️⃣"), 1)
        self.assertEqual(second, "**🚀 启动预警 | SOXS（今日第2次推送）**\n正文")

    def test_old_warning_marker_is_migrated(self) -> None:
        old = "**🚀 启动预警 | SOXS（今日第1次推送）⚠️**\n正文"

        updated = AlertNotifier._apply_daily_push_title(old, 1)

        self.assertEqual(updated, "**🚀 启动预警 | SOXS（今日第1次推送）1️⃣**\n正文")


if __name__ == "__main__":
    unittest.main()
