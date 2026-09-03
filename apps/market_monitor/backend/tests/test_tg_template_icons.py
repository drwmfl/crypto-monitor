from __future__ import annotations

import unittest
from pathlib import Path

try:
    from alerts.alert_policy import AlertDecision
    from alerts.tg_formatter import format_strategy_alert
    from candidates.candidate_models import Candidate
    from notifier import AlertEvent, AlertNotifier
    from run_accumulation_pool_scheduler import _build_failure_message, _build_summary_message
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.alert_policy import AlertDecision
    from apps.market_monitor.backend.alerts.tg_formatter import format_strategy_alert
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.notifier import AlertEvent, AlertNotifier
    from apps.market_monitor.backend.run_accumulation_pool_scheduler import (
        _build_failure_message,
        _build_summary_message,
    )


class StrategyTemplateIconTests(unittest.TestCase):
    @staticmethod
    def _candidate() -> Candidate:
        return Candidate(
            candidate_id="TAKEUSDT",
            symbol="TAKEUSDT",
            base_asset="TAKE",
            event_count=2,
            windows=["1m", "5m"],
            score=100.0,
            risk_score=32.5,
            latest_features={
                "direction": "up",
                "window": "1m",
                "price": 0.07239,
                "change_pct": 4.17,
                "rvol": 7.2,
                "source": "ws",
                "event_type": "early_start",
                "startup_window": "15m",
                "startup_change_pct": 6.52,
                "startup_rvol": 7.2,
                "startup_breakout_distance_pct": 0.64,
            },
            derivatives={
                "oi_usdt": 8_090_000,
                "oi_change_pct_5m": 0.048,
                "oi_change_pct_15m": 0.0772,
                "oi_change_pct_1h": 0.084,
                "oi_change_pct_4h": 0.0398,
                "oi_signal_level": "L3",
                "oi_regime": "new_longs",
                "micro_signal_level": "L1",
                "micro_regime": "breakout_continuation",
                "cvd_usdt_1m": -8_600,
                "cvd_usdt_3m": 11_800,
                "cvd_usdt_5m": -81_600,
                "buy_aggressor_ratio_1m": 0.2748,
                "buy_aggressor_ratio_3m": 0.5564,
                "buy_aggressor_ratio_5m": 0.3929,
                "micro_liq_imbalance_usdt_1m": 0,
                "taker_buy_ratio": 0.5558,
                "funding_rate": 0.0001,
                "oi_reason": "OI连续增加",
                "micro_reason": "主动买盘增强",
            },
            factor_snapshot={
                "factor_completeness": {
                    "available": 6,
                    "total": 6,
                    "pct": 100.0,
                    "statuses": {"liquidation": "none_recent"},
                }
            },
            confirmation={
                "checks": [
                    {
                        "key": "price_persistence",
                        "passed": True,
                        "value": {"event_count": 2, "windows": ["1m", "5m"]},
                    },
                    {"key": "flow", "passed": True},
                    {"key": "orderbook", "passed": False},
                ]
            },
            risk_breakdown={
                "overheat": 20.85,
                "derivatives_risk": 9.0,
                "weak_confirmation": 3.0,
                "raw": {"rvol": 7.2},
            },
            position_pressure={
                "display_enabled": True,
                "display_allowed": True,
                "display_states": ["active_squeeze", "exhaustion", "position_control"],
                "data_valid": True,
                "confidence": 68.0,
                "state": "active_squeeze",
                "driver": "short_cover",
            },
            accumulation={
                "in_accumulation_pool": True,
                "status": "warming",
                "score": 78.5,
                "sideways_days": 96,
                "range_pct": 22.4,
                "recent_vol_ratio_7d": 2.3,
                "market_cap": 125_000_000,
            },
        )

    def test_each_strategy_category_has_its_own_compact_content(self) -> None:
        expected = {
            "actionable_alert": "**💎 高价值候选 |",
            "risk_alert": "**🚨 高风险提醒 |",
            "strong_direct_alert": "**⚡ 急速异动 |",
            "startup_alert": "**🚀 启动预警 |",
        }

        for alert_type, title_prefix in expected.items():
            with self.subTest(alert_type=alert_type):
                message = format_strategy_alert(
                    self._candidate(),
                    AlertDecision(True, alert_type=alert_type, reason="accepted"),
                )
                lines = message.splitlines()
                self.assertTrue(lines[0].startswith(title_prefix))
                for prefix in ("📊 OI：", "⚖️ 仓位：", "🧲 收筹："):
                    self.assertTrue(any(line.startswith(prefix) for line in lines), prefix)
                self.assertTrue(lines[-1].startswith("🔗 https://www.binance.com/futures/"))
                self.assertEqual(lines[1].startswith("⚡ 异动："), alert_type == "strong_direct_alert")
                self.assertEqual(any(line.startswith("✅ 确认：") for line in lines), alert_type == "actionable_alert")
                self.assertEqual(any(line.startswith("⚠️ 风险：") for line in lines), alert_type == "risk_alert")
                self.assertEqual(any(line.startswith("🚀 启动：") for line in lines), alert_type == "startup_alert")
                for removed_prefix in ("🧭 状态：", "💵 价格：", "🧩 数据：", "🔬 微结构:", "💰 资金:", "💡 提示:"):
                    self.assertFalse(any(line.startswith(removed_prefix) for line in lines), removed_prefix)

    def test_strategy_specific_lines_are_concise_and_semantic(self) -> None:
        actionable = format_strategy_alert(
            self._candidate(),
            AlertDecision(True, alert_type="actionable_alert", reason="accepted"),
        )
        risk = format_strategy_alert(
            self._candidate(),
            AlertDecision(True, alert_type="risk_alert", reason="accepted"),
        )

        self.assertIn("📈 异动：1m上涨 +4.17% | RVOL 7.20x | 现价 0.07239000", actionable)
        self.assertIn("📊 OI：新多明显增仓", actionable)
        self.assertIn("✅ 确认：多周期持续触发 | 主动买盘增强", actionable)
        self.assertNotIn("⚠️ 风险：", actionable)
        self.assertIn("⚠️ 风险：1m涨幅过热 | 上涨与主动买盘背离", risk)
        self.assertNotIn("✅ 确认：", risk)
        self.assertIn(
            "🧲 收筹：升温 78.5 | 横盘 96天 | 区间 22.40% | Vol 2.30x | 市值 $125.00M",
            actionable,
        )

    def test_startup_breakout_wording_covers_close_positions(self) -> None:
        candidate = self._candidate()
        decision = AlertDecision(True, alert_type="startup_alert", reason="accepted")

        above = format_strategy_alert(candidate, decision)
        candidate.latest_features["startup_breakout_distance_pct"] = -0.1
        near = format_strategy_alert(candidate, decision)
        candidate.latest_features["startup_breakout_distance_pct"] = -0.5
        intrabar_reversal = format_strategy_alert(candidate, decision)

        self.assertIn("收盘突破60m高点 +0.64%", above)
        self.assertIn("距60m高点还差 0.10%", near)
        self.assertIn("盘中突破后回落，现低于60m高点 0.50%", intrabar_reversal)

    def test_neutral_oi_and_normal_position_pressure_are_hidden(self) -> None:
        candidate = self._candidate()
        candidate.derivatives["oi_regime"] = "neutral"
        candidate.position_pressure["state"] = "no_active_pressure"

        message = format_strategy_alert(
            candidate,
            AlertDecision(True, alert_type="strong_direct_alert", reason="accepted"),
        )

        self.assertNotIn("📊 OI：", message)
        self.assertNotIn("⚖️ 仓位：", message)

    def test_verbose_optional_lines_keep_semantic_icons(self) -> None:
        message = format_strategy_alert(
            self._candidate(),
            AlertDecision(True, alert_type="actionable_alert", reason="accepted"),
            detail_level="full",
        )

        self.assertIn("🧾 事件：early_start | 来源 实时WS", message)
        self.assertIn("📊 OI解释：OI连续增加", message)
        self.assertIn("🔬 微结构解释：主动买盘增强", message)


class OtherTelegramTemplateIconTests(unittest.TestCase):
    def test_base_market_alert_uses_line_icons(self) -> None:
        notifier = AlertNotifier.__new__(AlertNotifier)
        event = AlertEvent(
            symbol="TAKEUSDT",
            window="1m",
            direction="up",
            change_pct=4.17,
            price=0.07239,
            rule_name="rapid_move",
            reasons=["涨幅触发"],
            change_1h_pct=6.0,
            change_24h_pct=12.0,
            mc=125_000_000,
            fdv=180_000_000,
            confidence=88.0,
            confidence_band="A",
            rvol=7.2,
            repeat_count=2,
            merged_count=1,
            merged_peak_change_pct=5.2,
            coalesced_windows=["1m", "5m"],
            coalesced_changes={"1m": 4.17, "5m": 6.1},
        )

        lines = notifier.build_message(event, daily_push_count=1).splitlines()

        self.assertTrue(lines[0].startswith("**📣 TAKE（今日第1次推送）1️⃣"))
        for prefix in ("💵 核心变化：", "🕒 1h/24h：", "💰 MC/FDV：", "⏰ 时间：", "🎯 触发依据：", "📊 周期变化：", "🔁 1m同向连发：", "🧾 合并更新："):
            self.assertTrue(any(line.startswith(prefix) for line in lines), prefix)
        self.assertTrue(lines[-1].startswith("🔗 https://www.binance.com/futures/"))

    def test_accumulation_daily_summary_uses_report_specific_icons(self) -> None:
        payload = {
            "generated_at": "2026-08-08T13:00:00+00:00",
            "count": 2,
            "symbols": {
                "AAAUSDT": {
                    "symbol": "AAAUSDT",
                    "base_asset": "AAA",
                    "status": "ready",
                    "score": 88.0,
                    "sideways_days": 120,
                    "range_pct": 18.0,
                    "range_position": 0.6,
                    "recent_vol_ratio_7d": 2.0,
                    "market_cap": 50_000_000,
                    "data_quality": "ok",
                },
                "BBBUSDT": {
                    "symbol": "BBBUSDT",
                    "base_asset": "BBB",
                    "status": "warming",
                    "score": 72.0,
                    "sideways_days": 90,
                    "range_pct": 35.0,
                    "range_position": 1.3,
                    "recent_vol_ratio_7d": 9.0,
                    "market_cap": 20_000_000,
                    "data_quality": "ok",
                },
            },
        }

        message = _build_summary_message(payload, Path("accumulation_pool.json"), 20)

        self.assertIn("<b>📋 吸筹池日报 | 重点观察 Top1</b>", message)
        for fragment in ("🕒 时间：", "📊 概览：", "🎯 筛选：", "📁 文件：", "<b>🎯 重点观察</b>", "🪙 1.", "📐 横盘：", "📊 量能：", "<b>🚨 风险观察</b>"):
            self.assertIn(fragment, message)

    def test_accumulation_failure_message_uses_status_icons(self) -> None:
        message = _build_failure_message(3, "timeout")

        self.assertIn("<b>❌ 吸筹池日报失败</b>", message)
        self.assertIn("🕒 时间：", message)
        self.assertIn("🔁 尝试：3 次", message)
        self.assertIn("🧾 最后错误：timeout", message)


if __name__ == "__main__":
    unittest.main()
