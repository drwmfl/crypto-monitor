from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

try:
    import run_accumulation_pool_scheduler as scheduler
    from scan_accumulation_pool import _attach_shadow_metrics, _parse_24h_tickers
except ModuleNotFoundError:
    from apps.market_monitor.backend import run_accumulation_pool_scheduler as scheduler
    from apps.market_monitor.backend.scan_accumulation_pool import _attach_shadow_metrics, _parse_24h_tickers


def _pool_item(
    symbol: str,
    *,
    score: float,
    status: str = "ready",
    sideways_days: int = 120,
    range_position: float = 0.6,
    volume_ratio: float = 2.0,
    impulse_score: float = 12.0,
) -> dict:
    return {
        "symbol": symbol,
        "base_asset": symbol.removesuffix("USDT"),
        "status": status,
        "score": score,
        "component_scores": {
            "sideways_days": 18.0,
            "range_compression": 16.0,
            "flatness": 12.0,
            "quiet_volume": 11.0,
            "market_cap": 10.0,
            "volume_impulse": impulse_score,
        },
        "sideways_days": sideways_days,
        "range_pct": 20.0,
        "slope_pct": 1.0,
        "range_low": 10.0,
        "range_high": 20.0,
        "range_position": range_position,
        "current_price": 16.0,
        "avg_quote_vol_usdt": 1_000_000.0,
        "recent_avg_quote_vol_usdt_7d": volume_ratio * 1_000_000.0,
        "recent_vol_ratio_7d": volume_ratio,
        "market_cap": 50_000_000.0,
        "data_quality": "ok",
        "daily_data_cutoff_at": "2026-09-02T23:59:59.999000+00:00",
    }


def _payload() -> dict:
    focus = _pool_item("AAAUSDT", score=90.0)
    backup = _pool_item("BBBUSDT", score=80.0, sideways_days=60)
    risk = _pool_item("CCCUSDT", score=70.0, range_position=1.3, volume_ratio=9.0)
    for rank, item in enumerate((focus, backup, risk), start=1):
        item["shadow"] = {
            "version": "accumulation-pool-shadow-v1",
            "observed_at": "2026-09-03T13:00:00+00:00",
            "daily_data_cutoff_at": item["daily_data_cutoff_at"],
            "fresh_data_status": "ok",
            "current_price": item["current_price"],
            "market_data_cutoff_at": "2026-09-03T13:00:00+00:00",
            "range_position": item["range_position"],
            "rolling_24h_quote_volume": 2_000_000.0,
            "volume_ratio_24h": 2.0,
            "structure_score": 78.0,
            "activation_score": 72.0,
            "risk_score": 0.0,
            "risk_state": "active",
            "rank": rank,
        }
    return {
        "generated_at": "2026-09-03T13:00:05+00:00",
        "version": 1,
        "count": 3,
        "symbols": {item["symbol"]: item for item in (focus, backup, risk)},
        "shadow": {
            "version": "accumulation-pool-shadow-v1",
            "observed_at": "2026-09-03T13:00:00+00:00",
        },
    }


class AccumulationShadowTests(unittest.TestCase):
    def test_parse_bulk_ticker_uses_current_price_and_rolling_quote_volume(self) -> None:
        parsed = _parse_24h_tickers(
            [
                {
                    "symbol": "AAAUSDT",
                    "lastPrice": "18.5",
                    "quoteVolume": "2500000",
                    "closeTime": 1788440400000,
                },
                {"symbol": "INVALID", "lastPrice": "0", "quoteVolume": "1"},
            ]
        )

        self.assertEqual(set(parsed), {"AAAUSDT"})
        self.assertEqual(parsed["AAAUSDT"]["current_price"], 18.5)
        self.assertEqual(parsed["AAAUSDT"]["rolling_24h_quote_volume"], 2_500_000.0)

    def test_shadow_enrichment_does_not_change_legacy_fields(self) -> None:
        low_impulse = _pool_item("AAAUSDT", score=74.0, impulse_score=1.0)
        high_impulse = _pool_item("BBBUSDT", score=88.0, impulse_score=15.0)
        before = [
            (item["score"], item["status"], item["current_price"], item["range_position"], item["recent_vol_ratio_7d"])
            for item in (low_impulse, high_impulse)
        ]
        tickers = {
            "AAAUSDT": {"current_price": 18.0, "rolling_24h_quote_volume": 2_500_000.0, "close_time": 1788440400000},
            "BBBUSDT": {"current_price": 21.0, "rolling_24h_quote_volume": 9_000_000.0, "close_time": 1788440400000},
        }

        _attach_shadow_metrics(
            [low_impulse, high_impulse],
            tickers,
            observed_at="2026-09-03T13:00:00+00:00",
        )

        after = [
            (item["score"], item["status"], item["current_price"], item["range_position"], item["recent_vol_ratio_7d"])
            for item in (low_impulse, high_impulse)
        ]
        self.assertEqual(before, after)
        self.assertEqual(low_impulse["shadow"]["structure_score"], high_impulse["shadow"]["structure_score"])
        self.assertLess(low_impulse["shadow"]["activation_score"], high_impulse["shadow"]["activation_score"])
        self.assertEqual(high_impulse["shadow"]["risk_state"], "overheated")

    def test_missing_fresh_data_records_unavailable_without_failing(self) -> None:
        item = _pool_item("AAAUSDT", score=80.0)

        _attach_shadow_metrics(
            [item],
            {},
            observed_at="2026-09-03T13:00:00+00:00",
            source_error="ticker_24h_unavailable:TimeoutError",
        )

        self.assertEqual(item["score"], 80.0)
        self.assertEqual(item["status"], "ready")
        self.assertEqual(item["shadow"]["fresh_data_status"], "unavailable")
        self.assertEqual(item["shadow"]["risk_state"], "unavailable")
        self.assertIsNone(item["shadow"]["activation_score"])
        self.assertEqual(item["shadow"]["rank"], 1)

    def test_report_record_contains_exact_displayed_groups_and_shadow_fields(self) -> None:
        payload = _payload()
        message = scheduler._build_summary_message(payload, Path("accumulation_pool.json"), 20)

        row = scheduler._build_report_record(
            payload,
            Path("accumulation_pool.json"),
            20,
            message,
            sent_at=datetime(2026, 9, 3, 13, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(row["send_status"], "sent")
        self.assertEqual(row["displayed_symbols"], ["AAAUSDT", "BBBUSDT", "CCCUSDT"])
        self.assertEqual(row["group_counts"], {"focus": 1, "backup": 1, "risk": 1})
        self.assertEqual([item["display_order"] for item in row["items"]], [1, 2, 3])
        self.assertEqual([item["group"] for item in row["items"]], ["focus", "backup", "risk"])
        self.assertEqual(row["items"][0]["component_scores"]["range_compression"], 16.0)
        self.assertEqual(row["items"][0]["shadow"]["structure_score"], 78.0)
        self.assertEqual(row["message_html"], message)

    def test_ledger_append_is_idempotent_and_json_serializable(self) -> None:
        payload = _payload()
        message = scheduler._build_summary_message(payload, Path("accumulation_pool.json"), 20)
        row = scheduler._build_report_record(payload, Path("accumulation_pool.json"), 20, message)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "ledger.jsonl"
            self.assertTrue(scheduler._append_report_ledger(path, row))
            self.assertFalse(scheduler._append_report_ledger(path, row))
            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 1)
        restored = json.loads(lines[0])
        self.assertEqual(restored["report_id"], row["report_id"])
        self.assertEqual(restored["displayed_symbols"], row["displayed_symbols"])

    def test_shadow_fields_do_not_change_existing_telegram_template(self) -> None:
        payload = _payload()

        message = scheduler._build_summary_message(payload, Path("accumulation_pool.json"), 20)

        self.assertNotIn("shadow", message.lower())
        self.assertNotIn("结构分", message)
        self.assertIn("评分 <b>90.0</b>", message)
        self.assertIn("Vol 2.0x", message)

    def test_shadow_rank_does_not_change_legacy_telegram_order(self) -> None:
        payload = _payload()
        payload["symbols"]["AAAUSDT"]["score"] = 70.0
        payload["symbols"]["AAAUSDT"]["shadow"]["rank"] = 1
        payload["symbols"]["BBBUSDT"]["score"] = 90.0
        payload["symbols"]["BBBUSDT"]["sideways_days"] = 120
        payload["symbols"]["BBBUSDT"]["shadow"]["rank"] = 99

        selection = scheduler._select_summary_items(payload, 20)

        self.assertEqual(
            [item["symbol"] for item in selection["focus_items"]],
            ["BBBUSDT", "AAAUSDT"],
        )


class AccumulationLedgerFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_telegram_send_records_exact_report(self) -> None:
        args = SimpleNamespace(
            max_attempts=1,
            retry_delay_sec=1.0,
            config_path="",
            summary_limit=20,
            no_send=False,
        )
        payload = _payload()
        with patch.object(scheduler, "_run_scan_subprocess", AsyncMock(return_value=(True, "ok"))), \
             patch.object(scheduler, "_load_pool_payload", Mock(return_value=(payload, Path("accumulation_pool.json")))), \
             patch.object(scheduler, "_validate_fresh_payload", Mock(return_value=(True, ""))), \
             patch.object(scheduler, "_send_telegram", AsyncMock(return_value=True)), \
             patch.object(scheduler, "_record_sent_report", Mock(return_value=True)) as record, \
             patch("builtins.print"):
            result = await scheduler._run_cycle(args)

        self.assertTrue(result)
        record.assert_called_once()
        self.assertIs(record.call_args.args[0], payload)

    async def test_failed_telegram_send_does_not_record_sent_report(self) -> None:
        args = SimpleNamespace(
            max_attempts=1,
            retry_delay_sec=1.0,
            config_path="",
            summary_limit=20,
            no_send=False,
        )
        payload = _payload()
        with patch.object(scheduler, "_run_scan_subprocess", AsyncMock(return_value=(True, "ok"))), \
             patch.object(scheduler, "_load_pool_payload", Mock(return_value=(payload, Path("accumulation_pool.json")))), \
             patch.object(scheduler, "_validate_fresh_payload", Mock(return_value=(True, ""))), \
             patch.object(scheduler, "_send_telegram", AsyncMock(return_value=False)), \
             patch.object(scheduler, "_record_sent_report", Mock()) as record, \
             patch("builtins.print"):
            result = await scheduler._run_cycle(args)

        self.assertFalse(result)
        record.assert_not_called()


if __name__ == "__main__":
    unittest.main()
