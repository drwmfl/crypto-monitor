from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from alert_config import load_alert_config
    from alerts.alert_policy import AlertPolicy
    from candidates.candidate_models import Candidate
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_alert_config
    from apps.market_monitor.backend.alerts.alert_policy import AlertPolicy
    from apps.market_monitor.backend.candidates.candidate_models import Candidate


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.json"


def _candidate(direction: str) -> Candidate:
    return Candidate(
        candidate_id="METUSDT",
        symbol="METUSDT",
        base_asset="MET",
        score=90.0,
        risk_score=30.0,
        latest_features={"direction": direction, "window": "5m", "change_pct": 6.0},
    )


class ActionableDirectionTests(unittest.TestCase):
    def _policy(self, runtime_dir: str, **overrides: object) -> AlertPolicy:
        settings = {
            "runtime_dir": runtime_dir,
            "require_oi_for_actionable": False,
            "require_confirmation_for_actionable": False,
            "actionable_edge_filter_enabled": False,
            "actionable_require_factor_completeness": False,
        }
        settings.update(overrides)
        return AlertPolicy(settings)

    def test_down_actionable_blocked_when_only_up_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            "os.environ",
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            policy = self._policy(runtime_dir, actionable_allowed_directions=["up"])

            down = policy.decide(_candidate("down"))
            up = policy.decide(_candidate("up"))

        self.assertFalse(down.should_send)
        self.assertEqual(down.alert_type, "actionable_alert")
        self.assertEqual(down.reason, "actionable_direction_disabled")
        self.assertTrue(up.should_send)
        self.assertEqual(up.alert_type, "actionable_alert")

    def test_both_directions_allowed_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as runtime_dir, patch.dict(
            "os.environ",
            {"ALERT_STRATEGY_RUNTIME_DIR": runtime_dir},
        ):
            policy = self._policy(runtime_dir)

            down = policy.decide(_candidate("down"))

        self.assertTrue(down.should_send)
        self.assertEqual(down.alert_type, "actionable_alert")


class ConfigNormalizationTests(unittest.TestCase):
    def test_repo_config_enables_new_guards(self) -> None:
        strategy = load_alert_config(config_path=str(CONFIG_PATH)).raw["alert_strategy"]

        self.assertFalse(strategy["extreme_fast_shadow_only"])
        self.assertTrue(strategy["extreme_fast_followup_enabled"])
        self.assertEqual(strategy["extreme_fast_followup_minutes"], 15)
        self.assertEqual(strategy["startup_alert_min_confirmations"], 3)
        self.assertEqual(strategy["actionable_allowed_directions"], ["up"])

    def test_repo_config_runs_discovery_in_shadow_mode(self) -> None:
        discovery = load_alert_config(config_path=str(CONFIG_PATH)).raw["discovery"]

        raw = load_alert_config(config_path=str(CONFIG_PATH)).raw

        self.assertTrue(discovery["enabled"])
        self.assertEqual((discovery["t1_change_pct"], discovery["t2_runup_pct"]), (4.0, 8.0))
        self.assertTrue(discovery["t2_confirm_on_close"])
        self.assertEqual(discovery["push_directions"], ["up"])
        self.assertEqual(discovery["directions"], ["up", "down"])
        self.assertEqual(discovery["instrument_types"], ["crypto"])
        self.assertEqual((discovery["thread_hours"], discovery["upgrade_pct"], discovery["daily_cap"]), (4.0, 15.0, 3))
        self.assertEqual(raw["data_feed"]["ws_fallback_batch_size"], 60)
        self.assertEqual(raw["data_feed"]["poll_cycle_budget_sec"], 120)
        self.assertEqual(raw["data_feed"]["snapshot_timeout_sec"], 20)

    def test_invalid_directions_fall_back_to_both(self) -> None:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
        raw["alert_strategy"]["actionable_allowed_directions"] = ["sideways"]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            strategy = load_alert_config(config_path=str(path)).raw["alert_strategy"]

        self.assertEqual(strategy["actionable_allowed_directions"], ["up", "down"])


if __name__ == "__main__":
    unittest.main()
