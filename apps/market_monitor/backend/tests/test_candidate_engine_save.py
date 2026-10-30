from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from candidates.candidate_engine import CandidateEngine
    from candidates.candidate_models import Candidate
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.candidate_engine import CandidateEngine
    from apps.market_monitor.backend.candidates.candidate_models import Candidate


def _candidate(symbol: str, last_seen: datetime) -> Candidate:
    return Candidate(candidate_id=symbol, symbol=symbol, base_asset=symbol[:-4], last_seen=last_seen.isoformat())


class CandidateEngineSaveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.tmp.name)
        self.path = self.runtime / "candidates.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self, **settings) -> CandidateEngine:
        return CandidateEngine({"runtime_dir": str(self.runtime), **settings})

    def _saved_symbols(self) -> set:
        return set(json.loads(self.path.read_text(encoding="utf-8"))["candidates"])

    def test_saves_are_debounced_until_forced(self) -> None:
        engine = self._engine(candidate_save_interval_sec=60)
        now = datetime.now(timezone.utc)
        engine._candidates["AAAUSDT"] = _candidate("AAAUSDT", now)
        engine.save()
        self.assertEqual(self._saved_symbols(), {"AAAUSDT"})

        engine._candidates["BBBUSDT"] = _candidate("BBBUSDT", now)
        engine.save()
        self.assertEqual(self._saved_symbols(), {"AAAUSDT"})

        engine.save(force=True)
        self.assertEqual(self._saved_symbols(), {"AAAUSDT", "BBBUSDT"})

    def test_zero_interval_saves_every_time(self) -> None:
        engine = self._engine(candidate_save_interval_sec=0)
        now = datetime.now(timezone.utc)
        engine._candidates["AAAUSDT"] = _candidate("AAAUSDT", now)
        engine.save()
        engine._candidates["BBBUSDT"] = _candidate("BBBUSDT", now)
        engine.save()
        self.assertEqual(self._saved_symbols(), {"AAAUSDT", "BBBUSDT"})

    def test_candidates_past_retention_are_pruned_on_save(self) -> None:
        engine = self._engine(candidate_retention_hours=24)
        now = datetime.now(timezone.utc)
        engine._candidates["OLDUSDT"] = _candidate("OLDUSDT", now - timedelta(hours=30))
        engine._candidates["NEWUSDT"] = _candidate("NEWUSDT", now - timedelta(hours=2))
        engine.save(force=True)
        self.assertEqual(self._saved_symbols(), {"NEWUSDT"})
        self.assertNotIn("OLDUSDT", engine.all_candidates())

    def test_saved_file_is_compact_json(self) -> None:
        engine = self._engine()
        engine._candidates["AAAUSDT"] = _candidate("AAAUSDT", datetime.now(timezone.utc))
        engine.save(force=True)
        self.assertNotIn("\n", self.path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
