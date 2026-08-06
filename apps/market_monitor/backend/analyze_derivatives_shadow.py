from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo


def load_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def first_push_rows(rows: Iterable[Dict[str, Any]], timezone_name: str) -> List[Dict[str, Any]]:
    try:
        local_timezone = ZoneInfo(timezone_name)
    except Exception:
        local_timezone = timezone.utc
    selected: Dict[str, Dict[str, Any]] = {}
    ordered = sorted(rows, key=lambda row: str(row.get("created_at") or ""))
    for row in ordered:
        decision = row.get("actual_decision") if isinstance(row.get("actual_decision"), dict) else {}
        if not bool(decision.get("alert_sent")):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        timestamp = _parse_time(row.get("created_at"))
        if not symbol or timestamp is None:
            continue
        key = f"{timestamp.astimezone(local_timezone).date().isoformat()}:{symbol}"
        selected.setdefault(key, row)
    return list(selected.values())


def summarize(rows: List[Dict[str, Any]], timezone_name: str = "Asia/Shanghai") -> Dict[str, Any]:
    samples = first_push_rows(rows, timezone_name)
    basis_states: Counter[str] = Counter()
    oi_regimes: Counter[str] = Counter()
    oi_turns: Counter[str] = Counter()
    funding_states: Counter[str] = Counter()
    alert_types: Counter[str] = Counter()
    coverage = Counter()
    opportunity_modifiers: List[float] = []
    risk_modifiers: List[float] = []
    for row in samples:
        factors = row.get("factor_state") if isinstance(row.get("factor_state"), dict) else {}
        decision = row.get("actual_decision") if isinstance(row.get("actual_decision"), dict) else {}
        shadow = row.get("shadow_decision") if isinstance(row.get("shadow_decision"), dict) else {}
        basis_states[str(factors.get("basis_shadow_state") or "missing")] += 1
        oi_regimes[str(factors.get("oi_shadow_regime") or "missing")] += 1
        oi_turns[
            f"{str(factors.get('oi_turn_direction') or 'none')}:{str(factors.get('oi_turn_stage') or 'none')}"
        ] += 1
        funding_states[str(factors.get("funding_shadow_state") or "missing")] += 1
        alert_types[str(decision.get("alert_type") or "unknown")] += 1
        if _has_number(factors.get("basis_bps_now")):
            coverage["basis"] += 1
        if _has_number(factors.get("oi_amount_change_pct_5m")) or _has_number(
            factors.get("oi_amount_change_pct_15m")
        ):
            coverage["oi"] += 1
        if str(factors.get("funding_shadow_status") or "") == "ready":
            coverage["funding"] += 1
        opportunity_modifiers.append(_safe_float(shadow.get("opportunity_modifier"), 0.0))
        risk_modifiers.append(_safe_float(shadow.get("risk_modifier"), 0.0))

    count = len(samples)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timezone": timezone_name,
        "shadow_rows": len(rows),
        "valid_first_push_samples": count,
        "coverage_pct": {
            key: round(coverage[key] / count * 100.0, 2) if count else 0.0
            for key in ("basis", "oi", "funding")
        },
        "alert_types": dict(alert_types),
        "basis_states": dict(basis_states),
        "oi_regimes": dict(oi_regimes),
        "oi_turns": dict(oi_turns),
        "funding_states": dict(funding_states),
        "shadow_modifiers": {
            "mean_opportunity": round(mean(opportunity_modifiers), 4) if opportunity_modifiers else 0.0,
            "mean_risk": round(mean(risk_modifiers), 4) if risk_modifiers else 0.0,
            "opportunity_changed_samples": sum(1 for value in opportunity_modifiers if value != 0),
            "risk_changed_samples": sum(1 for value in risk_modifiers if value != 0),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize derivatives factor shadow data.")
    parser.add_argument("--runtime-dir", default="/runtime")
    parser.add_argument("--file", default="derivatives_factor_shadow.jsonl")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    source = Path(args.runtime_dir) / args.file
    report = summarize(load_rows(source), args.timezone)
    encoded = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    print(encoded)


def _parse_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _has_number(value: Any) -> bool:
    try:
        float(value)
        return value is not None
    except (TypeError, ValueError):
        return False


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


if __name__ == "__main__":
    main()
