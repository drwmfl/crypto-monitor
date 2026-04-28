from __future__ import annotations

import argparse
import asyncio
import logging
import math
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

try:
    from analyze_tg_push_accuracy import (
        PushEvent,
        _build_exchange,
        _fetch_symbol_rows,
        _format_dt,
        _parse_push_events,
    )
    from paths import default_reports_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.analyze_tg_push_accuracy import (
        PushEvent,
        _build_exchange,
        _fetch_symbol_rows,
        _format_dt,
        _parse_push_events,
    )
    from apps.market_monitor.backend.paths import default_reports_dir


logger = logging.getLogger(__name__)
UTC = timezone.utc


@dataclass
class BreakevenResult:
    symbol: str
    threshold_pct: float
    first_push_time_utc: str
    first_push_time_local: str
    rule_name: str
    window: str
    signal_direction: str
    signal_direction_key: str
    source: str
    candle_state: str
    entry_price: float
    signal_change_pct: float
    confidence: Optional[float]
    alert_tier: str
    rvol: Optional[float]
    activation_price: float
    armed: Optional[bool]
    activation_time_utc: Optional[str]
    activation_time_local: Optional[str]
    stop_hit: Optional[bool]
    stop_hit_same_candle: Optional[bool]
    exit_time_utc: Optional[str]
    exit_time_local: Optional[str]
    exit_price: Optional[float]
    realized_return_pct: Optional[float]
    latest_close_time_utc: Optional[str]
    latest_close_time_local: Optional[str]
    latest_close: Optional[float]
    hold_return_pct: Optional[float]
    improvement_vs_hold_pct: Optional[float]
    peak_price: Optional[float]
    peak_gain_pct: Optional[float]
    post_push_candle_count: int
    metric_status: str


def _setup_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _ceil_minute_ms(dt: datetime) -> int:
    epoch_ms = int(dt.timestamp() * 1000)
    return int(math.ceil(epoch_ms / 60_000.0) * 60_000)


def _keep_first_push_per_symbol(events: Iterable[PushEvent]) -> List[PushEvent]:
    first_by_symbol: Dict[str, PushEvent] = {}
    for event in sorted(events, key=lambda item: item.push_time_utc):
        first_by_symbol.setdefault(event.symbol, event)
    ordered = list(first_by_symbol.values())
    ordered.sort(key=lambda item: (item.push_time_utc, item.symbol))
    return ordered


def _load_market_mapping(exchange) -> Dict[str, str]:
    markets = exchange.load_markets()
    mapping: Dict[str, str] = {}
    for ccxt_symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        if market.get("quote") != "USDT":
            continue
        if not (market.get("swap") or market.get("contract")):
            continue
        if not market.get("active", True):
            continue

        market_id = str(market.get("id") or "").strip().upper().replace("/", "")
        market_symbol = str(market.get("symbol") or "").strip().upper().replace("/", "")
        if market_id:
            mapping.setdefault(market_id, ccxt_symbol)
        if market_symbol:
            mapping.setdefault(market_symbol.replace(":USDT", ""), ccxt_symbol)
    return mapping


def _slice_rows_by_time(rows: List[List[float]], start_ms: int, end_ms: int) -> List[List[float]]:
    timestamps = [int(row[0]) for row in rows]
    start_idx = bisect_left(timestamps, start_ms)
    end_idx = bisect_left(timestamps, end_ms)
    return rows[start_idx:end_idx]


def _first_index_of_max(values: Sequence[float]) -> int:
    max_value = max(values)
    for idx, value in enumerate(values):
        if value == max_value:
            return idx
    return 0


def _simulate_breakeven(
    event: PushEvent,
    rows: List[List[float]],
    threshold_pct: float,
    local_tz: ZoneInfo,
) -> BreakevenResult:
    activation_price = event.entry_price * (1.0 + threshold_pct / 100.0)
    base_kwargs = {
        "symbol": event.symbol,
        "threshold_pct": float(threshold_pct),
        "first_push_time_utc": _format_dt(event.push_time_utc, UTC) or "",
        "first_push_time_local": _format_dt(event.push_time_utc, local_tz) or "",
        "rule_name": event.rule_name,
        "window": event.window,
        "signal_direction": event.direction_label,
        "signal_direction_key": event.direction_key,
        "source": event.source,
        "candle_state": event.candle_state,
        "entry_price": round(event.entry_price, 12),
        "signal_change_pct": round(event.change_pct, 6),
        "confidence": event.confidence,
        "alert_tier": event.alert_tier,
        "rvol": event.rvol,
        "activation_price": round(activation_price, 12),
        "post_push_candle_count": len(rows),
    }

    if not rows:
        return BreakevenResult(
            **base_kwargs,
            armed=None,
            activation_time_utc=None,
            activation_time_local=None,
            stop_hit=None,
            stop_hit_same_candle=None,
            exit_time_utc=None,
            exit_time_local=None,
            exit_price=None,
            realized_return_pct=None,
            latest_close_time_utc=None,
            latest_close_time_local=None,
            latest_close=None,
            hold_return_pct=None,
            improvement_vs_hold_pct=None,
            peak_price=None,
            peak_gain_pct=None,
            metric_status="no_post_push_candles",
        )

    highs = [float(row[2]) for row in rows]
    closes = [float(row[4]) for row in rows]
    peak_idx = _first_index_of_max(highs)
    peak_row = rows[peak_idx]
    peak_dt = datetime.fromtimestamp(int(peak_row[0]) / 1000.0, tz=UTC)
    peak_price = float(peak_row[2])
    peak_gain_pct = (peak_price - event.entry_price) / event.entry_price * 100.0

    latest_row = rows[-1]
    latest_dt = datetime.fromtimestamp(int(latest_row[0]) / 1000.0, tz=UTC)
    latest_close = float(latest_row[4])
    hold_return_pct = (latest_close - event.entry_price) / event.entry_price * 100.0

    armed = False
    activation_time_dt: Optional[datetime] = None
    stop_hit = False
    stop_hit_same_candle = False
    exit_dt: Optional[datetime] = None
    exit_price: Optional[float] = None
    metric_status = "never_armed_hold_to_latest"

    for row in rows:
        candle_dt = datetime.fromtimestamp(int(row[0]) / 1000.0, tz=UTC)
        high = float(row[2])
        low = float(row[3])

        if not armed:
            if high >= activation_price:
                armed = True
                activation_time_dt = candle_dt
                # Conservative assumption:
                # if activation and pullback to entry happen in the same 1m candle,
                # treat it as a breakeven stop hit.
                if low <= event.entry_price:
                    stop_hit = True
                    stop_hit_same_candle = True
                    exit_dt = candle_dt
                    exit_price = event.entry_price
                    metric_status = "stop_hit_same_candle"
                    break
                metric_status = "armed_hold_to_latest"
            continue

        if low <= event.entry_price:
            stop_hit = True
            exit_dt = candle_dt
            exit_price = event.entry_price
            metric_status = "stop_hit_after_activation"
            break

    if exit_dt is None:
        exit_dt = latest_dt
        exit_price = latest_close
        if armed:
            metric_status = "armed_hold_to_latest"

    realized_return_pct = (float(exit_price) - event.entry_price) / event.entry_price * 100.0
    improvement_vs_hold_pct = realized_return_pct - hold_return_pct

    return BreakevenResult(
        **base_kwargs,
        armed=armed,
        activation_time_utc=_format_dt(activation_time_dt, UTC),
        activation_time_local=_format_dt(activation_time_dt, local_tz),
        stop_hit=stop_hit,
        stop_hit_same_candle=stop_hit_same_candle,
        exit_time_utc=_format_dt(exit_dt, UTC),
        exit_time_local=_format_dt(exit_dt, local_tz),
        exit_price=round(float(exit_price), 12),
        realized_return_pct=round(realized_return_pct, 6),
        latest_close_time_utc=_format_dt(latest_dt, UTC),
        latest_close_time_local=_format_dt(latest_dt, local_tz),
        latest_close=round(latest_close, 12),
        hold_return_pct=round(hold_return_pct, 6),
        improvement_vs_hold_pct=round(improvement_vs_hold_pct, 6),
        peak_price=round(peak_price, 12),
        peak_gain_pct=round(peak_gain_pct, 6),
        metric_status=metric_status,
    )


async def _collect_results(
    first_pushes: List[PushEvent],
    thresholds: Sequence[float],
    local_tz: ZoneInfo,
    fetch_limit: int,
    max_concurrency: int,
) -> List[BreakevenResult]:
    if not first_pushes:
        return []

    exchange = _build_exchange()
    mapping = await asyncio.to_thread(_load_market_mapping, exchange)
    now_utc = datetime.now(tz=UTC)
    end_ms = int(now_utc.timestamp() * 1000)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    per_symbol_rows: Dict[str, List[List[float]]] = {}

    async def fetch_one(event: PushEvent) -> None:
        ccxt_symbol = mapping.get(event.symbol)
        if not ccxt_symbol:
            per_symbol_rows[event.symbol] = []
            logger.warning("Symbol not found in Binance futures market: %s", event.symbol)
            return

        start_ms = _ceil_minute_ms(event.push_time_utc)
        async with sem:
            per_symbol_rows[event.symbol] = await _fetch_symbol_rows(
                exchange=exchange,
                ccxt_symbol=ccxt_symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                fetch_limit=fetch_limit,
            )

    await asyncio.gather(*(fetch_one(event) for event in first_pushes))

    results: List[BreakevenResult] = []
    for event in first_pushes:
        rows = per_symbol_rows.get(event.symbol, [])
        for threshold in thresholds:
            results.append(
                _simulate_breakeven(
                    event=event,
                    rows=rows,
                    threshold_pct=float(threshold),
                    local_tz=local_tz,
                )
            )

    results.sort(key=lambda item: (item.threshold_pct, item.first_push_time_utc, item.symbol))

    try:
        await asyncio.to_thread(exchange.close)
    except Exception:
        logger.debug("Exchange close failed.", exc_info=True)
    return results


def _build_summary(
    results: List[BreakevenResult],
    focus_symbols: Sequence[str],
    log_coverage_start: Optional[datetime],
    local_tz: ZoneInfo,
) -> pd.DataFrame:
    ready = [item for item in results if item.metric_status != "no_post_push_candles"]
    focus_set = {str(symbol or "").strip().upper() for symbol in focus_symbols if str(symbol or "").strip()}
    rows: List[Dict[str, Any]] = []

    def summarize(threshold: float, scope: str, items: List[BreakevenResult]) -> None:
        if not items:
            rows.append(
                {
                    "threshold_pct": threshold,
                    "scope": scope,
                    "rows": 0,
                    "armed_count": 0,
                    "armed_ratio_pct": 0.0,
                    "stop_hit_count": 0,
                    "stop_hit_ratio_pct": 0.0,
                    "win_count": 0,
                    "win_rate_pct": 0.0,
                    "avg_realized_return_pct": 0.0,
                    "median_realized_return_pct": 0.0,
                    "avg_hold_return_pct": 0.0,
                    "median_hold_return_pct": 0.0,
                    "avg_improvement_vs_hold_pct": 0.0,
                    "avg_peak_gain_pct": 0.0,
                    "log_coverage_start_local": _format_dt(log_coverage_start, local_tz),
                }
            )
            return

        frame = pd.DataFrame([item.__dict__ for item in items])
        realized = frame["realized_return_pct"].astype(float)
        hold = frame["hold_return_pct"].astype(float)
        peak = frame["peak_gain_pct"].astype(float)
        armed_count = int(frame["armed"].fillna(False).astype(bool).sum())
        stop_hit_count = int(frame["stop_hit"].fillna(False).astype(bool).sum())
        win_count = int((realized > 0).sum())

        rows.append(
            {
                "threshold_pct": threshold,
                "scope": scope,
                "rows": int(len(frame)),
                "armed_count": armed_count,
                "armed_ratio_pct": round(armed_count / len(frame) * 100.0, 6),
                "stop_hit_count": stop_hit_count,
                "stop_hit_ratio_pct": round(stop_hit_count / len(frame) * 100.0, 6),
                "win_count": win_count,
                "win_rate_pct": round(win_count / len(frame) * 100.0, 6),
                "avg_realized_return_pct": round(float(realized.mean()), 6),
                "median_realized_return_pct": round(float(realized.median()), 6),
                "avg_hold_return_pct": round(float(hold.mean()), 6),
                "median_hold_return_pct": round(float(hold.median()), 6),
                "avg_improvement_vs_hold_pct": round(float((realized - hold).mean()), 6),
                "avg_peak_gain_pct": round(float(peak.mean()), 6),
                "log_coverage_start_local": _format_dt(log_coverage_start, local_tz),
            }
        )

    thresholds = sorted({float(item.threshold_pct) for item in ready})
    for threshold in thresholds:
        subset = [item for item in ready if float(item.threshold_pct) == threshold]
        summarize(threshold, "all_symbols", subset)
        summarize(threshold, "signal_up_only", [item for item in subset if item.signal_direction_key == "up"])
        summarize(threshold, "signal_down_only", [item for item in subset if item.signal_direction_key == "down"])
        summarize(threshold, "focus_symbols", [item for item in subset if item.symbol in focus_set])
        summarize(threshold, "other_symbols", [item for item in subset if item.symbol not in focus_set])
    return pd.DataFrame(rows)


def _export(
    results: List[BreakevenResult],
    summary_df: pd.DataFrame,
    export_dir: Path,
    export_prefix: str,
) -> List[Path]:
    export_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{export_prefix}_{ts}"
    detail_path = export_dir / f"{base}_detail.csv"
    summary_path = export_dir / f"{base}_summary.csv"

    pd.DataFrame([item.__dict__ for item in results]).to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return [detail_path, summary_path]


def _parse_thresholds(raw: str) -> List[float]:
    values: List[float] = []
    for chunk in str(raw or "").split(","):
        text = chunk.strip()
        if not text:
            continue
        try:
            value = float(text)
        except ValueError:
            continue
        if value > 0:
            values.append(value)
    if not values:
        return [5.0, 10.0]
    return sorted(dict.fromkeys(values))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze first-push long entries with breakeven stop activation after +X%."
    )
    parser.add_argument("--log-path", required=True, help="Saved alert_monitor log text file.")
    parser.add_argument("--days", type=int, default=7, help="Parse pushes not older than this lookback.")
    parser.add_argument("--timezone", type=str, default="Asia/Shanghai")
    parser.add_argument("--thresholds", type=str, default="5,10", help="Comma-separated activation thresholds in pct.")
    parser.add_argument("--fetch-limit", type=int, default=1000)
    parser.add_argument("--max-concurrency", type=int, default=3)
    parser.add_argument("--export-dir", type=str, default=default_reports_dir())
    parser.add_argument("--export-prefix", type=str, default="tg_first_push_breakeven")
    parser.add_argument("--focus-symbols", type=str, default="RAVEUSDT,SIRENUSDT,STOUSDT")
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> None:
    _setup_logging(args.log_level)
    local_tz = ZoneInfo(str(args.timezone or "Asia/Shanghai"))
    thresholds = _parse_thresholds(args.thresholds)
    focus_symbols = [item.strip().upper() for item in str(args.focus_symbols or "").split(",") if item.strip()]
    cutoff_utc = datetime.now(tz=UTC) - timedelta(days=max(1, int(args.days)))
    log_path = Path(str(args.log_path)).resolve()

    events = _parse_push_events(log_path=log_path, cutoff_utc=cutoff_utc)
    if not events:
        raise RuntimeError(f"No push events parsed from {log_path}")

    first_pushes = _keep_first_push_per_symbol(events)
    results = await _collect_results(
        first_pushes=first_pushes,
        thresholds=thresholds,
        local_tz=local_tz,
        fetch_limit=max(200, min(1500, int(args.fetch_limit))),
        max_concurrency=max(1, int(args.max_concurrency)),
    )
    summary_df = _build_summary(
        results=results,
        focus_symbols=focus_symbols,
        log_coverage_start=events[0].push_time_utc if events else None,
        local_tz=local_tz,
    )
    exported = _export(
        results=results,
        summary_df=summary_df,
        export_dir=Path(str(args.export_dir)).resolve(),
        export_prefix=str(args.export_prefix or "tg_first_push_breakeven").strip() or "tg_first_push_breakeven",
    )

    print("\n========== TG FIRST PUSH BREAKEVEN REPORT ==========")
    print(f"log_path: {log_path}")
    print(f"parsed_push_events: {len(events)}")
    print(f"unique_symbols_first_push: {len(first_pushes)}")
    print(f"log_coverage_start_local: {_format_dt(events[0].push_time_utc, local_tz)}")
    for threshold in thresholds:
        row = summary_df[
            (summary_df["threshold_pct"] == float(threshold))
            & (summary_df["scope"] == "all_symbols")
        ]
        if row.empty:
            continue
        item = row.iloc[0].to_dict()
        print(
            f"threshold={threshold}%: win_rate={float(item['win_rate_pct']):.2f}% "
            f"avg_realized={float(item['avg_realized_return_pct']):.2f}% "
            f"avg_hold={float(item['avg_hold_return_pct']):.2f}% "
            f"avg_improvement={float(item['avg_improvement_vs_hold_pct']):.2f}% "
            f"armed_ratio={float(item['armed_ratio_pct']):.2f}% "
            f"stop_hit_ratio={float(item['stop_hit_ratio_pct']):.2f}%"
        )
    print("exported_files:")
    for path in exported:
        print(f"- {path}")
    print("====================================================\n")


def main() -> None:
    args = parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
