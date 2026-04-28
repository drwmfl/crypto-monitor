from __future__ import annotations

import argparse
import asyncio
import logging
import math
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
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
class SymbolRunupMetric:
    symbol: str
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
    peak_time_utc: Optional[str]
    peak_time_local: Optional[str]
    time_to_peak_minutes: Optional[float]
    peak_price: Optional[float]
    peak_gain_pct: Optional[float]
    latest_close_time_utc: Optional[str]
    latest_close_time_local: Optional[str]
    latest_close: Optional[float]
    latest_close_return_pct: Optional[float]
    post_push_candle_count: int
    fake_breakout_strict: Optional[bool]
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


def _build_metric(
    event: PushEvent,
    rows: List[List[float]],
    local_tz: ZoneInfo,
) -> SymbolRunupMetric:
    if not rows:
        return SymbolRunupMetric(
            symbol=event.symbol,
            first_push_time_utc=_format_dt(event.push_time_utc, UTC) or "",
            first_push_time_local=_format_dt(event.push_time_utc, local_tz) or "",
            rule_name=event.rule_name,
            window=event.window,
            signal_direction=event.direction_label,
            signal_direction_key=event.direction_key,
            source=event.source,
            candle_state=event.candle_state,
            entry_price=round(event.entry_price, 12),
            signal_change_pct=round(event.change_pct, 6),
            confidence=event.confidence,
            alert_tier=event.alert_tier,
            rvol=event.rvol,
            peak_time_utc=None,
            peak_time_local=None,
            time_to_peak_minutes=None,
            peak_price=None,
            peak_gain_pct=None,
            latest_close_time_utc=None,
            latest_close_time_local=None,
            latest_close=None,
            latest_close_return_pct=None,
            post_push_candle_count=0,
            fake_breakout_strict=None,
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
    latest_close = closes[-1]
    latest_close_return_pct = (latest_close - event.entry_price) / event.entry_price * 100.0

    return SymbolRunupMetric(
        symbol=event.symbol,
        first_push_time_utc=_format_dt(event.push_time_utc, UTC) or "",
        first_push_time_local=_format_dt(event.push_time_utc, local_tz) or "",
        rule_name=event.rule_name,
        window=event.window,
        signal_direction=event.direction_label,
        signal_direction_key=event.direction_key,
        source=event.source,
        candle_state=event.candle_state,
        entry_price=round(event.entry_price, 12),
        signal_change_pct=round(event.change_pct, 6),
        confidence=event.confidence,
        alert_tier=event.alert_tier,
        rvol=event.rvol,
        peak_time_utc=_format_dt(peak_dt, UTC),
        peak_time_local=_format_dt(peak_dt, local_tz),
        time_to_peak_minutes=round((peak_dt - event.push_time_utc).total_seconds() / 60.0, 3),
        peak_price=round(peak_price, 12),
        peak_gain_pct=round(peak_gain_pct, 6),
        latest_close_time_utc=_format_dt(latest_dt, UTC),
        latest_close_time_local=_format_dt(latest_dt, local_tz),
        latest_close=round(latest_close, 12),
        latest_close_return_pct=round(latest_close_return_pct, 6),
        post_push_candle_count=len(rows),
        fake_breakout_strict=bool(peak_price <= event.entry_price),
        metric_status="ok",
    )


async def _collect_metrics(
    first_pushes: List[PushEvent],
    local_tz: ZoneInfo,
    fetch_limit: int,
    max_concurrency: int,
) -> List[SymbolRunupMetric]:
    if not first_pushes:
        return []

    exchange = _build_exchange()
    mapping = await asyncio.to_thread(_load_market_mapping, exchange)
    now_utc = datetime.now(tz=UTC)
    end_ms = int(now_utc.timestamp() * 1000)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    fetched_rows: Dict[str, List[List[float]]] = {}

    async def fetch_one(event: PushEvent) -> None:
        ccxt_symbol = mapping.get(event.symbol)
        if not ccxt_symbol:
            fetched_rows[event.symbol] = []
            logger.warning("Symbol not found in Binance futures market: %s", event.symbol)
            return

        async with sem:
            fetched_rows[event.symbol] = await _fetch_symbol_rows(
                exchange=exchange,
                ccxt_symbol=ccxt_symbol,
                start_ms=_ceil_minute_ms(event.push_time_utc),
                end_ms=end_ms,
                fetch_limit=fetch_limit,
            )

    await asyncio.gather(*(fetch_one(event) for event in first_pushes))

    metrics = [
        _build_metric(event=event, rows=fetched_rows.get(event.symbol, []), local_tz=local_tz)
        for event in first_pushes
    ]
    metrics.sort(key=lambda item: item.first_push_time_utc)

    try:
        await asyncio.to_thread(exchange.close)
    except Exception:
        logger.debug("Exchange close failed.", exc_info=True)
    return metrics


def _build_summary(
    metrics: List[SymbolRunupMetric],
    focus_symbols: Sequence[str],
    log_coverage_start: Optional[datetime],
    local_tz: ZoneInfo,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    ready = [item for item in metrics if item.metric_status == "ok" and item.fake_breakout_strict is not None]

    def summarize(scope: str, items: List[SymbolRunupMetric]) -> None:
        fake_items = [item for item in items if item.fake_breakout_strict is True]
        rows.append(
            {
                "scope": scope,
                "symbols": len(items),
                "fake_breakout_count": len(fake_items),
                "fake_breakout_ratio_pct": round((len(fake_items) / len(items) * 100.0), 6) if items else 0.0,
                "avg_peak_gain_pct": round(sum((item.peak_gain_pct or 0.0) for item in items) / len(items), 6) if items else 0.0,
                "median_peak_gain_pct": round(float(pd.Series([item.peak_gain_pct for item in items]).median()), 6) if items else 0.0,
                "max_peak_gain_pct": round(max((item.peak_gain_pct or 0.0) for item in items), 6) if items else 0.0,
                "avg_latest_close_return_pct": round(sum((item.latest_close_return_pct or 0.0) for item in items) / len(items), 6) if items else 0.0,
                "log_coverage_start_local": _format_dt(log_coverage_start, local_tz),
            }
        )

    summarize("all_symbols", ready)
    focus_set = {str(symbol or "").strip().upper() for symbol in focus_symbols if str(symbol or "").strip()}
    summarize("focus_symbols", [item for item in ready if item.symbol in focus_set])
    summarize("other_symbols", [item for item in ready if item.symbol not in focus_set])
    return pd.DataFrame(rows)


def _export(
    metrics: List[SymbolRunupMetric],
    summary_df: pd.DataFrame,
    export_dir: Path,
    export_prefix: str,
) -> List[Path]:
    export_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{export_prefix}_{ts}"
    detail_path = export_dir / f"{base}_detail.csv"
    summary_path = export_dir / f"{base}_summary.csv"

    pd.DataFrame([item.__dict__ for item in metrics]).to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return [detail_path, summary_path]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze each symbol's first Telegram push versus the subsequent recent peak."
    )
    parser.add_argument("--log-path", required=True, help="Saved alert_monitor log text file.")
    parser.add_argument("--days", type=int, default=7, help="Parse pushes not older than this lookback.")
    parser.add_argument("--timezone", type=str, default="Asia/Shanghai")
    parser.add_argument("--fetch-limit", type=int, default=1000)
    parser.add_argument("--max-concurrency", type=int, default=3)
    parser.add_argument("--export-dir", type=str, default=default_reports_dir())
    parser.add_argument("--export-prefix", type=str, default="tg_first_push_runup")
    parser.add_argument("--focus-symbols", type=str, default="RAVEUSDT,SIRENUSDT,STOUSDT")
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> None:
    _setup_logging(args.log_level)
    local_tz = ZoneInfo(str(args.timezone or "Asia/Shanghai"))
    cutoff_utc = datetime.now(tz=UTC) - timedelta(days=max(1, int(args.days)))
    log_path = Path(str(args.log_path)).resolve()
    focus_symbols = [item.strip().upper() for item in str(args.focus_symbols or "").split(",") if item.strip()]

    events = _parse_push_events(log_path=log_path, cutoff_utc=cutoff_utc)
    if not events:
        raise RuntimeError(f"No push events parsed from {log_path}")

    first_pushes = _keep_first_push_per_symbol(events)
    metrics = await _collect_metrics(
        first_pushes=first_pushes,
        local_tz=local_tz,
        fetch_limit=max(200, min(1500, int(args.fetch_limit))),
        max_concurrency=max(1, int(args.max_concurrency)),
    )
    summary_df = _build_summary(
        metrics=metrics,
        focus_symbols=focus_symbols,
        log_coverage_start=events[0].push_time_utc if events else None,
        local_tz=local_tz,
    )
    exported = _export(
        metrics=metrics,
        summary_df=summary_df,
        export_dir=Path(str(args.export_dir)).resolve(),
        export_prefix=str(args.export_prefix or "tg_first_push_runup").strip() or "tg_first_push_runup",
    )

    print("\n========== TG FIRST PUSH RUNUP REPORT ==========")
    print(f"log_path: {log_path}")
    print(f"parsed_push_events: {len(events)}")
    print(f"unique_symbols_first_push: {len(first_pushes)}")
    print(f"log_coverage_start_local: {_format_dt(events[0].push_time_utc, local_tz)}")
    focus_map = {item.symbol: item for item in metrics}
    for symbol in focus_symbols:
        item = focus_map.get(symbol)
        if item is None:
            print(f"{symbol}: not_found_in_push_log")
            continue
        print(
            f"{symbol}: first_push={item.first_push_time_local} entry={item.entry_price} "
            f"peak={item.peak_price} peak_gain={item.peak_gain_pct}% fake_breakout={item.fake_breakout_strict}"
        )
    print("exported_files:")
    for path in exported:
        print(f"- {path}")
    print("================================================\n")


def main() -> None:
    args = parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
