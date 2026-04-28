from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import re
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import ccxt
import pandas as pd

try:
    from backtest import _fetch_ohlcv_history
    from paths import default_reports_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.backtest import _fetch_ohlcv_history
    from apps.market_monitor.backend.paths import default_reports_dir


logger = logging.getLogger(__name__)
UTC = timezone.utc
ALERT_QUEUED_RE = re.compile(
    r"^(?P<log_ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\|\s+INFO\s+\|\s+notifier\s+\|\s+"
    r"Alert queued:\s+(?P<symbol>.+?)\s+\|\s+(?P<rule_name>[^|]+?)\s+\|\s+(?P<window>[^|]+?)\s+\|\s+"
    r"(?P<direction>[^|]+?)\s+\|\s+(?P<change_pct>[+-]?\d+(?:\.\d+)?)%\s+\|\s+price=(?P<price>[0-9]+(?:\.\d+)?)\s+\|\s+"
    r"source=(?P<source>[^|]+?)\s+\|\s+candle=(?P<candle_state>[^|]+?)\s+\|\s+conf=(?P<confidence>[^|]+?)\s+\|\s+"
    r"tier=(?P<alert_tier>[^|]+?)\s+\|\s+rvol=(?P<rvol>[^|]+?)\s+\|\s+merged=(?P<merged>\d+)"
)


@dataclass
class PushEvent:
    symbol: str
    rule_name: str
    window: str
    direction_label: str
    direction_key: str
    change_pct: float
    entry_price: float
    source: str
    candle_state: str
    confidence: Optional[float]
    alert_tier: str
    rvol: Optional[float]
    merged_count: int
    push_time_utc: datetime


@dataclass
class FirstPushMetric:
    beijing_date: str
    symbol: str
    push_time_utc: str
    push_time_local: str
    rule_name: str
    window: str
    signal_direction: str
    signal_direction_key: str
    source: str
    candle_state: str
    signal_change_pct: float
    entry_price: float
    confidence: Optional[float]
    alert_tier: str
    rvol: Optional[float]
    merged_count: int
    observation_end_utc: str
    observation_end_local: str
    observation_complete: bool
    post_push_candle_count: int
    peak_time_utc: Optional[str]
    peak_time_local: Optional[str]
    time_to_peak_minutes: Optional[float]
    peak_price: Optional[float]
    best_case_return_pct: Optional[float]
    trough_time_utc: Optional[str]
    trough_time_local: Optional[str]
    time_to_trough_minutes: Optional[float]
    trough_price: Optional[float]
    max_drawdown_pct: Optional[float]
    day_end_close: Optional[float]
    close_return_pct: Optional[float]
    long_win: Optional[bool]
    metric_status: str


def _setup_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_confidence(raw: str) -> Optional[float]:
    text = str(raw or "").strip()
    if not text or text.upper() == "N/A":
        return None
    if "/" in text:
        text = text.split("/", 1)[0].strip()
    return _safe_float(text, default=None)


def _parse_direction_key(label: str) -> str:
    text = str(label or "").strip().lower()
    if text in {"上涨", "up", "long", "bullish"}:
        return "up"
    if text in {"下跌", "down", "short", "bearish"}:
        return "down"
    return "flat"


def _parse_push_events(log_path: Path, cutoff_utc: datetime) -> List[PushEvent]:
    if not log_path.exists():
        raise FileNotFoundError(f"log file not found: {log_path}")

    events: List[PushEvent] = []
    for raw_line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if "Alert queued:" not in line:
            continue
        match = ALERT_QUEUED_RE.match(line)
        if not match:
            continue

        push_time_utc = datetime.strptime(match.group("log_ts"), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        if push_time_utc < cutoff_utc:
            continue

        direction_label = match.group("direction").strip()
        events.append(
            PushEvent(
                symbol=match.group("symbol").strip().upper(),
                rule_name=match.group("rule_name").strip(),
                window=match.group("window").strip(),
                direction_label=direction_label,
                direction_key=_parse_direction_key(direction_label),
                change_pct=float(match.group("change_pct")),
                entry_price=float(match.group("price")),
                source=match.group("source").strip(),
                candle_state=match.group("candle_state").strip(),
                confidence=_parse_confidence(match.group("confidence")),
                alert_tier=match.group("alert_tier").strip(),
                rvol=_safe_float(match.group("rvol"), default=None),
                merged_count=int(match.group("merged")),
                push_time_utc=push_time_utc,
            )
        )

    events.sort(key=lambda item: (item.push_time_utc, item.symbol, item.rule_name, item.window))
    return events


def _keep_first_push_per_symbol_day(events: Iterable[PushEvent], local_tz: ZoneInfo) -> List[PushEvent]:
    first_by_key: Dict[Tuple[str, str], PushEvent] = {}
    for event in sorted(events, key=lambda item: item.push_time_utc):
        local_day = event.push_time_utc.astimezone(local_tz).strftime("%Y-%m-%d")
        key = (event.symbol, local_day)
        if key not in first_by_key:
            first_by_key[key] = event
    ordered = list(first_by_key.values())
    ordered.sort(key=lambda item: (item.push_time_utc, item.symbol))
    return ordered


def _build_exchange() -> ccxt.binance:
    exchange_args: Dict[str, Any] = {
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
    }
    http_proxy = os.getenv("ALERT_HTTP_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_proxy = os.getenv("ALERT_HTTPS_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if http_proxy or https_proxy:
        exchange_args["proxies"] = {
            "http": http_proxy or https_proxy,
            "https": https_proxy or http_proxy,
        }
    return ccxt.binance(exchange_args)


def _normalize_symbol_key(text: str) -> str:
    return str(text or "").strip().upper().replace("/", "")


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

        raw_id = _normalize_symbol_key(str(market.get("id") or ""))
        if raw_id:
            mapping.setdefault(raw_id, ccxt_symbol)

        alt_key = _normalize_symbol_key(str(market.get("symbol") or ""))
        if alt_key:
            mapping.setdefault(alt_key.replace(":USDT", ""), ccxt_symbol)
    return mapping


def _format_dt(dt: Optional[datetime], target_tz: timezone | ZoneInfo) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(target_tz).strftime("%Y-%m-%d %H:%M:%S%z")


def _ceil_minute_ms(dt: datetime) -> int:
    epoch_ms = int(dt.timestamp() * 1000)
    return int(math.ceil(epoch_ms / 60_000.0) * 60_000)


def _observation_end(push_time_utc: datetime, local_tz: ZoneInfo, now_utc: datetime) -> Tuple[datetime, bool]:
    push_time_local = push_time_utc.astimezone(local_tz)
    next_midnight_local = datetime.combine(
        push_time_local.date() + timedelta(days=1),
        dtime.min,
        tzinfo=local_tz,
    )
    next_midnight_utc = next_midnight_local.astimezone(UTC)
    if next_midnight_utc <= now_utc:
        return next_midnight_utc, True
    return now_utc, False


def _slice_rows_by_time(rows: List[List[float]], start_ms: int, end_ms: int) -> List[List[float]]:
    timestamps = [int(row[0]) for row in rows]
    start_idx = bisect_left(timestamps, start_ms)
    end_idx = bisect_left(timestamps, end_ms)
    return rows[start_idx:end_idx]


def _first_index_of_max(values: List[float]) -> int:
    max_value = max(values)
    for idx, value in enumerate(values):
        if value == max_value:
            return idx
    return 0


def _first_index_of_min(values: List[float]) -> int:
    min_value = min(values)
    for idx, value in enumerate(values):
        if value == min_value:
            return idx
    return 0


def _build_metric_for_event(
    event: PushEvent,
    rows: List[List[float]],
    local_tz: ZoneInfo,
    observation_end_utc: datetime,
    observation_complete: bool,
) -> FirstPushMetric:
    push_time_local = event.push_time_utc.astimezone(local_tz)
    base_kwargs = {
        "beijing_date": push_time_local.strftime("%Y-%m-%d"),
        "symbol": event.symbol,
        "push_time_utc": _format_dt(event.push_time_utc, UTC),
        "push_time_local": _format_dt(event.push_time_utc, local_tz),
        "rule_name": event.rule_name,
        "window": event.window,
        "signal_direction": event.direction_label,
        "signal_direction_key": event.direction_key,
        "source": event.source,
        "candle_state": event.candle_state,
        "signal_change_pct": round(event.change_pct, 6),
        "entry_price": round(event.entry_price, 12),
        "confidence": event.confidence,
        "alert_tier": event.alert_tier,
        "rvol": event.rvol,
        "merged_count": event.merged_count,
        "observation_end_utc": _format_dt(observation_end_utc, UTC),
        "observation_end_local": _format_dt(observation_end_utc, local_tz),
        "observation_complete": observation_complete,
        "post_push_candle_count": len(rows),
    }

    if not rows:
        return FirstPushMetric(
            **base_kwargs,
            peak_time_utc=None,
            peak_time_local=None,
            time_to_peak_minutes=None,
            peak_price=None,
            best_case_return_pct=None,
            trough_time_utc=None,
            trough_time_local=None,
            time_to_trough_minutes=None,
            trough_price=None,
            max_drawdown_pct=None,
            day_end_close=None,
            close_return_pct=None,
            long_win=None,
            metric_status="no_post_push_candles",
        )

    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    peak_idx = _first_index_of_max(highs)
    trough_idx = _first_index_of_min(lows)

    peak_row = rows[peak_idx]
    trough_row = rows[trough_idx]
    peak_dt = datetime.fromtimestamp(int(peak_row[0]) / 1000.0, tz=UTC)
    trough_dt = datetime.fromtimestamp(int(trough_row[0]) / 1000.0, tz=UTC)

    peak_price = float(peak_row[2])
    trough_price = float(trough_row[3])
    last_close = closes[-1]

    best_case_return_pct = (peak_price - event.entry_price) / event.entry_price * 100.0
    max_drawdown_pct = (trough_price - event.entry_price) / event.entry_price * 100.0
    close_return_pct = (last_close - event.entry_price) / event.entry_price * 100.0

    metric_status = "complete_day" if observation_complete else "partial_day"
    return FirstPushMetric(
        **base_kwargs,
        peak_time_utc=_format_dt(peak_dt, UTC),
        peak_time_local=_format_dt(peak_dt, local_tz),
        time_to_peak_minutes=round((peak_dt - event.push_time_utc).total_seconds() / 60.0, 3),
        peak_price=round(peak_price, 12),
        best_case_return_pct=round(best_case_return_pct, 6),
        trough_time_utc=_format_dt(trough_dt, UTC),
        trough_time_local=_format_dt(trough_dt, local_tz),
        time_to_trough_minutes=round((trough_dt - event.push_time_utc).total_seconds() / 60.0, 3),
        trough_price=round(trough_price, 12),
        max_drawdown_pct=round(max_drawdown_pct, 6),
        day_end_close=round(last_close, 12),
        close_return_pct=round(close_return_pct, 6),
        long_win=bool(close_return_pct > 0),
        metric_status=metric_status,
    )


async def _fetch_symbol_rows(
    exchange,
    ccxt_symbol: str,
    start_ms: int,
    end_ms: int,
    fetch_limit: int,
) -> List[List[float]]:
    if end_ms <= start_ms:
        return []
    return await _fetch_ohlcv_history(
        exchange=exchange,
        ccxt_symbol=ccxt_symbol,
        window="1m",
        since_ms=start_ms,
        until_ms=end_ms - 1,
        limit=fetch_limit,
    )


async def _build_metrics(
    first_pushes: List[PushEvent],
    local_tz: ZoneInfo,
    fetch_limit: int,
    max_concurrency: int,
) -> List[FirstPushMetric]:
    if not first_pushes:
        return []

    exchange = _build_exchange()
    mapping = await asyncio.to_thread(_load_market_mapping, exchange)
    now_utc = datetime.now(tz=UTC)

    per_symbol_ranges: Dict[str, Dict[str, int]] = {}
    per_event_window: Dict[Tuple[str, str], Tuple[int, int, bool]] = {}

    for event in first_pushes:
        observation_end_utc, observation_complete = _observation_end(event.push_time_utc, local_tz, now_utc)
        start_ms = _ceil_minute_ms(event.push_time_utc)
        end_ms = int(observation_end_utc.timestamp() * 1000)
        per_event_window[(event.symbol, _format_dt(event.push_time_utc, UTC) or "")] = (
            start_ms,
            end_ms,
            observation_complete,
        )
        bucket = per_symbol_ranges.setdefault(
            event.symbol,
            {"start_ms": start_ms, "end_ms": end_ms},
        )
        bucket["start_ms"] = min(bucket["start_ms"], start_ms)
        bucket["end_ms"] = max(bucket["end_ms"], end_ms)

    sem = asyncio.Semaphore(max(1, max_concurrency))
    fetched_rows: Dict[str, List[List[float]]] = {}

    async def fetch_one(symbol: str, start_ms: int, end_ms: int) -> None:
        ccxt_symbol = mapping.get(symbol)
        if not ccxt_symbol:
            fetched_rows[symbol] = []
            logger.warning("Symbol not found in futures markets, skip candle fetch: %s", symbol)
            return

        async with sem:
            rows = await _fetch_symbol_rows(
                exchange=exchange,
                ccxt_symbol=ccxt_symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                fetch_limit=fetch_limit,
            )
        fetched_rows[symbol] = rows

    tasks = [
        fetch_one(symbol, int(window["start_ms"]), int(window["end_ms"]))
        for symbol, window in per_symbol_ranges.items()
    ]
    await asyncio.gather(*tasks)

    metrics: List[FirstPushMetric] = []
    for event in first_pushes:
        key = (event.symbol, _format_dt(event.push_time_utc, UTC) or "")
        start_ms, end_ms, observation_complete = per_event_window[key]
        rows = _slice_rows_by_time(fetched_rows.get(event.symbol, []), start_ms, end_ms)

        if event.symbol not in mapping:
            push_time_local = event.push_time_utc.astimezone(local_tz)
            observation_end_utc = datetime.fromtimestamp(end_ms / 1000.0, tz=UTC)
            metrics.append(
                FirstPushMetric(
                    beijing_date=push_time_local.strftime("%Y-%m-%d"),
                    symbol=event.symbol,
                    push_time_utc=_format_dt(event.push_time_utc, UTC) or "",
                    push_time_local=_format_dt(event.push_time_utc, local_tz) or "",
                    rule_name=event.rule_name,
                    window=event.window,
                    signal_direction=event.direction_label,
                    signal_direction_key=event.direction_key,
                    source=event.source,
                    candle_state=event.candle_state,
                    signal_change_pct=round(event.change_pct, 6),
                    entry_price=round(event.entry_price, 12),
                    confidence=event.confidence,
                    alert_tier=event.alert_tier,
                    rvol=event.rvol,
                    merged_count=event.merged_count,
                    observation_end_utc=_format_dt(observation_end_utc, UTC) or "",
                    observation_end_local=_format_dt(observation_end_utc, local_tz) or "",
                    observation_complete=observation_complete,
                    post_push_candle_count=0,
                    peak_time_utc=None,
                    peak_time_local=None,
                    time_to_peak_minutes=None,
                    peak_price=None,
                    best_case_return_pct=None,
                    trough_time_utc=None,
                    trough_time_local=None,
                    time_to_trough_minutes=None,
                    trough_price=None,
                    max_drawdown_pct=None,
                    day_end_close=None,
                    close_return_pct=None,
                    long_win=None,
                    metric_status="symbol_not_found",
                )
            )
            continue

        metrics.append(
            _build_metric_for_event(
                event=event,
                rows=rows,
                local_tz=local_tz,
                observation_end_utc=datetime.fromtimestamp(end_ms / 1000.0, tz=UTC),
                observation_complete=observation_complete,
            )
        )

    metrics.sort(key=lambda item: (item.beijing_date, item.symbol, item.push_time_utc))
    try:
        await asyncio.to_thread(exchange.close)
    except Exception:
        logger.debug("Exchange close failed.", exc_info=True)
    return metrics


def _median_or_zero(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(median(values))


def _avg_or_zero(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _summarize_metrics(
    metrics: List[FirstPushMetric],
    requested_days: int,
    log_coverage_start: Optional[datetime],
    log_coverage_end: Optional[datetime],
    local_tz: ZoneInfo,
) -> pd.DataFrame:
    rows = []
    data_ready = [item for item in metrics if item.close_return_pct is not None]
    complete_day = [item for item in data_ready if item.metric_status == "complete_day"]
    partial_day = [item for item in data_ready if item.metric_status == "partial_day"]

    def add_scope(scope: str, items: List[FirstPushMetric]) -> None:
        close_returns = [float(item.close_return_pct) for item in items if item.close_return_pct is not None]
        peak_returns = [float(item.best_case_return_pct) for item in items if item.best_case_return_pct is not None]
        drawdowns = [float(item.max_drawdown_pct) for item in items if item.max_drawdown_pct is not None]
        win_count = sum(1 for item in items if item.long_win is True)
        loss_count = sum(1 for item in items if item.long_win is False)
        evaluated = win_count + loss_count
        win_rate_pct = (win_count / evaluated * 100.0) if evaluated else 0.0

        rows.append(
            {
                "scope": scope,
                "requested_days": requested_days,
                "log_coverage_start_local": _format_dt(log_coverage_start, local_tz),
                "log_coverage_end_local": _format_dt(log_coverage_end, local_tz),
                "rows": len(items),
                "evaluated_rows": evaluated,
                "wins": win_count,
                "losses": loss_count,
                "win_rate_pct": round(win_rate_pct, 6),
                "avg_best_case_return_pct": round(_avg_or_zero(peak_returns), 6),
                "median_best_case_return_pct": round(_median_or_zero(peak_returns), 6),
                "best_peak_return_pct": round(max(peak_returns), 6) if peak_returns else 0.0,
                "avg_close_return_pct": round(_avg_or_zero(close_returns), 6),
                "median_close_return_pct": round(_median_or_zero(close_returns), 6),
                "avg_max_drawdown_pct": round(_avg_or_zero(drawdowns), 6),
                "worst_max_drawdown_pct": round(min(drawdowns), 6) if drawdowns else 0.0,
            }
        )

    add_scope("all_data_ready", data_ready)
    add_scope("complete_day_only", complete_day)
    add_scope("partial_day_only", partial_day)
    add_scope(
        "complete_day_signal_up",
        [item for item in complete_day if item.signal_direction_key == "up"],
    )
    add_scope(
        "complete_day_signal_down",
        [item for item in complete_day if item.signal_direction_key == "down"],
    )
    add_scope(
        "complete_day_signal_flat_or_other",
        [item for item in complete_day if item.signal_direction_key not in {"up", "down"}],
    )
    return pd.DataFrame(rows)


def _export_reports(
    metrics: List[FirstPushMetric],
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
        description="Analyze real Telegram push accuracy from alert_monitor logs.",
    )
    parser.add_argument("--log-path", required=True, help="Path to saved alert_monitor log text file.")
    parser.add_argument("--days", type=int, default=7, help="Lookback days based on log timestamps.")
    parser.add_argument("--timezone", type=str, default="Asia/Shanghai", help="Timezone for day grouping.")
    parser.add_argument("--fetch-limit", type=int, default=1000, help="Per-request 1m candle fetch size.")
    parser.add_argument("--max-concurrency", type=int, default=4, help="Concurrent symbol fetches.")
    parser.add_argument("--export-dir", type=str, default=default_reports_dir(), help="Directory for CSV exports.")
    parser.add_argument(
        "--export-prefix",
        type=str,
        default="tg_push_accuracy",
        help="Prefix for generated CSV files.",
    )
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> None:
    _setup_logging(args.log_level)
    local_tz = ZoneInfo(str(args.timezone or "Asia/Shanghai"))
    cutoff_utc = datetime.now(tz=UTC) - timedelta(days=max(1, int(args.days)))
    log_path = Path(str(args.log_path)).resolve()

    events = _parse_push_events(log_path=log_path, cutoff_utc=cutoff_utc)
    if not events:
        raise RuntimeError(f"No queued push events parsed from {log_path}")

    first_pushes = _keep_first_push_per_symbol_day(events=events, local_tz=local_tz)
    metrics = await _build_metrics(
        first_pushes=first_pushes,
        local_tz=local_tz,
        fetch_limit=max(200, min(1500, int(args.fetch_limit))),
        max_concurrency=max(1, int(args.max_concurrency)),
    )

    summary_df = _summarize_metrics(
        metrics=metrics,
        requested_days=max(1, int(args.days)),
        log_coverage_start=events[0].push_time_utc if events else None,
        log_coverage_end=events[-1].push_time_utc if events else None,
        local_tz=local_tz,
    )
    exported = _export_reports(
        metrics=metrics,
        summary_df=summary_df,
        export_dir=Path(str(args.export_dir)).resolve(),
        export_prefix=str(args.export_prefix or "tg_push_accuracy").strip() or "tg_push_accuracy",
    )

    complete_day_summary = summary_df.loc[summary_df["scope"] == "complete_day_only"]
    if complete_day_summary.empty:
        headline = {}
    else:
        headline = complete_day_summary.iloc[0].to_dict()

    print("\n========== TG PUSH ACCURACY REPORT ==========")
    print(f"log_path: {log_path}")
    print(f"parsed_push_events: {len(events)}")
    print(f"first_push_rows: {len(first_pushes)}")
    print(f"log_coverage_local: {_format_dt(events[0].push_time_utc, local_tz)} -> {_format_dt(events[-1].push_time_utc, local_tz)}")
    if headline:
        print(
            "complete_day_only: "
            f"rows={int(headline.get('rows', 0))} "
            f"win_rate={float(headline.get('win_rate_pct', 0.0)):.2f}% "
            f"avg_best_case={float(headline.get('avg_best_case_return_pct', 0.0)):.2f}% "
            f"avg_close_return={float(headline.get('avg_close_return_pct', 0.0)):.2f}% "
            f"worst_drawdown={float(headline.get('worst_max_drawdown_pct', 0.0)):.2f}%"
        )
    print("exported_files:")
    for path in exported:
        print(f"- {path}")
    print("=============================================\n")


def main() -> None:
    args = parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
