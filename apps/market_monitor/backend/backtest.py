from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    from alert_config import AlertConfig, load_alert_config
    from data_feed import BinanceKlineDataFeed
    from indicators import ohlcv_to_dataframe
    from paths import default_config_path, default_reports_dir
    from rule_engine import MarketSnapshot, evaluate_snapshot
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import AlertConfig, load_alert_config
    from apps.market_monitor.backend.data_feed import BinanceKlineDataFeed
    from apps.market_monitor.backend.indicators import ohlcv_to_dataframe
    from apps.market_monitor.backend.paths import default_config_path, default_reports_dir
    from apps.market_monitor.backend.rule_engine import MarketSnapshot, evaluate_snapshot

logger = logging.getLogger(__name__)

WINDOW_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
}

DEFAULT_HORIZON_BY_WINDOW = {
    "1m": 10,
    "5m": 6,
    "15m": 4,
    "30m": 3,
    "1h": 2,
}

DEFAULT_CONFIRM_PCT_BY_WINDOW = {
    "1m": 0.6,
    "5m": 0.9,
    "15m": 1.3,
    "30m": 1.8,
    "1h": 2.5,
}


@dataclass
class BacktestSettings:
    days: int
    report_days: int
    max_symbols: int
    max_concurrency: int
    fetch_limit: int
    windows: List[str]
    horizon_by_window: Dict[str, int]
    confirm_pct_by_window: Dict[str, float]
    top_n: int
    min_samples_for_rate: int
    export_dir: str
    export_prefix: str


@dataclass
class BacktestEvent:
    symbol: str
    window: str
    rule_name: str
    direction: str
    level: str
    event_time_ms: int
    entry_price: float
    change_pct: float
    follow_through_success: Optional[bool]


def _parse_windows(raw: str, fallback: Iterable[str]) -> List[str]:
    windows = [x.strip() for x in str(raw or "").split(",") if x.strip()]
    if not windows:
        windows = list(fallback)
    return [w for w in windows if w in WINDOW_MS]


def _build_settings(config: AlertConfig, args: argparse.Namespace) -> BacktestSettings:
    windows = _parse_windows(
        args.windows or os.getenv("BACKTEST_WINDOWS", ""),
        fallback=["1m", "5m"],
    )

    max_symbols = args.max_symbols or int(os.getenv("BACKTEST_MAX_SYMBOLS", "24"))
    max_concurrency = args.max_concurrency or int(os.getenv("BACKTEST_MAX_CONCURRENCY", "4"))
    fetch_limit = args.fetch_limit or int(os.getenv("BACKTEST_FETCH_LIMIT", "1000"))
    top_n = args.top_n if args.top_n is not None else int(os.getenv("BACKTEST_TOP_N", "10"))
    min_samples_for_rate = (
        args.min_samples_for_rate
        if args.min_samples_for_rate is not None
        else int(os.getenv("BACKTEST_MIN_SAMPLES_FOR_RATE", "3"))
    )
    export_dir = str(args.export_dir or "").strip()
    export_prefix = str(args.export_prefix or "backtest_report").strip() or "backtest_report"

    return BacktestSettings(
        days=max(1, args.days),
        report_days=max(1, args.report_days),
        max_symbols=max(0, max_symbols),
        max_concurrency=max(1, max_concurrency),
        fetch_limit=max(100, min(1500, fetch_limit)),
        windows=windows,
        horizon_by_window=dict(DEFAULT_HORIZON_BY_WINDOW),
        confirm_pct_by_window=dict(DEFAULT_CONFIRM_PCT_BY_WINDOW),
        top_n=max(0, int(top_n)),
        min_samples_for_rate=max(1, int(min_samples_for_rate)),
        export_dir=export_dir,
        export_prefix=export_prefix,
    )


def _setup_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def _fetch_ohlcv_history(
    exchange,
    ccxt_symbol: str,
    window: str,
    since_ms: int,
    until_ms: int,
    limit: int,
) -> List[List[float]]:
    frame_ms = WINDOW_MS[window]
    all_rows: List[List[float]] = []
    cursor = since_ms
    last_seen_ts = -1

    while cursor < until_ms:
        rows = await _fetch_ohlcv_with_retry(
            exchange=exchange,
            ccxt_symbol=ccxt_symbol,
            window=window,
            since_ms=cursor,
            limit=limit,
        )
        if not rows:
            break

        appended = 0
        for row in rows:
            ts = int(row[0])
            if ts < since_ms or ts > until_ms:
                continue
            if all_rows and ts <= int(all_rows[-1][0]):
                continue
            all_rows.append(row)
            appended += 1

        batch_last_ts = int(rows[-1][0])
        if batch_last_ts <= last_seen_ts:
            break
        last_seen_ts = batch_last_ts

        cursor = batch_last_ts + frame_ms
        if appended == 0:
            break

    return all_rows


async def _fetch_ohlcv_with_retry(
    exchange,
    ccxt_symbol: str,
    window: str,
    since_ms: int,
    limit: int,
    max_attempts: int = 4,
) -> List[List[float]]:
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            return await asyncio.to_thread(
                exchange.fetch_ohlcv,
                ccxt_symbol,
                window,
                since_ms,
                limit,
            )
        except Exception as exc:
            text = str(exc)
            if "-1003" in text or "418" in text or "rate limit" in text.lower():
                wait_s = min(8.0, 1.5 * attempt)
                logger.warning(
                    "Rate-limited during backtest fetch: symbol=%s window=%s attempt=%s/%s wait=%.1fs",
                    ccxt_symbol,
                    window,
                    attempt,
                    max_attempts,
                    wait_s,
                )
                await asyncio.sleep(wait_s)
                continue
            logger.warning(
                "fetch_ohlcv failed: symbol=%s window=%s since=%s err=%s",
                ccxt_symbol,
                window,
                since_ms,
                exc,
            )
            return []
    return []


def _prepare_indicator_frame(
    ohlcv_rows: List[List[float]],
    config: AlertConfig,
) -> Optional[pd.DataFrame]:
    df = ohlcv_to_dataframe(ohlcv_rows)
    if df.empty:
        return None

    close = df["close"]
    high = df["high"]
    low = df["low"]

    atr_period = int(config.atr.get("period", 14))
    rsi_period = int(config.indicators.get("rsi_period", 14))
    bb_period = int(config.indicators.get("bb_period", 20))
    bb_std = float(config.indicators.get("bb_std", 2.0))
    macd_fast = int(config.indicators.get("macd_fast", 12))
    macd_slow = int(config.indicators.get("macd_slow", 26))
    macd_signal = int(config.indicators.get("macd_signal", 9))
    long_rule = config.rule("long_anomaly")
    ma_period = int(long_rule.get("ma_period", config.indicators.get("ma_period", 20)))
    std_period = int(long_rule.get("std_period", config.indicators.get("std_period", 20)))

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.rolling(window=atr_period, min_periods=atr_period).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / max(1, rsi_period), min_periods=rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / max(1, rsi_period), min_periods=rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    df["rsi"] = 100 - (100 / (1 + rs))

    bb_mid = close.rolling(window=bb_period, min_periods=bb_period).mean()
    bb_dev = close.rolling(window=bb_period, min_periods=bb_period).std(ddof=0)
    df["bb_upper"] = bb_mid + bb_std * bb_dev
    df["bb_lower"] = bb_mid - bb_std * bb_dev

    ema_fast = close.ewm(span=macd_fast, adjust=False, min_periods=macd_fast).mean()
    ema_slow = close.ewm(span=macd_slow, adjust=False, min_periods=macd_slow).mean()
    macd_line = ema_fast - ema_slow
    macd_signal_line = macd_line.ewm(span=macd_signal, adjust=False, min_periods=macd_signal).mean()
    df["macd_hist"] = macd_line - macd_signal_line

    df["ma"] = close.rolling(window=ma_period, min_periods=ma_period).mean()
    df["std_dev"] = close.rolling(window=std_period, min_periods=std_period).std(ddof=0)

    volume_lookback = int(config.data_feed.get("volume_lookback", 20))
    df["avg_volume"] = df["volume"].shift(1).rolling(window=volume_lookback, min_periods=1).mean()
    return df


def _safe_series_float(value) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(v):
        return None
    return v


def _evaluate_follow_through(
    candles: List[List[float]],
    idx: int,
    window: str,
    direction: str,
    settings: BacktestSettings,
) -> Optional[bool]:
    horizon = max(1, int(settings.horizon_by_window.get(window, 3)))
    confirm_pct = float(settings.confirm_pct_by_window.get(window, 0.6))

    if idx + 1 >= len(candles):
        return None
    end_idx = min(len(candles), idx + 1 + horizon)
    if end_idx <= idx + 1:
        return None

    entry = float(candles[idx][4])
    future_rows = candles[idx + 1 : end_idx]
    if not future_rows:
        return None

    if direction == "up":
        max_high = max(float(row[2]) for row in future_rows)
        follow_pct = (max_high - entry) / entry * 100.0
    elif direction == "down":
        min_low = min(float(row[3]) for row in future_rows)
        follow_pct = (entry - min_low) / entry * 100.0
    else:
        return None

    return follow_pct >= confirm_pct


def _simulate_window(
    symbol: str,
    window: str,
    ohlcv_rows: List[List[float]],
    frame: pd.DataFrame,
    config: AlertConfig,
    settings: BacktestSettings,
) -> List[BacktestEvent]:
    events: List[BacktestEvent] = []
    if len(ohlcv_rows) < 40 or frame.empty:
        return events

    min_start_idx = max(35, int(config.data_feed.get("volume_lookback", 20)) + 2)
    cooldown_expiry_ms: Dict[str, int] = {}
    high_extra = config.high_priority_cooldown_extra_minutes()

    for idx in range(min_start_idx, len(ohlcv_rows)):
        row = ohlcv_rows[idx]
        event_time_ms = int(row[0])

        snapshot = MarketSnapshot(
            symbol=symbol,
            window=window,
            open_price=float(row[1]),
            close_price=float(row[4]),
            volume=float(row[5]),
            avg_volume=float(frame["avg_volume"].iloc[idx]) if not pd.isna(frame["avg_volume"].iloc[idx]) else 0.0,
            event_time=datetime.fromtimestamp(event_time_ms / 1000.0, tz=timezone.utc),
            indicators={
                "atr": _safe_series_float(frame["atr"].iloc[idx]),
                "rsi": _safe_series_float(frame["rsi"].iloc[idx]),
                "bb_upper": _safe_series_float(frame["bb_upper"].iloc[idx]),
                "bb_lower": _safe_series_float(frame["bb_lower"].iloc[idx]),
                "macd_hist": _safe_series_float(frame["macd_hist"].iloc[idx]),
                "ma": _safe_series_float(frame["ma"].iloc[idx]),
                "std_dev": _safe_series_float(frame["std_dev"].iloc[idx]),
            },
        )

        signal_events = evaluate_snapshot(snapshot, config)
        if not signal_events:
            continue

        for event in signal_events:
            level = str(event.get("level", "medium")).lower().strip()
            direction = str(event.get("direction", "flat"))
            cooldown_minutes = config.cooldown_for(window)
            if level == "high":
                cooldown_minutes += high_extra

            cooldown_key = f"{symbol}:{event['rule_name']}:{window}:{direction}"
            if event_time_ms < cooldown_expiry_ms.get(cooldown_key, 0):
                continue
            cooldown_expiry_ms[cooldown_key] = event_time_ms + int(cooldown_minutes * 60_000)

            follow_through_success = _evaluate_follow_through(
                candles=ohlcv_rows,
                idx=idx,
                window=window,
                direction=direction,
                settings=settings,
            )
            events.append(
                BacktestEvent(
                    symbol=symbol,
                    window=window,
                    rule_name=str(event["rule_name"]),
                    direction=direction,
                    level=level,
                    event_time_ms=event_time_ms,
                    entry_price=float(event["price"]),
                    change_pct=float(event["change_pct"]),
                    follow_through_success=follow_through_success,
                )
            )

    return events


async def _run_backtest(config: AlertConfig, settings: BacktestSettings) -> List[BacktestEvent]:
    feed = BinanceKlineDataFeed(config=config)
    await feed.initialize()

    symbols = list(feed.symbol_mapping.keys())
    if settings.max_symbols > 0:
        symbols = symbols[: settings.max_symbols]

    if not symbols:
        logger.warning("No symbols selected for backtest.")
        return []

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - settings.days * 24 * 60 * 60 * 1000

    logger.info(
        "Backtest start: symbols=%s windows=%s days=%s report_days=%s",
        len(symbols),
        settings.windows,
        settings.days,
        settings.report_days,
    )

    sem = asyncio.Semaphore(settings.max_concurrency)
    events: List[BacktestEvent] = []

    async def run_symbol_window(symbol: str, window: str) -> List[BacktestEvent]:
        ccxt_symbol = feed.symbol_mapping.get(symbol)
        if not ccxt_symbol:
            return []

        async with sem:
            candles = await _fetch_ohlcv_history(
                exchange=feed.exchange,
                ccxt_symbol=ccxt_symbol,
                window=window,
                since_ms=since_ms,
                until_ms=now_ms,
                limit=settings.fetch_limit,
            )

        if len(candles) < 40:
            return []
        frame = _prepare_indicator_frame(candles, config)
        if frame is None:
            return []
        return _simulate_window(symbol, window, candles, frame, config, settings)

    tasks = [
        run_symbol_window(symbol, window)
        for symbol in symbols
        for window in settings.windows
    ]
    for batch in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(batch, Exception):
            logger.warning("Backtest task failed: %s", batch)
            continue
        events.extend(batch)

    events.sort(key=lambda x: x.event_time_ms)
    return events


def _calc_overall_metrics(events: List[BacktestEvent]) -> Dict[str, float]:
    evaluated = [e for e in events if e.follow_through_success is not None]
    false_positive = [e for e in evaluated if e.follow_through_success is False]
    success = [e for e in evaluated if e.follow_through_success is True]
    evaluated_count = len(evaluated)
    fp_rate = (len(false_positive) / evaluated_count * 100.0) if evaluated_count else 0.0
    hit_rate = (len(success) / evaluated_count * 100.0) if evaluated_count else 0.0

    return {
        "alerts": len(events),
        "evaluated": evaluated_count,
        "success": len(success),
        "false_positive": len(false_positive),
        "false_positive_rate_pct": fp_rate,
        "hit_rate_pct": hit_rate,
    }


def _aggregate_by(
    events: List[BacktestEvent],
    key_fn: Callable[[BacktestEvent], str],
) -> Dict[str, Dict[str, float]]:
    buckets: Dict[str, Dict[str, float]] = {}
    for event in events:
        key = key_fn(event)
        if not key:
            key = "UNKNOWN"

        bucket = buckets.setdefault(
            key,
            {
                "alerts": 0,
                "evaluated": 0,
                "success": 0,
                "false_positive": 0,
                "hit_rate_pct": 0.0,
                "false_positive_rate_pct": 0.0,
                "avg_abs_change_pct": 0.0,
                "_abs_change_sum": 0.0,
            },
        )
        bucket["alerts"] += 1
        bucket["_abs_change_sum"] += abs(float(event.change_pct))

        if event.follow_through_success is None:
            continue

        bucket["evaluated"] += 1
        if event.follow_through_success:
            bucket["success"] += 1
        else:
            bucket["false_positive"] += 1

    for bucket in buckets.values():
        alerts = int(bucket["alerts"])
        evaluated = int(bucket["evaluated"])
        if alerts > 0:
            bucket["avg_abs_change_pct"] = float(bucket["_abs_change_sum"]) / alerts
        if evaluated > 0:
            bucket["hit_rate_pct"] = float(bucket["success"]) / evaluated * 100.0
            bucket["false_positive_rate_pct"] = float(bucket["false_positive"]) / evaluated * 100.0
        bucket.pop("_abs_change_sum", None)
    return buckets


def _to_rank_rows(
    buckets: Dict[str, Dict[str, float]],
    name_key: str,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for name, bucket in buckets.items():
        row: Dict[str, float] = {
            name_key: name,  # type: ignore[assignment]
            "alerts": int(bucket.get("alerts", 0)),
            "evaluated": int(bucket.get("evaluated", 0)),
            "success": int(bucket.get("success", 0)),
            "false_positive": int(bucket.get("false_positive", 0)),
            "hit_rate_pct": float(bucket.get("hit_rate_pct", 0.0)),
            "false_positive_rate_pct": float(bucket.get("false_positive_rate_pct", 0.0)),
            "avg_abs_change_pct": float(bucket.get("avg_abs_change_pct", 0.0)),
        }
        rows.append(row)
    return rows


def _top_rows(
    rows: List[Dict[str, float]],
    top_n: int,
    sort_key: Callable[[Dict[str, float]], Any],
) -> List[Dict[str, float]]:
    ordered = sorted(rows, key=sort_key)
    if top_n <= 0:
        return ordered
    return ordered[:top_n]


def _summarize(
    events: List[BacktestEvent],
    report_days: int,
    top_n: int,
    min_samples_for_rate: int,
) -> Dict[str, object]:
    now_ms = int(time.time() * 1000)
    report_since_ms = now_ms - report_days * 24 * 60 * 60 * 1000

    report_events = [e for e in events if e.event_time_ms >= report_since_ms]
    overall = _calc_overall_metrics(events)
    report = _calc_overall_metrics(report_events)

    by_rule_report = _aggregate_by(report_events, key_fn=lambda e: e.rule_name)
    by_symbol_report = _aggregate_by(report_events, key_fn=lambda e: e.symbol)
    by_window_report = _aggregate_by(report_events, key_fn=lambda e: e.window)
    by_level_report = _aggregate_by(report_events, key_fn=lambda e: e.level)

    symbol_rows = _to_rank_rows(by_symbol_report, name_key="symbol")
    rule_rows = _to_rank_rows(by_rule_report, name_key="rule_name")

    top_symbols_by_alerts = _top_rows(
        symbol_rows,
        top_n=top_n,
        sort_key=lambda row: (
            -int(row["alerts"]),
            -float(row["avg_abs_change_pct"]),
            str(row["symbol"]),
        ),
    )

    eligible_symbol_rows = [r for r in symbol_rows if int(r["evaluated"]) >= max(1, min_samples_for_rate)]
    top_symbols_by_hit_rate = _top_rows(
        eligible_symbol_rows,
        top_n=top_n,
        sort_key=lambda row: (
            -float(row["hit_rate_pct"]),
            -int(row["evaluated"]),
            -int(row["alerts"]),
            str(row["symbol"]),
        ),
    )
    top_symbols_by_false_positive_rate = _top_rows(
        eligible_symbol_rows,
        top_n=top_n,
        sort_key=lambda row: (
            -float(row["false_positive_rate_pct"]),
            -int(row["evaluated"]),
            -int(row["alerts"]),
            str(row["symbol"]),
        ),
    )

    top_rules_by_alerts = _top_rows(
        rule_rows,
        top_n=top_n,
        sort_key=lambda row: (
            -int(row["alerts"]),
            -float(row["avg_abs_change_pct"]),
            str(row["rule_name"]),
        ),
    )

    return {
        # Backward-compatible keys
        "alerts_30d": int(overall["alerts"]),
        "evaluated_30d": int(overall["evaluated"]),
        "false_positive_30d": int(overall["false_positive"]),
        "false_positive_rate_30d": float(overall["false_positive_rate_pct"]),
        "alerts_7d": int(report["alerts"]),
        "evaluated_7d": int(report["evaluated"]),
        "false_positive_7d": int(report["false_positive"]),
        "false_positive_rate_7d": float(report["false_positive_rate_pct"]),
        "by_rule_7d": by_rule_report,
        # Enhanced report payload
        "report_days": report_days,
        "overall": overall,
        "report": report,
        "by_rule_report": by_rule_report,
        "by_symbol_report": by_symbol_report,
        "by_window_report": by_window_report,
        "by_level_report": by_level_report,
        "top_symbols_by_alerts": top_symbols_by_alerts,
        "top_symbols_by_hit_rate": top_symbols_by_hit_rate,
        "top_symbols_by_false_positive_rate": top_symbols_by_false_positive_rate,
        "top_rules_by_alerts": top_rules_by_alerts,
    }


def _print_bucket_breakdown(
    title: str,
    buckets: Dict[str, Dict[str, float]],
) -> None:
    if not buckets:
        return
    print(f"\n{title}:")
    for name, bucket in sorted(buckets.items(), key=lambda x: (-int(x[1].get("alerts", 0)), x[0])):
        print(
            f"- {name}: alerts={int(bucket.get('alerts', 0))}, "
            f"hit_rate={float(bucket.get('hit_rate_pct', 0.0)):.2f}%, "
            f"fp_rate={float(bucket.get('false_positive_rate_pct', 0.0)):.2f}% "
            f"({int(bucket.get('false_positive', 0))}/{int(bucket.get('evaluated', 0))}), "
            f"avg_abs_change={float(bucket.get('avg_abs_change_pct', 0.0)):.2f}%"
        )


def _print_top_rows(title: str, rows: List[Dict[str, float]], name_key: str) -> None:
    if not rows:
        return
    print(f"\n{title}:")
    for idx, row in enumerate(rows, start=1):
        print(
            f"{idx}. {row.get(name_key)} | alerts={int(row.get('alerts', 0))}, "
            f"hit_rate={float(row.get('hit_rate_pct', 0.0)):.2f}%, "
            f"fp_rate={float(row.get('false_positive_rate_pct', 0.0)):.2f}% "
            f"({int(row.get('false_positive', 0))}/{int(row.get('evaluated', 0))}), "
            f"avg_abs_change={float(row.get('avg_abs_change_pct', 0.0)):.2f}%"
        )


def _export_reports(
    events: List[BacktestEvent],
    summary: Dict[str, object],
    settings: BacktestSettings,
) -> List[Path]:
    if not settings.export_dir:
        return []

    export_dir = Path(settings.export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = settings.export_prefix
    base_name = f"{prefix}_{ts}"
    output_paths: List[Path] = []

    event_rows: List[Dict[str, object]] = []
    for event in events:
        event_rows.append(
            {
                "event_time": datetime.fromtimestamp(event.event_time_ms / 1000.0, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S%z"
                ),
                "event_time_ms": event.event_time_ms,
                "symbol": event.symbol,
                "window": event.window,
                "rule_name": event.rule_name,
                "direction": event.direction,
                "level": event.level,
                "entry_price": event.entry_price,
                "change_pct": event.change_pct,
                "follow_through_success": event.follow_through_success,
            }
        )

    events_csv = export_dir / f"{base_name}_events.csv"
    pd.DataFrame(event_rows).to_csv(events_csv, index=False, encoding="utf-8-sig")
    output_paths.append(events_csv)

    report_metrics = summary.get("report", {}) if isinstance(summary.get("report"), dict) else {}
    overall_metrics = summary.get("overall", {}) if isinstance(summary.get("overall"), dict) else {}
    overview_rows = [
        {
            "scope": f"past_{settings.days}_days",
            "alerts": int(overall_metrics.get("alerts", 0)),
            "evaluated": int(overall_metrics.get("evaluated", 0)),
            "success": int(overall_metrics.get("success", 0)),
            "false_positive": int(overall_metrics.get("false_positive", 0)),
            "hit_rate_pct": float(overall_metrics.get("hit_rate_pct", 0.0)),
            "false_positive_rate_pct": float(overall_metrics.get("false_positive_rate_pct", 0.0)),
        },
        {
            "scope": f"past_{settings.report_days}_days",
            "alerts": int(report_metrics.get("alerts", 0)),
            "evaluated": int(report_metrics.get("evaluated", 0)),
            "success": int(report_metrics.get("success", 0)),
            "false_positive": int(report_metrics.get("false_positive", 0)),
            "hit_rate_pct": float(report_metrics.get("hit_rate_pct", 0.0)),
            "false_positive_rate_pct": float(report_metrics.get("false_positive_rate_pct", 0.0)),
        },
    ]
    overview_csv = export_dir / f"{base_name}_overview.csv"
    pd.DataFrame(overview_rows).to_csv(overview_csv, index=False, encoding="utf-8-sig")
    output_paths.append(overview_csv)

    for key, name_col, suffix in [
        ("by_rule_report", "rule_name", "by_rule"),
        ("by_symbol_report", "symbol", "by_symbol"),
        ("by_window_report", "window", "by_window"),
        ("by_level_report", "level", "by_level"),
    ]:
        buckets = summary.get(key, {})
        if not isinstance(buckets, dict):
            continue
        rows: List[Dict[str, object]] = []
        for name, bucket in buckets.items():
            if not isinstance(bucket, dict):
                continue
            rows.append(
                {
                    name_col: name,
                    "alerts": int(bucket.get("alerts", 0)),
                    "evaluated": int(bucket.get("evaluated", 0)),
                    "success": int(bucket.get("success", 0)),
                    "false_positive": int(bucket.get("false_positive", 0)),
                    "hit_rate_pct": float(bucket.get("hit_rate_pct", 0.0)),
                    "false_positive_rate_pct": float(bucket.get("false_positive_rate_pct", 0.0)),
                    "avg_abs_change_pct": float(bucket.get("avg_abs_change_pct", 0.0)),
                }
            )

        rows.sort(key=lambda r: (-int(r["alerts"]), str(r.get(name_col))))
        out_path = export_dir / f"{base_name}_{suffix}.csv"
        pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
        output_paths.append(out_path)

    summary_json = export_dir / f"{base_name}_summary.json"
    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "settings": {
            "days": settings.days,
            "report_days": settings.report_days,
            "windows": settings.windows,
            "max_symbols": settings.max_symbols,
            "max_concurrency": settings.max_concurrency,
            "fetch_limit": settings.fetch_limit,
            "top_n": settings.top_n,
            "min_samples_for_rate": settings.min_samples_for_rate,
        },
        "summary": summary,
    }
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    output_paths.append(summary_json)

    return output_paths


def _print_report(
    summary: Dict[str, object],
    settings: BacktestSettings,
    exported_files: Optional[List[Path]] = None,
) -> None:
    report_days = int(summary.get("report_days", settings.report_days))
    now_cn = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n========== BACKTEST REPORT ==========")
    print(f"生成时间: {now_cn}")
    print(f"回测范围: 过去 {settings.days} 天")
    print(f"统计窗口: 过去 {report_days} 天")
    print(f"回测窗口: {settings.windows}")
    print("")
    print(f"过去 {settings.days} 天警报总数: {summary['alerts_30d']}")
    print(
        f"过去 {settings.days} 天假阳性率: {summary['false_positive_rate_30d']:.2f}% "
        f"({summary['false_positive_30d']}/{summary['evaluated_30d']})"
    )
    print("")
    print(f"过去 {report_days} 天预计警报次数: {summary['alerts_7d']}")
    print(
        f"过去 {report_days} 天假阳性率: {summary['false_positive_rate_7d']:.2f}% "
        f"({summary['false_positive_7d']}/{summary['evaluated_7d']})"
    )

    by_rule = summary.get("by_rule_report", {})
    if isinstance(by_rule, dict):
        _print_bucket_breakdown(f"按规则分解（过去 {report_days} 天）", by_rule)

    by_window = summary.get("by_window_report", {})
    if isinstance(by_window, dict):
        _print_bucket_breakdown(f"按窗口分解（过去 {report_days} 天）", by_window)

    _print_top_rows(
        f"Top{settings.top_n} 币种（按警报次数）",
        summary.get("top_symbols_by_alerts", []) if isinstance(summary.get("top_symbols_by_alerts"), list) else [],
        "symbol",
    )
    _print_top_rows(
        f"Top{settings.top_n} 币种（按命中率，最少{settings.min_samples_for_rate}次评估）",
        summary.get("top_symbols_by_hit_rate", []) if isinstance(summary.get("top_symbols_by_hit_rate"), list) else [],
        "symbol",
    )
    _print_top_rows(
        f"Top{settings.top_n} 币种（按假阳性率，最少{settings.min_samples_for_rate}次评估）",
        summary.get("top_symbols_by_false_positive_rate", [])
        if isinstance(summary.get("top_symbols_by_false_positive_rate"), list)
        else [],
        "symbol",
    )
    _print_top_rows(
        f"Top{settings.top_n} 规则（按警报次数）",
        summary.get("top_rules_by_alerts", []) if isinstance(summary.get("top_rules_by_alerts"), list) else [],
        "rule_name",
    )

    if exported_files:
        print("\n导出文件:")
        for path in exported_files:
            print(f"- {path}")

    print("\n说明: 假阳性定义为信号后短窗口内未出现预设方向延续幅度。")
    print("====================================\n")


async def _async_main(args: argparse.Namespace) -> None:
    config = load_alert_config(config_path=args.config_path)
    _setup_logging(config.log_level)
    settings = _build_settings(config, args)

    events = await _run_backtest(config=config, settings=settings)
    summary = _summarize(
        events,
        report_days=settings.report_days,
        top_n=settings.top_n,
        min_samples_for_rate=settings.min_samples_for_rate,
    )
    exported_files = _export_reports(events, summary, settings=settings)
    _print_report(summary, settings=settings, exported_files=exported_files)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="crypto-monitor historical backtest")
    parser.add_argument("--config-path", default=os.getenv("ALERT_CONFIG_PATH", default_config_path()))
    parser.add_argument("--days", type=int, default=int(os.getenv("BACKTEST_DAYS", "30")))
    parser.add_argument("--report-days", type=int, default=int(os.getenv("BACKTEST_REPORT_DAYS", "7")))
    parser.add_argument("--max-symbols", type=int, default=int(os.getenv("BACKTEST_MAX_SYMBOLS", "24")))
    parser.add_argument("--windows", type=str, default=os.getenv("BACKTEST_WINDOWS", "1m,5m"))
    parser.add_argument("--max-concurrency", type=int, default=int(os.getenv("BACKTEST_MAX_CONCURRENCY", "4")))
    parser.add_argument("--fetch-limit", type=int, default=int(os.getenv("BACKTEST_FETCH_LIMIT", "1000")))
    parser.add_argument("--top-n", type=int, default=int(os.getenv("BACKTEST_TOP_N", "10")))
    parser.add_argument(
        "--min-samples-for-rate",
        type=int,
        default=int(os.getenv("BACKTEST_MIN_SAMPLES_FOR_RATE", "3")),
    )
    parser.add_argument("--export-dir", type=str, default=os.getenv("BACKTEST_EXPORT_DIR", default_reports_dir()))
    parser.add_argument("--export-prefix", type=str, default=os.getenv("BACKTEST_EXPORT_PREFIX", "backtest_report"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
