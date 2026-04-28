from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from alert_config import AlertConfig, load_alert_config
    from backtest import (
        DEFAULT_CONFIRM_PCT_BY_WINDOW,
        DEFAULT_HORIZON_BY_WINDOW,
        WINDOW_MS,
        _fetch_ohlcv_history,
        _prepare_indicator_frame,
    )
    from data_feed import BinanceKlineDataFeed
    from paths import default_config_path, default_reports_dir
    from rule_engine import MarketSnapshot, evaluate_snapshot
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import AlertConfig, load_alert_config
    from apps.market_monitor.backend.backtest import (
        DEFAULT_CONFIRM_PCT_BY_WINDOW,
        DEFAULT_HORIZON_BY_WINDOW,
        WINDOW_MS,
        _fetch_ohlcv_history,
        _prepare_indicator_frame,
    )
    from apps.market_monitor.backend.data_feed import BinanceKlineDataFeed
    from apps.market_monitor.backend.paths import default_config_path, default_reports_dir
    from apps.market_monitor.backend.rule_engine import MarketSnapshot, evaluate_snapshot

logger = logging.getLogger(__name__)


@dataclass
class PathEvent:
    symbol: str
    window: str
    rule_name: str
    direction: str
    level: str
    confidence: Optional[float]
    confidence_band: str
    event_time_ms: int
    entry_price: float
    change_pct: float
    max_favor_pct: float
    max_adverse_pct: float
    close_return_pct: float
    follow_through_success: bool


def _setup_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _parse_windows(raw: str, fallback: List[str]) -> List[str]:
    windows = [x.strip() for x in str(raw or "").split(",") if x.strip()]
    if not windows:
        windows = list(fallback)
    return [w for w in windows if w in WINDOW_MS]


def _safe_series_float(value: Any) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return v


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _direction_key(direction: str) -> str:
    text = str(direction or "").strip().lower()
    if text in {"up", "long", "bullish"}:
        return "up"
    if text in {"down", "short", "bearish"}:
        return "down"
    return "flat"


def _quantile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def _compute_path_metrics(
    entry: float,
    direction: str,
    future_rows: List[List[float]],
) -> Tuple[float, float, float]:
    if not future_rows or entry <= 0:
        return 0.0, 0.0, 0.0

    d = _direction_key(direction)
    highs = [float(r[2]) for r in future_rows]
    lows = [float(r[3]) for r in future_rows]
    close_last = float(future_rows[-1][4])

    if d == "up":
        max_favor = (max(highs) - entry) / entry * 100.0
        max_adverse = (entry - min(lows)) / entry * 100.0
        close_ret = (close_last - entry) / entry * 100.0
    elif d == "down":
        max_favor = (entry - min(lows)) / entry * 100.0
        max_adverse = (max(highs) - entry) / entry * 100.0
        close_ret = (entry - close_last) / entry * 100.0
    else:
        return 0.0, 0.0, 0.0

    return max(0.0, max_favor), max(0.0, max_adverse), close_ret


def _simulate_sl_tp_return(
    entry: float,
    direction: str,
    future_rows: List[List[float]],
    sl_pct: float,
    tp_pct: float,
) -> float:
    if not future_rows or entry <= 0:
        return 0.0
    d = _direction_key(direction)
    if d not in {"up", "down"}:
        return 0.0

    sl_pct = max(0.1, float(sl_pct))
    tp_pct = max(0.1, float(tp_pct))
    sl_px_up = entry * (1 - sl_pct / 100.0)
    tp_px_up = entry * (1 + tp_pct / 100.0)
    sl_px_down = entry * (1 + sl_pct / 100.0)
    tp_px_down = entry * (1 - tp_pct / 100.0)

    for row in future_rows:
        high = float(row[2])
        low = float(row[3])
        if d == "up":
            sl_hit = low <= sl_px_up
            tp_hit = high >= tp_px_up
        else:
            sl_hit = high >= sl_px_down
            tp_hit = low <= tp_px_down

        # Conservative tie-breaker: when both touched in one bar, assume SL first.
        if sl_hit and tp_hit:
            return -sl_pct
        if sl_hit:
            return -sl_pct
        if tp_hit:
            return tp_pct

    close_last = float(future_rows[-1][4])
    if d == "up":
        return (close_last - entry) / entry * 100.0
    return (entry - close_last) / entry * 100.0


def _window_grid(window: str) -> Tuple[List[float], List[float]]:
    if window == "1m":
        return [0.5, 0.7, 0.9, 1.1, 1.4, 1.8], [0.8, 1.0, 1.3, 1.6, 2.0, 2.6, 3.2]
    if window == "5m":
        return [0.9, 1.2, 1.5, 1.9, 2.4, 3.0], [1.4, 1.8, 2.3, 3.0, 3.8, 4.8, 6.0]
    if window == "15m":
        return [1.4, 1.8, 2.3, 2.9, 3.6, 4.5], [2.0, 2.6, 3.3, 4.2, 5.4, 6.8, 8.5]
    if window == "30m":
        return [1.8, 2.3, 2.9, 3.6, 4.6, 5.8], [2.8, 3.6, 4.6, 5.8, 7.2, 9.0, 11.0]
    return [2.5, 3.2, 4.0, 5.0, 6.2, 7.5], [3.6, 4.8, 6.0, 7.5, 9.2, 11.5, 14.0]


def _optimize_window_params(events: List[PathEvent], window: str) -> Dict[str, Any]:
    if not events:
        return {}

    sl_candidates, tp_candidates = _window_grid(window)
    best: Optional[Dict[str, Any]] = None

    for sl in sl_candidates:
        for tp in tp_candidates:
            if tp < sl * 1.2:
                continue

            returns: List[float] = []
            for e in events:
                horizon = max(1, int(DEFAULT_HORIZON_BY_WINDOW.get(e.window, 3)))
                returns.append(
                    _simulate_sl_tp_return(
                        entry=e.entry_price,
                        direction=e.direction,
                        future_rows=[],  # placeholder; replaced in event_payload below
                        sl_pct=sl,
                        tp_pct=tp,
                    )
                )

            # returns are recomputed with path rows in _recompute_candidate_metrics below
            # this block is replaced by result from that helper.

    return {}


def _avg(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _round_half(x: float) -> float:
    return round(x * 2.0) / 2.0


@dataclass
class EventPathPayload:
    event: PathEvent
    future_rows: List[List[float]]


def _evaluate_candidate(
    payloads: List[EventPathPayload],
    sl: float,
    tp: float,
) -> Dict[str, float]:
    if not payloads:
        return {
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "win_rate_pct": 0.0,
        }

    returns = [
        _simulate_sl_tp_return(
            entry=p.event.entry_price,
            direction=p.event.direction,
            future_rows=p.future_rows,
            sl_pct=sl,
            tp_pct=tp,
        )
        for p in payloads
    ]
    ordered = sorted(returns)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        median = (ordered[mid - 1] + ordered[mid]) / 2.0
    else:
        median = ordered[mid]
    win_rate = sum(1 for r in returns if r > 0) * 100.0 / len(returns)
    return {
        "avg_return_pct": _avg(returns),
        "median_return_pct": median,
        "win_rate_pct": win_rate,
    }


def _optimize_window_payloads(payloads: List[EventPathPayload], window: str) -> Dict[str, Any]:
    if not payloads:
        return {}

    sl_candidates, tp_candidates = _window_grid(window)
    best: Optional[Dict[str, Any]] = None

    for sl in sl_candidates:
        for tp in tp_candidates:
            if tp < sl * 1.2:
                continue
            metrics = _evaluate_candidate(payloads, sl=sl, tp=tp)
            avg_ret = float(metrics["avg_return_pct"])
            win_rate = float(metrics["win_rate_pct"])
            score = avg_ret + (win_rate - 50.0) * 0.015
            candidate = {
                "sl_pct": round(sl, 2),
                "tp2_pct": round(tp, 2),
                "tp1_pct": round(tp * 0.6, 2),
                "avg_return_pct": round(avg_ret, 4),
                "median_return_pct": round(float(metrics["median_return_pct"]), 4),
                "win_rate_pct": round(win_rate, 2),
                "score": round(score, 4),
            }
            if best is None or candidate["score"] > best["score"]:
                best = candidate

    if best is None:
        return {}
    return best


def _recommend_position_pct(sample_count: int, win_rate_pct: float, avg_return_pct: float) -> float:
    win_edge = max(0.0, win_rate_pct - 50.0)
    expectancy_edge = max(0.0, avg_return_pct)
    raw = 3.0 + win_edge * 0.18 + expectancy_edge * 2.0

    if sample_count >= 120:
        sample_factor = 1.0
    elif sample_count >= 60:
        sample_factor = 0.9
    elif sample_count >= 30:
        sample_factor = 0.8
    else:
        sample_factor = 0.65

    return min(18.0, max(3.0, _round_half(raw * sample_factor)))


def _recommended_leverage(window: str, win_rate_pct: float) -> float:
    if window == "1m":
        return 1.5 if win_rate_pct < 85 else 2.0
    if window == "5m":
        return 2.0 if win_rate_pct < 88 else 2.5
    return 1.5


def _calc_event_success(window: str, max_favor_pct: float) -> bool:
    confirm = float(DEFAULT_CONFIRM_PCT_BY_WINDOW.get(window, 0.6))
    return max_favor_pct >= confirm


async def _collect_payloads(
    config: AlertConfig,
    days: int,
    windows: List[str],
    max_symbols: int,
    max_concurrency: int,
    fetch_limit: int,
) -> List[EventPathPayload]:
    feed = BinanceKlineDataFeed(config=config)
    await feed.initialize()

    symbols = list(feed.symbol_mapping.keys())
    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    if not symbols:
        return []

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - days * 24 * 60 * 60 * 1000
    sem = asyncio.Semaphore(max(1, max_concurrency))
    high_extra = config.high_priority_cooldown_extra_minutes()
    conf_enabled = config.confidence_enabled()
    conf_min = config.confidence_min_push_score()

    async def run_symbol_window(symbol: str, window: str) -> List[EventPathPayload]:
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
                limit=fetch_limit,
            )

        if len(candles) < 40:
            return []
        frame = _prepare_indicator_frame(candles, config)
        if frame is None or frame.empty:
            return []

        payloads: List[EventPathPayload] = []
        cooldown_expiry_ms: Dict[str, int] = {}
        min_start_idx = max(35, int(config.data_feed.get("volume_lookback", 20)) + 2)
        horizon = max(1, int(DEFAULT_HORIZON_BY_WINDOW.get(window, 3)))

        for idx in range(min_start_idx, len(candles)):
            row = candles[idx]
            event_time_ms = int(row[0])
            snapshot = MarketSnapshot(
                symbol=symbol,
                window=window,
                open_price=float(row[1]),
                close_price=float(row[4]),
                volume=float(row[5]),
                avg_volume=float(frame["avg_volume"].iloc[idx]) if _safe_series_float(frame["avg_volume"].iloc[idx]) else 0.0,
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
                direction = _direction_key(str(event.get("direction", "")))
                if direction not in {"up", "down"}:
                    continue

                confidence = _safe_float(event.get("confidence"), default=None)
                band_cfg = config.confidence_band_for(confidence)
                band = str(band_cfg.get("label", "N/A"))
                if conf_enabled and (confidence is None or confidence < conf_min):
                    continue

                level = str(event.get("level", "medium")).strip().lower()
                cooldown_minutes = config.cooldown_for(window)
                cooldown_minutes += int(band_cfg.get("cooldown_adjust_minutes", 0))
                if level == "high":
                    cooldown_minutes += high_extra
                cooldown_minutes = max(1, int(cooldown_minutes))

                cd_key = f"{symbol}:{event.get('rule_name')}:{window}:{direction}"
                if event_time_ms < cooldown_expiry_ms.get(cd_key, 0):
                    continue
                cooldown_expiry_ms[cd_key] = event_time_ms + cooldown_minutes * 60_000

                end_idx = min(len(candles), idx + 1 + horizon)
                future_rows = candles[idx + 1 : end_idx]
                if not future_rows:
                    continue

                entry_price = float(event.get("price", row[4]))
                max_favor, max_adverse, close_ret = _compute_path_metrics(
                    entry=entry_price,
                    direction=direction,
                    future_rows=future_rows,
                )
                payloads.append(
                    EventPathPayload(
                        event=PathEvent(
                            symbol=symbol,
                            window=window,
                            rule_name=str(event.get("rule_name", "")),
                            direction=direction,
                            level=level,
                            confidence=confidence,
                            confidence_band=band,
                            event_time_ms=event_time_ms,
                            entry_price=entry_price,
                            change_pct=float(event.get("change_pct", 0.0)),
                            max_favor_pct=max_favor,
                            max_adverse_pct=max_adverse,
                            close_return_pct=close_ret,
                            follow_through_success=_calc_event_success(window, max_favor),
                        ),
                        future_rows=future_rows,
                    )
                )

        return payloads

    tasks = [run_symbol_window(s, w) for s in symbols for w in windows]
    all_payloads: List[EventPathPayload] = []
    for batch in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(batch, Exception):
            logger.warning("Calibration task failed: %s", batch)
            continue
        all_payloads.extend(batch)

    all_payloads.sort(key=lambda x: x.event.event_time_ms)
    return all_payloads


def _summarize_payloads(
    payloads: List[EventPathPayload],
    min_events_per_band: int,
) -> Dict[str, Any]:
    events = [p.event for p in payloads]
    by_window: Dict[str, List[EventPathPayload]] = {}
    by_band: Dict[str, List[EventPathPayload]] = {}

    for p in payloads:
        by_window.setdefault(p.event.window, []).append(p)
        by_band.setdefault(p.event.confidence_band, []).append(p)

    best_window_params: Dict[str, Dict[str, Any]] = {}
    for window, plist in by_window.items():
        best = _optimize_window_payloads(plist, window=window)
        if best:
            favors = [x.event.max_favor_pct for x in plist]
            adverse = [x.event.max_adverse_pct for x in plist]
            hit_rate = sum(1 for x in plist if x.event.follow_through_success) * 100.0 / len(plist)
            best_window_params[window] = {
                "events": len(plist),
                "hit_rate_pct": round(hit_rate, 2),
                "max_favor_q50_pct": round(_quantile(favors, 0.50), 3),
                "max_favor_q75_pct": round(_quantile(favors, 0.75), 3),
                "max_adverse_q50_pct": round(_quantile(adverse, 0.50), 3),
                "max_adverse_q75_pct": round(_quantile(adverse, 0.75), 3),
                "max_adverse_q85_pct": round(_quantile(adverse, 0.85), 3),
                **best,
            }

    band_rows: Dict[str, Dict[str, Any]] = {}
    for band, plist in by_band.items():
        if len(plist) < min_events_per_band:
            continue

        returns: List[float] = []
        for p in plist:
            param = best_window_params.get(p.event.window)
            if not param:
                continue
            returns.append(
                _simulate_sl_tp_return(
                    entry=p.event.entry_price,
                    direction=p.event.direction,
                    future_rows=p.future_rows,
                    sl_pct=float(param["sl_pct"]),
                    tp_pct=float(param["tp2_pct"]),
                )
            )
        if not returns:
            continue

        win_rate = sum(1 for r in returns if r > 0) * 100.0 / len(returns)
        avg_ret = _avg(returns)
        sample_count = len(returns)
        position_pct = _recommend_position_pct(sample_count, win_rate, avg_ret)

        # Pick dominant window to anchor leverage suggestion.
        dominant_window = max(
            (w for w in best_window_params.keys()),
            key=lambda w: sum(1 for p in plist if p.event.window == w),
            default="1m",
        )
        leverage = _recommended_leverage(dominant_window, win_rate)

        band_rows[band] = {
            "events": sample_count,
            "win_rate_pct": round(win_rate, 2),
            "avg_return_pct": round(avg_ret, 4),
            "position_pct": position_pct,
            "max_leverage": leverage,
        }

    summary = {
        "events_total": len(events),
        "windows": sorted(set(e.window for e in events)),
        "rules": sorted(set(e.rule_name for e in events)),
        "confidence_bands_seen": sorted(set(e.confidence_band for e in events)),
        "window_params": best_window_params,
        "band_positioning": band_rows,
    }
    return summary


def _export(
    payloads: List[EventPathPayload],
    summary: Dict[str, Any],
    export_dir: str,
    export_prefix: str,
) -> List[Path]:
    if not export_dir:
        return []

    out_dir = Path(export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{export_prefix}_{ts}"
    paths: List[Path] = []

    events_json = out_dir / f"{base}_events.json"
    events_payload = [asdict(p.event) for p in payloads]
    events_json.write_text(json.dumps(events_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    paths.append(events_json)

    summary_json = out_dir / f"{base}_summary.json"
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    paths.append(summary_json)

    return paths


def _print_summary(summary: Dict[str, Any], files: List[Path]) -> None:
    print("\n========== ACTION PARAM CALIBRATION ==========")
    print(f"events_total: {summary.get('events_total', 0)}")
    print(f"windows: {summary.get('windows', [])}")
    print(f"rules: {summary.get('rules', [])}")
    print("")

    wp = summary.get("window_params", {})
    if isinstance(wp, dict) and wp:
        print("Window SL/TP recommendations:")
        for window, cfg in sorted(wp.items()):
            print(
                f"- {window}: events={cfg.get('events')} hit_rate={cfg.get('hit_rate_pct')}% "
                f"SL={cfg.get('sl_pct')}% TP1={cfg.get('tp1_pct')}% TP2={cfg.get('tp2_pct')}% "
                f"avg_ret={cfg.get('avg_return_pct')}%"
            )
    print("")

    bp = summary.get("band_positioning", {})
    if isinstance(bp, dict) and bp:
        print("Confidence band positioning:")
        for band, cfg in sorted(bp.items()):
            print(
                f"- {band}: events={cfg.get('events')} win_rate={cfg.get('win_rate_pct')}% "
                f"avg_ret={cfg.get('avg_return_pct')}% pos={cfg.get('position_pct')}% "
                f"lev<={cfg.get('max_leverage')}x"
            )
    if files:
        print("\nExported files:")
        for f in files:
            print(f"- {f}")
    print("=============================================\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calibrate actionable position and SL/TP params from historical signals")
    parser.add_argument("--config-path", type=str, default=os.getenv("ALERT_CONFIG_PATH", default_config_path()))
    parser.add_argument("--days", type=int, default=int(os.getenv("CALIB_DAYS", "21")))
    parser.add_argument("--windows", type=str, default=os.getenv("CALIB_WINDOWS", "1m,5m"))
    parser.add_argument("--max-symbols", type=int, default=int(os.getenv("CALIB_MAX_SYMBOLS", "50")))
    parser.add_argument("--max-concurrency", type=int, default=int(os.getenv("CALIB_MAX_CONCURRENCY", "3")))
    parser.add_argument("--fetch-limit", type=int, default=int(os.getenv("CALIB_FETCH_LIMIT", "1000")))
    parser.add_argument("--min-events-per-band", type=int, default=int(os.getenv("CALIB_MIN_EVENTS_PER_BAND", "30")))
    parser.add_argument("--export-dir", type=str, default=os.getenv("CALIB_EXPORT_DIR", default_reports_dir()))
    parser.add_argument("--export-prefix", type=str, default=os.getenv("CALIB_EXPORT_PREFIX", "action_param_calib"))
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> None:
    config = load_alert_config(config_path=args.config_path)
    _setup_logging(config.log_level)
    windows = _parse_windows(args.windows, fallback=["1m", "5m"])

    payloads = await _collect_payloads(
        config=config,
        days=max(1, int(args.days)),
        windows=windows,
        max_symbols=max(1, int(args.max_symbols)),
        max_concurrency=max(1, int(args.max_concurrency)),
        fetch_limit=max(100, min(1500, int(args.fetch_limit))),
    )
    summary = _summarize_payloads(
        payloads=payloads,
        min_events_per_band=max(1, int(args.min_events_per_band)),
    )
    files = _export(
        payloads=payloads,
        summary=summary,
        export_dir=str(args.export_dir or "").strip(),
        export_prefix=str(args.export_prefix or "action_param_calib").strip(),
    )
    _print_summary(summary, files)


def main() -> None:
    args = parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
