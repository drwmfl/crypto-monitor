from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

try:
    from alert_config import load_alert_config
    from backtest import _fetch_ohlcv_history, _prepare_indicator_frame
    from data_feed import BinanceKlineDataFeed
    from paths import default_config_path
    from rule_engine import MarketSnapshot, evaluate_snapshot
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_alert_config
    from apps.market_monitor.backend.backtest import _fetch_ohlcv_history, _prepare_indicator_frame
    from apps.market_monitor.backend.data_feed import BinanceKlineDataFeed
    from apps.market_monitor.backend.paths import default_config_path
    from apps.market_monitor.backend.rule_engine import MarketSnapshot, evaluate_snapshot

logger = logging.getLogger(__name__)

VALID_WINDOWS = {"1m", "5m", "15m", "30m", "1h"}


def _setup_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _parse_windows(raw: str) -> List[str]:
    windows = [x.strip() for x in str(raw or "").split(",") if x.strip()]
    windows = [w for w in windows if w in VALID_WINDOWS]
    if not windows:
        windows = ["1m", "5m", "15m", "30m", "1h"]
    return windows


def _safe_series_float(value: Any) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(v):
        return 0.0
    return v


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay symbol signals with rule reasons")
    parser.add_argument("--symbol", required=True, help="Symbol like GUSDT")
    parser.add_argument("--hours", type=int, default=24, help="Lookback hours")
    parser.add_argument("--windows", type=str, default="1m,5m,15m,30m,1h", help="Comma-separated windows")
    parser.add_argument("--limit-hits", type=int, default=30, help="Max lines to print")
    parser.add_argument("--with-cooldown", action="store_true", help="Apply cooldown logic in replay")
    parser.add_argument("--config-path", type=str, default=os.getenv("ALERT_CONFIG_PATH", default_config_path()))
    parser.add_argument("--export-json", type=str, default="", help="Optional export JSON file path")
    return parser.parse_args()


async def _replay_symbol(args: argparse.Namespace) -> int:
    config = load_alert_config(config_path=args.config_path)
    _setup_logging(config.log_level)

    symbol = str(args.symbol or "").upper().strip()
    if not symbol:
        print("symbol 不能为空")
        return 2

    windows = _parse_windows(args.windows)
    hours = max(1, int(args.hours))
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - hours * 60 * 60 * 1000

    feed = BinanceKlineDataFeed(config=config)
    init_attempts = 3
    for attempt in range(1, init_attempts + 1):
        await feed.initialize()
        if feed.symbol_mapping:
            break
        if attempt < init_attempts:
            logger.warning("Universe is empty on replay init (attempt=%s/%s), retrying...", attempt, init_attempts)
            await asyncio.sleep(3)

    ccxt_symbol = feed.symbol_mapping.get(symbol)
    if not ccxt_symbol:
        print(f"symbol 不在当前监控池中: {symbol}")
        print(f"当前池子数量: {len(feed.symbol_mapping)}")
        return 2

    all_hits: List[Dict[str, Any]] = []
    per_window_stats: Dict[str, Dict[str, int]] = {}
    high_extra_minutes = config.high_priority_cooldown_extra_minutes()

    for window in windows:
        candles = await _fetch_ohlcv_history(
            exchange=feed.exchange,
            ccxt_symbol=ccxt_symbol,
            window=window,
            since_ms=since_ms,
            until_ms=now_ms,
            limit=1000,
        )
        frame = _prepare_indicator_frame(candles, config)
        if not candles or frame is None or frame.empty:
            per_window_stats[window] = {"candles": len(candles), "hits": 0}
            continue

        min_start_idx = max(35, int(config.data_feed.get("volume_lookback", 20)) + 2)
        window_hits = 0
        cooldown_expiry_ms: Dict[str, int] = {}

        for idx in range(min_start_idx, len(candles)):
            row = candles[idx]
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

            events = evaluate_snapshot(snapshot, config)
            if not events:
                continue

            for event in events:
                if args.with_cooldown:
                    level = str(event.get("level", "medium")).strip().lower()
                    cd_minutes = config.cooldown_for(window)
                    if level == "high":
                        cd_minutes += high_extra_minutes

                    cd_key = f"{symbol}:{event['rule_name']}:{window}:{event['direction']}"
                    if event_time_ms < cooldown_expiry_ms.get(cd_key, 0):
                        continue
                    cooldown_expiry_ms[cd_key] = event_time_ms + int(cd_minutes * 60_000)

                volume_ratio = 0.0
                if snapshot.avg_volume > 0:
                    volume_ratio = snapshot.volume / snapshot.avg_volume

                all_hits.append(
                    {
                        "event_time_ms": event_time_ms,
                        "event_time_utc": datetime.fromtimestamp(event_time_ms / 1000.0, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S UTC"
                        ),
                        "symbol": symbol,
                        "window": window,
                        "rule_name": str(event.get("rule_name", "")),
                        "level": str(event.get("level", "medium")),
                        "direction": str(event.get("direction", "")),
                        "change_pct": float(event.get("change_pct", 0.0)),
                        "price": float(event.get("price", 0.0)),
                        "volume_ratio": volume_ratio,
                        "reasons": [str(x) for x in (event.get("reasons") or [])],
                    }
                )
                window_hits += 1

        per_window_stats[window] = {"candles": len(candles), "hits": window_hits}

    all_hits.sort(key=lambda x: int(x["event_time_ms"]), reverse=True)

    print("\n========== SYMBOL REPLAY REPORT ==========")
    print(f"symbol: {symbol} ({ccxt_symbol})")
    print(f"lookback_hours: {hours}")
    print(f"windows: {windows}")
    print(f"with_cooldown: {bool(args.with_cooldown)}")
    print("")
    print("每窗口统计:")
    for w in windows:
        stat = per_window_stats.get(w, {"candles": 0, "hits": 0})
        print(f"- {w}: candles={stat['candles']} hits={stat['hits']}")
    print("")
    print(f"总命中数: {len(all_hits)}")

    limit_hits = max(1, int(args.limit_hits))
    preview = all_hits[:limit_hits]
    if preview:
        print(f"\n最近 {len(preview)} 条命中:")
        for idx, hit in enumerate(preview, start=1):
            print(
                f"{idx}. {hit['event_time_utc']} | {hit['symbol']} | {hit['window']} | "
                f"{hit['rule_name']} | {hit['level']} | {hit['direction']} | "
                f"change={hit['change_pct']:+.3f}% | price={hit['price']:.6f} | vol_ratio={hit['volume_ratio']:.2f}x"
            )
            reasons = hit.get("reasons", [])
            for reason in reasons:
                print(f"   - {reason}")
    else:
        print("\n最近窗口没有命中。")

    if args.export_json:
        export_path = Path(str(args.export_json))
        export_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "ccxt_symbol": ccxt_symbol,
            "hours": hours,
            "windows": windows,
            "with_cooldown": bool(args.with_cooldown),
            "per_window_stats": per_window_stats,
            "hits": all_hits,
        }
        export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n已导出: {export_path}")

    print("=========================================\n")
    return 0


def main() -> None:
    args = parse_args()
    exit_code = asyncio.run(_replay_symbol(args))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
