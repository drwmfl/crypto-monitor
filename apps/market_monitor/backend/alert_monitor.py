from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

try:
    from alert_config import AlertConfig, load_alert_config
    from cooldown import AlertCooldownManager
    from data_feed import BinanceKlineDataFeed
    from notifier import AlertNotifier
    from realtime_ws import RealtimeKlineWatcher
    from rule_engine import evaluate_snapshot
    from strategy_pipeline import AlertStrategyPipeline
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import AlertConfig, load_alert_config
    from apps.market_monitor.backend.cooldown import AlertCooldownManager
    from apps.market_monitor.backend.data_feed import BinanceKlineDataFeed
    from apps.market_monitor.backend.notifier import AlertNotifier
    from apps.market_monitor.backend.realtime_ws import RealtimeKlineWatcher
    from apps.market_monitor.backend.rule_engine import evaluate_snapshot
    from apps.market_monitor.backend.strategy_pipeline import AlertStrategyPipeline

logger = logging.getLogger(__name__)
WINDOW_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}


def _parse_bool(value: str, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _setup_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _safe_float(value: object, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def _run_cycle(
    config: AlertConfig,
    feed: BinanceKlineDataFeed,
    notifier: AlertNotifier,
    cooldown_mgr: AlertCooldownManager,
    windows: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    strategy: Optional[AlertStrategyPipeline] = None,
    scan_source: str = "poll",
) -> Tuple[int, int, int, int]:
    if windows is not None and len(windows) == 0:
        return 0, 0, 0, 0

    snapshots = await feed.fetch_all_snapshots(windows=windows, symbols=symbols, scan_source=scan_source)
    event_count = 0
    sent_count = 0
    skipped_by_cooldown = 0
    high_priority_extra_minutes = config.high_priority_cooldown_extra_minutes()

    for snapshot in snapshots:
        events = evaluate_snapshot(snapshot, config)
        event_count += len(events)

        for event in events:
            event["trigger_source"] = "poll"
            event["scan_source"] = scan_source
            event["candle_state"] = "closed" if bool(config.data_feed.get("close_candle_only", True)) else "open"
            _attach_ws_gap_if_needed(config=config, feed=feed, event=event)
            event = await feed.enrich_event_trend(event=event, snapshot=snapshot)
            confidence_score = _safe_float(event.get("confidence"), default=None)
            confidence_band = config.confidence_band_for(confidence_score)
            event["confidence_band"] = confidence_band.get("label")

            if strategy is not None and strategy.enabled and not strategy.direct_tg_enabled:
                try:
                    result = await strategy.process_event(event, source=scan_source, notifier=notifier)
                except Exception as exc:
                    logger.exception(
                        "Strategy pipeline failed, event dropped: symbol=%s rule=%s window=%s err=%s",
                        event["symbol"],
                        event["rule_name"],
                        event["window"],
                        exc,
                    )
                    continue
                if result.alert_sent:
                    sent_count += 1
                continue

            if config.confidence_enabled():
                min_push_score = config.confidence_min_push_score()
                if confidence_score is None or confidence_score < min_push_score:
                    logger.info(
                        "Confidence skip: %s %s %s score=%s threshold=%.1f",
                        event["symbol"],
                        event["rule_name"],
                        event["window"],
                        confidence_score,
                        min_push_score,
                    )
                    continue
            cooldown_minutes = config.cooldown_for(event["window"])
            cooldown_minutes += int(confidence_band.get("cooldown_adjust_minutes", 0))
            cooldown_minutes = max(1, cooldown_minutes)
            allowed = await cooldown_mgr.allow_and_mark(
                symbol=event["symbol"],
                rule_name=event["rule_name"],
                window=event["window"],
                direction=event["direction"],
                cooldown_minutes=cooldown_minutes,
                level=str(event.get("level", "medium")),
                high_priority_extra_minutes=high_priority_extra_minutes,
            )
            if not allowed:
                skipped_by_cooldown += 1
                logger.info(
                    "Cooldown skip: %s %s %s %s",
                    event["symbol"],
                    event["rule_name"],
                    event["window"],
                    event["direction"],
                )
                continue

            try:
                ok = await notifier.notify_from_dict(event)
            except Exception as exc:
                logger.exception(
                    "Notifier raised unexpectedly, event dropped: symbol=%s rule=%s window=%s err=%s",
                    event["symbol"],
                    event["rule_name"],
                    event["window"],
                    exc,
                )
                ok = False
            if ok:
                sent_count += 1

    return len(snapshots), event_count, sent_count, skipped_by_cooldown


def _window_seconds(window: str) -> int:
    return WINDOW_SECONDS.get(window, 60)


def _compute_due_windows(
    windows: List[str],
    schedule_state: Dict[str, float],
    now: float,
    schedule_enabled: bool,
) -> List[str]:
    if not windows:
        return []
    if not schedule_enabled:
        return list(windows)

    if not schedule_state:
        ordered = sorted(windows, key=_window_seconds)
        first_window = ordered[0]
        for w in windows:
            schedule_state[w] = now + float(_window_seconds(w))
        schedule_state[first_window] = now

    due: List[str] = []
    for w in windows:
        due_at = schedule_state.get(w, now)
        if now >= due_at:
            due.append(w)
            schedule_state[w] = now + float(_window_seconds(w))
    return due


def _pick_symbol_batch(
    window: str,
    symbols: List[str],
    batch_size: int,
    cursor_state: Dict[str, int],
) -> List[str]:
    if not symbols:
        return []
    if batch_size <= 0 or batch_size >= len(symbols):
        return list(symbols)

    start = cursor_state.get(window, 0)
    end = start + batch_size
    if end <= len(symbols):
        batch = symbols[start:end]
        next_cursor = end
    else:
        overflow = end - len(symbols)
        batch = symbols[start:] + symbols[:overflow]
        next_cursor = overflow
    cursor_state[window] = next_cursor % len(symbols)
    return batch


def _pick_priority_symbols(config: AlertConfig, feed: BinanceKlineDataFeed) -> List[str]:
    data_feed_cfg = config.data_feed or {}
    if not bool(data_feed_cfg.get("priority_scan_enabled", False)):
        return []

    top_n = int(data_feed_cfg.get("priority_scan_top_n", 0) or 0)
    if top_n <= 0:
        return []

    min_change = _safe_float(data_feed_cfg.get("priority_scan_min_24h_change_pct"), default=0.0) or 0.0
    return feed.top_24h_gainer_symbols(limit=top_n, min_change_pct=min_change)


def _attach_ws_gap_if_needed(config: AlertConfig, feed: BinanceKlineDataFeed, event: Dict[str, object]) -> None:
    data_feed_cfg = config.data_feed or {}
    if not bool(data_feed_cfg.get("ws_gap_detection_enabled", False)):
        return
    if str(event.get("trigger_source") or "").strip().lower() == "ws":
        return

    window = str(event.get("window") or "").strip()
    min_change_map = data_feed_cfg.get("ws_gap_min_change_pct", {})
    if not isinstance(min_change_map, dict):
        min_change_map = {}
    min_change = _safe_float(min_change_map.get(window), default=0.0) or 0.0
    if min_change <= 0:
        return

    change_pct = abs(_safe_float(event.get("change_pct"), default=0.0) or 0.0)
    if change_pct < min_change:
        return

    ws_window = str(data_feed_cfg.get("ws_gap_window", "1m")).strip() or "1m"
    min_silence_sec = _safe_float(data_feed_cfg.get("ws_gap_min_silence_sec"), default=90.0) or 90.0
    gap = feed.ws_gap_status(str(event.get("symbol") or ""), ws_window, min_silence_sec)
    if not gap:
        return

    event["ws_gap_detected"] = True
    event["ws_gap_window"] = ws_window
    event["ws_gap_age_sec"] = gap.get("age_sec")
    event["ws_gap_reason"] = gap.get("reason")
    reasons = event.setdefault("reasons", [])
    if isinstance(reasons, list):
        age = gap.get("age_sec")
        if age is None:
            reasons.append(f"WS gap: no {ws_window} message seen before REST trigger")
        else:
            reasons.append(f"WS gap: {ws_window} silent {age}s before REST trigger")
    logger.warning(
        "WS gap detected: symbol=%s event_window=%s ws_window=%s change=%.2f age=%s reason=%s",
        event.get("symbol"),
        window,
        ws_window,
        change_pct,
        gap.get("age_sec"),
        gap.get("reason"),
    )


async def _initialize_feed_with_retry(feed: BinanceKlineDataFeed, retry_seconds: int = 15) -> None:
    while True:
        try:
            await feed.initialize()
            if feed.symbol_mapping:
                logger.info("Feed initialized with %s symbols.", len(feed.symbol_mapping))
                return
            logger.warning("Feed initialized but symbol mapping is empty, retry in %ss.", retry_seconds)
        except Exception as exc:
            logger.error("Feed initialize failed, retry in %ss: %s", retry_seconds, exc)
        await asyncio.sleep(retry_seconds)


async def run_alert_monitor(config_path: Optional[str] = None) -> None:
    config = load_alert_config(config_path=config_path)
    _setup_logging(config.log_level)

    logger.info(
        "Starting alert monitor: universe_mode=%s configured_symbols=%s windows=%s atr_enabled=%s refresh_sec=%s full_scan_windows=%s",
        config.data_feed.get("symbol_universe_mode", "configured"),
        config.symbols,
        config.windows,
        config.atr_enabled(),
        config.data_feed.get("universe_refresh_sec", 60),
        config.data_feed.get("full_scan_windows", []),
    )

    notifier = AlertNotifier.from_config(config.raw)
    await notifier.start()
    strategy = AlertStrategyPipeline.from_config(config.raw)
    logger.info(
        "Alert strategy pipeline: enabled=%s direct_tg_enabled=%s runtime_dir=%s",
        strategy.enabled,
        strategy.direct_tg_enabled,
        strategy.event_store.runtime_dir,
    )
    cooldown_mgr = AlertCooldownManager(
        redis_url=os.getenv("ALERT_REDIS_URL", "redis://redis:6379/0"),
        prefix=os.getenv("ALERT_COOLDOWN_PREFIX", "cooldown"),
        use_redis=_parse_bool(os.getenv("ALERT_USE_REDIS_COOLDOWN", "true"), default=True),
    )
    await cooldown_mgr.setup()

    feed = BinanceKlineDataFeed(config=config)
    await _initialize_feed_with_retry(
        feed=feed,
        retry_seconds=int(os.getenv("ALERT_INIT_RETRY_SECONDS", "15")),
    )
    schedule_state: Dict[str, float] = {}
    priority_schedule_state: Dict[str, float] = {}
    schedule_enabled = bool(config.data_feed.get("window_schedule_enabled", True))
    symbol_batch_size = int(config.data_feed.get("symbol_batch_size", 25))
    full_scan_windows = {
        str(w).strip() for w in (config.data_feed.get("full_scan_windows", []) or []) if str(w).strip()
    }
    raw_batch_map = config.data_feed.get("symbol_batch_size_by_window", {})
    if isinstance(raw_batch_map, dict):
        symbol_batch_size_by_window = {
            str(k): int(v) for k, v in raw_batch_map.items()
        }
    else:
        symbol_batch_size_by_window = {}
    window_cursor_state: Dict[str, int] = {}
    realtime_watcher = RealtimeKlineWatcher(
        config=config,
        feed=feed,
        notifier=notifier,
        cooldown_mgr=cooldown_mgr,
        strategy=strategy,
    )
    skip_windows = realtime_watcher.poll_windows_to_skip()
    poll_windows = [w for w in config.windows if w not in skip_windows]
    priority_scan_enabled = bool(config.data_feed.get("priority_scan_enabled", False))
    priority_scan_windows = [
        str(w).strip()
        for w in (config.data_feed.get("priority_scan_windows", []) or [])
        if str(w).strip() in config.windows
    ]
    realtime_task: Optional[asyncio.Task] = None

    if realtime_watcher.is_enabled():
        realtime_task = asyncio.create_task(realtime_watcher.run_forever(), name="realtime_kline_ws")
        logger.info("Realtime WS task started; poll-only windows=%s skipped_windows=%s", poll_windows, sorted(skip_windows))
    else:
        logger.info("Realtime WS disabled; poll windows=%s", poll_windows)
    logger.info(
        "Priority scan: enabled=%s top_n=%s windows=%s min_24h_change=%s coverage_file=%s",
        priority_scan_enabled,
        config.data_feed.get("priority_scan_top_n", 0),
        priority_scan_windows,
        config.data_feed.get("priority_scan_min_24h_change_pct", 0.0),
        config.data_feed.get("scan_coverage_file", ""),
    )

    try:
        while True:
            cycle_start = time.monotonic()
            now = time.time()
            await feed.refresh_universe_if_due(force=False)
            due_windows = _compute_due_windows(
                windows=poll_windows,
                schedule_state=schedule_state,
                now=now,
                schedule_enabled=schedule_enabled,
            )
            priority_due_windows = _compute_due_windows(
                windows=priority_scan_windows if priority_scan_enabled else [],
                schedule_state=priority_schedule_state,
                now=now,
                schedule_enabled=True,
            )
            if not due_windows and not priority_due_windows:
                sleep_for = max(1.0, float(config.poll_interval_sec))
                await asyncio.sleep(sleep_for)
                continue
            try:
                all_symbols = list(feed.symbol_mapping.keys())
                snapshot_count = 0
                event_count = 0
                sent_count = 0
                cooldown_skipped = 0
                per_window_batches: Dict[str, int] = {}
                scanned_by_window: Dict[str, set[str]] = {}

                for window in due_windows:
                    if window in full_scan_windows:
                        window_batch_size = 0
                    else:
                        window_batch_size = symbol_batch_size_by_window.get(window, symbol_batch_size)
                    batch_symbols = _pick_symbol_batch(
                        window=window,
                        symbols=all_symbols,
                        batch_size=window_batch_size,
                        cursor_state=window_cursor_state,
                    )
                    per_window_batches[window] = len(batch_symbols)
                    scanned_by_window[window] = set(batch_symbols)
                    sc, ec, ss, cs = await _run_cycle(
                        config=config,
                        feed=feed,
                        notifier=notifier,
                        cooldown_mgr=cooldown_mgr,
                        windows=[window],
                        symbols=batch_symbols,
                        strategy=strategy,
                        scan_source="poll",
                    )
                    snapshot_count += sc
                    event_count += ec
                    sent_count += ss
                    cooldown_skipped += cs

                priority_batches: Dict[str, int] = {}
                priority_symbols = _pick_priority_symbols(config=config, feed=feed) if priority_due_windows else []
                for window in priority_due_windows:
                    normal_symbols = scanned_by_window.get(window, set())
                    batch_symbols = [symbol for symbol in priority_symbols if symbol not in normal_symbols]
                    if not batch_symbols:
                        priority_batches[window] = 0
                        continue
                    priority_batches[window] = len(batch_symbols)
                    sc, ec, ss, cs = await _run_cycle(
                        config=config,
                        feed=feed,
                        notifier=notifier,
                        cooldown_mgr=cooldown_mgr,
                        windows=[window],
                        symbols=batch_symbols,
                        strategy=strategy,
                        scan_source="priority_poll",
                    )
                    snapshot_count += sc
                    event_count += ec
                    sent_count += ss
                    cooldown_skipped += cs

                elapsed = time.monotonic() - cycle_start
                logger.info(
                    "Cycle finished: windows=%s batches=%s priority_windows=%s priority_batches=%s snapshots=%s events=%s accepted=%s cooldown_skipped=%s notifier_queue=%s elapsed=%.2fs",
                    due_windows,
                    per_window_batches,
                    priority_due_windows,
                    priority_batches,
                    snapshot_count,
                    event_count,
                    sent_count,
                    cooldown_skipped,
                    notifier.stats(),
                    elapsed,
                )
            except Exception as exc:
                elapsed = time.monotonic() - cycle_start
                logger.error("Cycle failed (elapsed=%.2fs): %s", elapsed, exc)
                # 遇到连接级错误时尝试重新初始化行情源
                await _initialize_feed_with_retry(
                    feed=feed,
                    retry_seconds=int(os.getenv("ALERT_INIT_RETRY_SECONDS", "15")),
                )

            sleep_for = max(1.0, float(config.poll_interval_sec))
            await asyncio.sleep(sleep_for)
    finally:
        if realtime_task is not None:
            realtime_task.cancel()
            try:
                await realtime_task
            except asyncio.CancelledError:
                pass
        await notifier.stop()
        await cooldown_mgr.close()


async def main() -> None:
    await run_alert_monitor()


if __name__ == "__main__":
    asyncio.run(main())
