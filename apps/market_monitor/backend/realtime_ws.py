from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None

try:
    from alert_config import AlertConfig
    from binance_ws_routes import FUTURES_MARKET_STREAM_URL, normalize_market_stream_url
    from cooldown import AlertCooldownManager
    from data_feed import BinanceKlineDataFeed
    from indicators import calculate_latest_indicators
    from notifier import AlertNotifier
    from rule_engine import MarketSnapshot, evaluate_snapshot
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import AlertConfig
    from apps.market_monitor.backend.binance_ws_routes import (
        FUTURES_MARKET_STREAM_URL,
        normalize_market_stream_url,
    )
    from apps.market_monitor.backend.cooldown import AlertCooldownManager
    from apps.market_monitor.backend.data_feed import BinanceKlineDataFeed
    from apps.market_monitor.backend.indicators import calculate_latest_indicators
    from apps.market_monitor.backend.notifier import AlertNotifier
    from apps.market_monitor.backend.rule_engine import MarketSnapshot, evaluate_snapshot

logger = logging.getLogger(__name__)
WINDOW_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
}
AGGREGATABLE_WINDOWS = {"5m", "15m", "30m", "1h"}


class RealtimeDataStalled(RuntimeError):
    pass


class RealtimeSubscriptionError(RuntimeError):
    pass


class RealtimeKlineWatcher:
    def __init__(
        self,
        config: AlertConfig,
        feed: BinanceKlineDataFeed,
        notifier: AlertNotifier,
        cooldown_mgr: AlertCooldownManager,
        strategy: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.feed = feed
        self.notifier = notifier
        self.cooldown_mgr = cooldown_mgr
        self.strategy = strategy

        data_feed_cfg = config.data_feed or {}
        self.enabled = bool(data_feed_cfg.get("ws_realtime_enabled", False))
        self.windows = [str(w).strip() for w in (data_feed_cfg.get("ws_realtime_windows", []) or []) if str(w).strip()]
        self.skip_poll_windows = bool(data_feed_cfg.get("ws_realtime_skip_poll_windows", True))
        self.prefilter_pct_by_window = _normalize_prefilter(
            data_feed_cfg.get("ws_realtime_prefilter_pct", {}),
            self.windows,
            default=1.0,
        )
        self.recheck_interval_sec = float(data_feed_cfg.get("ws_realtime_recheck_interval_sec", 6.0))
        self.reconnect_sec = int(data_feed_cfg.get("ws_realtime_reconnect_sec", 3))
        self.no_message_reconnect_sec = float(data_feed_cfg.get("ws_realtime_no_message_reconnect_sec", 45.0))
        self.sub_chunk_size = int(data_feed_cfg.get("ws_realtime_subscription_chunk_size", 180))
        self.symbol_refresh_sec = int(data_feed_cfg.get("ws_realtime_symbol_refresh_sec", 60))
        self.ws_url = normalize_market_stream_url(
            str(data_feed_cfg.get("ws_realtime_url", FUTURES_MARKET_STREAM_URL))
        )

        self.local_agg_enabled = bool(data_feed_cfg.get("ws_local_agg_enabled", True))
        raw_local_agg_windows = data_feed_cfg.get("ws_local_agg_windows", [])
        if isinstance(raw_local_agg_windows, list):
            candidate_local_agg_windows = [str(w).strip() for w in raw_local_agg_windows if str(w).strip()]
        else:
            candidate_local_agg_windows = []
        if not candidate_local_agg_windows:
            candidate_local_agg_windows = [
                w for w in (config.windows or []) if w in AGGREGATABLE_WINDOWS
            ]
        self.local_agg_windows = [w for w in candidate_local_agg_windows if w in AGGREGATABLE_WINDOWS]
        self.local_agg_skip_poll_windows = bool(data_feed_cfg.get("ws_local_agg_skip_poll_windows", True))
        self.local_agg_history_limit = max(
            80,
            int(data_feed_cfg.get("ws_local_agg_history_limit", data_feed_cfg.get("kline_limit", 200))),
        )
        self.local_agg_bootstrap_concurrency = max(
            1,
            int(data_feed_cfg.get("ws_local_agg_bootstrap_concurrency", 2)),
        )

        http_proxy = os.getenv("ALERT_HTTP_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        https_proxy = os.getenv("ALERT_HTTPS_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        self.ws_proxy = https_proxy or http_proxy

        self._request_id = 1
        self._last_sub_refresh_ts = 0.0
        self._last_data_msg_ts = 0.0
        self._last_valid_kline_msg_ts = 0.0
        self._connection_message_count = 0
        self._connection_kline_count = 0
        self._requested_subscriptions: Set[str] = set()
        self._confirmed_subscriptions: Set[str] = set()
        self._pending_subscription_requests: Dict[int, Dict[str, Any]] = {}
        self._last_subscription_error = ""
        self._check_state: Dict[Tuple[str, str], Tuple[int, float]] = {}
        self.high_priority_extra_minutes = config.high_priority_cooldown_extra_minutes()
        self._closed_1m_last_start: Dict[str, int] = {}
        self._agg_state: Dict[Tuple[str, str], Dict[str, float]] = {}
        self._agg_history: Dict[Tuple[str, str], List[List[float]]] = {}
        self._agg_bootstrap_ready: Set[Tuple[str, str]] = set()
        self._agg_bootstrap_pending: Set[Tuple[str, str]] = set()
        self._agg_bootstrap_tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        self._agg_bootstrap_semaphore = asyncio.Semaphore(self.local_agg_bootstrap_concurrency)

    def is_enabled(self) -> bool:
        return self.enabled and bool(self.windows)

    def poll_windows_to_skip(self, require_healthy: bool = False) -> Set[str]:
        if not self.is_enabled():
            return set()
        if require_healthy and not self.is_data_healthy():
            return set()

        skipped: Set[str] = set()
        if self.skip_poll_windows:
            skipped.update(self.windows)
        if self.local_agg_enabled and self.local_agg_skip_poll_windows:
            skipped.update(
                window
                for window in self.local_agg_windows
                if self._is_local_agg_window_ready(window)
            )
        return skipped

    def _is_local_agg_window_ready(self, window: str) -> bool:
        symbols = set(self.feed.symbol_mapping.keys())
        if not symbols:
            return False
        return all((symbol, window) in self._agg_bootstrap_ready for symbol in symbols)

    def is_data_healthy(self) -> bool:
        if not self.is_enabled():
            return False
        if self._last_valid_kline_msg_ts <= 0:
            return False
        max_age = max(10.0, float(self.no_message_reconnect_sec))
        return (time.time() - self._last_valid_kline_msg_ts) <= max_age

    def data_health_snapshot(self) -> Dict[str, Any]:
        last_ts = float(self._last_valid_kline_msg_ts or 0.0)
        local_agg_target_count = len(self.feed.symbol_mapping)
        local_agg_ready = {
            window: sum(
                1
                for symbol in self.feed.symbol_mapping.keys()
                if (symbol, window) in self._agg_bootstrap_ready
            )
            for window in self.local_agg_windows
        }
        return {
            "enabled": self.is_enabled(),
            "healthy": self.is_data_healthy(),
            "last_kline_age_sec": round(time.time() - last_ts, 1) if last_ts > 0 else None,
            "last_kline_at": datetime.fromtimestamp(last_ts).isoformat() if last_ts > 0 else None,
            "stale_after_sec": max(10.0, float(self.no_message_reconnect_sec)),
            "received_messages": int(self._connection_message_count),
            "received_klines": int(self._connection_kline_count),
            "requested_subscriptions": len(self._requested_subscriptions),
            "confirmed_subscriptions": len(self._confirmed_subscriptions),
            "pending_subscription_batches": len(self._pending_subscription_requests),
            "last_subscription_error": self._last_subscription_error,
            "local_agg_target_count": local_agg_target_count,
            "local_agg_ready": local_agg_ready,
        }

    async def run_forever(self) -> None:
        if not self.is_enabled():
            logger.info("Realtime WS disabled, skip startup.")
            return

        if self.local_agg_enabled and "1m" not in self.windows:
            logger.warning("Local aggregation disabled because ws_realtime_windows does not include 1m.")
            self.local_agg_enabled = False

        if websockets is None and (self.ws_proxy is None or aiohttp is None):
            logger.error("Realtime WS cannot start: websockets/aiohttp dependency missing.")
            return

        logger.info(
            (
                "Realtime WS enabled: windows=%s prefilter=%s skip_poll_windows=%s "
                "local_agg_enabled=%s local_agg_windows=%s local_agg_skip_poll_windows=%s "
                "url=%s proxy=%s"
            ),
            self.windows,
            self.prefilter_pct_by_window,
            self.skip_poll_windows,
            self.local_agg_enabled,
            self.local_agg_windows,
            self.local_agg_skip_poll_windows,
            self.ws_url,
            bool(self.ws_proxy),
        )

        while True:
            try:
                await self._run_one_connection()
            except asyncio.CancelledError:
                raise
            except (RealtimeDataStalled, RealtimeSubscriptionError) as exc:
                logger.warning("%s; reconnecting soon.", exc)
            except Exception:
                logger.exception("Realtime WS loop failed, reconnecting soon.")

            await asyncio.sleep(max(1, self.reconnect_sec))

    async def _run_one_connection(self) -> None:
        await self.feed.refresh_universe_if_due(force=False)
        if self.ws_proxy and aiohttp is not None:
            await self._run_proxy_connection()
            return

        if websockets is None:
            raise RuntimeError("websockets dependency is required for direct WS mode")
        await self._run_direct_connection()

    async def _run_direct_connection(self) -> None:
        async with websockets.connect(
            self.ws_url,
            open_timeout=20,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        ) as ws:
            logger.info("Realtime WS connected (direct).")
            self._reset_connection_state()
            current_subs: Set[str] = set()
            current_subs = await self._sync_subscriptions(
                send_payload=lambda payload: ws.send(json.dumps(payload)),
                current_subs=current_subs,
                force=True,
            )
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    current_subs = await self._sync_subscriptions(
                        send_payload=lambda payload: ws.send(json.dumps(payload)),
                        current_subs=current_subs,
                        force=False,
                    )
                    self._raise_if_data_stalled(current_subs)
                    continue
                await self._handle_message(raw)
                current_subs = await self._sync_subscriptions(
                    send_payload=lambda payload: ws.send(json.dumps(payload)),
                    current_subs=current_subs,
                    force=False,
                )

    async def _run_proxy_connection(self) -> None:
        if aiohttp is None:
            raise RuntimeError("aiohttp dependency is required for proxy WS mode")

        timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(self.ws_url, proxy=self.ws_proxy, heartbeat=20) as ws:
                logger.info("Realtime WS connected via proxy: %s", self.ws_proxy)
                self._reset_connection_state()
                current_subs: Set[str] = set()
                current_subs = await self._sync_subscriptions(
                    send_payload=lambda payload: ws.send_str(json.dumps(payload)),
                    current_subs=current_subs,
                    force=True,
                )
                while True:
                    try:
                        msg = await ws.receive(timeout=1.0)
                    except asyncio.TimeoutError:
                        current_subs = await self._sync_subscriptions(
                            send_payload=lambda payload: ws.send_str(json.dumps(payload)),
                            current_subs=current_subs,
                            force=False,
                        )
                        self._raise_if_data_stalled(current_subs)
                        continue
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                        current_subs = await self._sync_subscriptions(
                            send_payload=lambda payload: ws.send_str(json.dumps(payload)),
                            current_subs=current_subs,
                            force=False,
                        )
                        continue
                    if msg.type == aiohttp.WSMsgType.CLOSED:
                        raise RuntimeError("aiohttp WS closed")
                    if msg.type == aiohttp.WSMsgType.CLOSING:
                        raise RuntimeError("aiohttp WS closing")
                    if msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError(f"aiohttp WS error: {ws.exception()}")
                    if msg.type == aiohttp.WSMsgType.CLOSE:
                        raise RuntimeError("aiohttp WS close frame received")
                    if msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong()
                        continue
                    if msg.type == aiohttp.WSMsgType.PONG:
                        continue

                    current_subs = await self._sync_subscriptions(
                        send_payload=lambda payload: ws.send_str(json.dumps(payload)),
                        current_subs=current_subs,
                        force=False,
                    )

    async def _sync_subscriptions(
        self,
        send_payload,
        current_subs: Set[str],
        force: bool,
    ) -> Set[str]:
        self._raise_if_subscription_ack_stalled()
        now = time.time()
        if not force and (now - self._last_sub_refresh_ts) < float(self.symbol_refresh_sec):
            return current_subs

        await self.feed.refresh_universe_if_due(force=False)

        target_subs: Set[str] = set()
        for symbol in self.feed.symbol_mapping.keys():
            symbol_lower = symbol.lower()
            for window in self.windows:
                target_subs.add(f"{symbol_lower}@kline_{window}")

        added = sorted(target_subs - current_subs)
        removed = sorted(current_subs - target_subs)

        for chunk in _chunked(added, self.sub_chunk_size):
            await send_payload(self._subscription_request("SUBSCRIBE", chunk))
        for chunk in _chunked(removed, self.sub_chunk_size):
            await send_payload(self._subscription_request("UNSUBSCRIBE", chunk))

        self._last_sub_refresh_ts = now
        self._requested_subscriptions = set(target_subs)
        if force or added or removed:
            logger.info(
                "Realtime WS subscription requests sent: total=%s add=%s remove=%s pending_batches=%s",
                len(target_subs),
                len(added),
                len(removed),
                len(self._pending_subscription_requests),
            )
        return target_subs

    def _reset_connection_state(self) -> None:
        self._last_sub_refresh_ts = 0.0
        self._last_data_msg_ts = time.time()
        self._last_valid_kline_msg_ts = 0.0
        self._connection_message_count = 0
        self._connection_kline_count = 0
        self._requested_subscriptions.clear()
        self._confirmed_subscriptions.clear()
        self._pending_subscription_requests.clear()
        self._last_subscription_error = ""

    def _subscription_request(self, action: str, streams: List[str]) -> Dict[str, Any]:
        request_id = self._next_id()
        normalized_action = str(action or "").strip().upper()
        normalized_streams = tuple(str(stream) for stream in streams if str(stream))
        self._pending_subscription_requests[request_id] = {
            "action": normalized_action,
            "streams": normalized_streams,
            "sent_at": time.time(),
        }
        return {
            "method": normalized_action,
            "params": list(normalized_streams),
            "id": request_id,
        }

    def _handle_subscription_control(self, payload: Dict[str, Any]) -> bool:
        is_error = payload.get("code") is not None
        if "result" not in payload and not is_error:
            return False

        request_id = payload.get("id")
        pending = self._pending_subscription_requests.pop(request_id, None)
        if is_error:
            error = (
                f"Realtime WS subscription rejected: id={request_id} "
                f"code={payload.get('code')} msg={payload.get('msg') or 'unknown'}"
            )
            self._last_subscription_error = error
            raise RealtimeSubscriptionError(error)

        if pending:
            streams = set(pending.get("streams") or ())
            action = str(pending.get("action") or "")
            if action == "SUBSCRIBE":
                self._confirmed_subscriptions.update(streams)
            elif action == "UNSUBSCRIBE":
                self._confirmed_subscriptions.difference_update(streams)
            logger.info(
                (
                    "Realtime WS subscription acknowledged: id=%s action=%s count=%s "
                    "confirmed=%s pending_batches=%s"
                ),
                request_id,
                action,
                len(streams),
                len(self._confirmed_subscriptions),
                len(self._pending_subscription_requests),
            )
        else:
            logger.debug("Realtime WS unmatched control response: id=%s", request_id)
        return True

    def _raise_if_subscription_ack_stalled(self) -> None:
        if not self._pending_subscription_requests:
            return
        oldest_sent_at = min(
            float(item.get("sent_at") or 0.0)
            for item in self._pending_subscription_requests.values()
        )
        timeout_sec = max(5.0, min(30.0, float(self.no_message_reconnect_sec) / 2.0))
        age = time.time() - oldest_sent_at
        if oldest_sent_at > 0 and age < timeout_sec:
            return
        error = (
            f"Realtime WS subscription ACK stalled for {age:.1f}s: "
            f"pending_batches={len(self._pending_subscription_requests)}"
        )
        self._last_subscription_error = error
        raise RealtimeSubscriptionError(error)

    async def _handle_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(payload, dict):
            return
        self._connection_message_count += 1

        if self._handle_subscription_control(payload):
            return

        if "stream" in payload and "data" in payload and isinstance(payload.get("data"), dict):
            payload = payload["data"]

        if payload.get("e") != "kline" and "k" not in payload:
            return

        kline = payload.get("k") or {}
        if not isinstance(kline, dict):
            return

        symbol = str(payload.get("s") or kline.get("s") or "").upper().strip()
        window = str(kline.get("i") or "").strip()
        if not symbol or window not in self.windows:
            return

        open_price = _safe_float(kline.get("o"))
        high_price = _safe_float(kline.get("h"))
        low_price = _safe_float(kline.get("l"))
        close_price = _safe_float(kline.get("c"))
        volume = _safe_float(kline.get("v"))
        if open_price <= 0 or high_price <= 0 or low_price <= 0 or close_price <= 0:
            return
        now = time.time()
        self._last_data_msg_ts = now
        self._last_valid_kline_msg_ts = now
        self._connection_kline_count += 1
        if self._connection_kline_count == 1:
            logger.info(
                "Realtime WS first valid kline received: symbol=%s window=%s confirmed_subscriptions=%s",
                symbol,
                window,
                len(self._confirmed_subscriptions),
            )

        candle_start_ms = _safe_int(kline.get("t"))
        candle_close_ms = _safe_int(kline.get("T"))
        is_closed = bool(kline.get("x"))
        if hasattr(self.feed, "mark_ws_kline"):
            self.feed.mark_ws_kline(
                symbol,
                window,
                candle_start_ms=candle_start_ms,
                is_closed=is_closed,
            )

        if self.local_agg_enabled and window == "1m" and is_closed:
            await self._on_closed_1m_candle(
                symbol=symbol,
                candle_start_ms=candle_start_ms,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume,
            )

        change_pct = abs((close_price - open_price) / open_price * 100.0)
        prefilter_pct = self.prefilter_pct_by_window.get(window, 0.0)
        if change_pct < prefilter_pct:
            return

        state_key = (symbol, window)
        prev_state = self._check_state.get(state_key)
        if prev_state and prev_state[0] == candle_start_ms:
            if (now - prev_state[1]) < float(self.recheck_interval_sec):
                return
        self._check_state[state_key] = (candle_start_ms, now)

        ccxt_symbol = self.feed.symbol_mapping.get(symbol)
        if not ccxt_symbol:
            return

        snapshot = await self.feed.fetch_snapshot(
            alert_symbol=symbol,
            ccxt_symbol=ccxt_symbol,
            window=window,
            use_open_candle=not is_closed,
            scan_source="ws_recheck",
        )
        if snapshot is None:
            event_ts_ms = candle_close_ms or candle_start_ms
            event_time = datetime.fromtimestamp(event_ts_ms / 1000.0) if event_ts_ms > 0 else datetime.now()
            snapshot = MarketSnapshot(
                symbol=symbol,
                window=window,
                open_price=open_price,
                close_price=close_price,
                volume=volume,
                avg_volume=0.0,
                event_time=event_time,
                indicators={},
            )

        events = evaluate_snapshot(snapshot, self.config)
        if not events:
            return
        for event in events:
            await self._process_realtime_event(
                event=event,
                snapshot=snapshot,
                trigger_source="ws",
                candle_state=("closed" if is_closed else "open"),
            )

    async def _on_closed_1m_candle(
        self,
        symbol: str,
        candle_start_ms: int,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: float,
    ) -> None:
        if candle_start_ms <= 0:
            return

        previous_start = self._closed_1m_last_start.get(symbol, 0)
        if candle_start_ms <= previous_start:
            return
        self._closed_1m_last_start[symbol] = candle_start_ms

        one_minute_row = [
            int(candle_start_ms),
            float(open_price),
            float(high_price),
            float(low_price),
            float(close_price),
            float(volume),
        ]
        for agg_window in self.local_agg_windows:
            closed_row = self._update_local_agg_bucket(symbol=symbol, window=agg_window, one_minute_row=one_minute_row)
            if closed_row is None:
                continue
            await self._evaluate_local_agg_closed(symbol=symbol, window=agg_window, closed_row=closed_row)

    def _update_local_agg_bucket(
        self,
        symbol: str,
        window: str,
        one_minute_row: List[float],
    ) -> Optional[List[float]]:
        bucket_ms = WINDOW_MS.get(window)
        if not bucket_ms:
            return None

        minute_start_ms = int(one_minute_row[0])
        bucket_start_ms = (minute_start_ms // int(bucket_ms)) * int(bucket_ms)
        key = (symbol, window)
        state = self._agg_state.get(key)

        if state is None:
            self._agg_state[key] = {
                "bucket_start_ms": float(bucket_start_ms),
                "open": float(one_minute_row[1]),
                "high": float(one_minute_row[2]),
                "low": float(one_minute_row[3]),
                "close": float(one_minute_row[4]),
                "volume": float(one_minute_row[5]),
                "count": 1.0,
            }
            return None

        current_bucket_start = int(state.get("bucket_start_ms", 0.0))
        if bucket_start_ms < current_bucket_start:
            return None

        if bucket_start_ms == current_bucket_start:
            state["high"] = max(float(state.get("high", one_minute_row[2])), float(one_minute_row[2]))
            state["low"] = min(float(state.get("low", one_minute_row[3])), float(one_minute_row[3]))
            state["close"] = float(one_minute_row[4])
            state["volume"] = float(state.get("volume", 0.0)) + float(one_minute_row[5])
            state["count"] = float(state.get("count", 0.0)) + 1.0
            return None

        closed_row = [
            int(current_bucket_start),
            float(state.get("open", one_minute_row[1])),
            float(state.get("high", one_minute_row[2])),
            float(state.get("low", one_minute_row[3])),
            float(state.get("close", one_minute_row[4])),
            float(state.get("volume", one_minute_row[5])),
        ]
        self._agg_state[key] = {
            "bucket_start_ms": float(bucket_start_ms),
            "open": float(one_minute_row[1]),
            "high": float(one_minute_row[2]),
            "low": float(one_minute_row[3]),
            "close": float(one_minute_row[4]),
            "volume": float(one_minute_row[5]),
            "count": 1.0,
        }
        return closed_row

    async def _evaluate_local_agg_closed(self, symbol: str, window: str, closed_row: List[float]) -> None:
        key = (symbol, window)
        self._append_history_row(key=key, row=closed_row)

        if key not in self._agg_bootstrap_ready:
            self._ensure_local_agg_bootstrap(symbol=symbol, window=window)
            return

        history = self._agg_history.get(key, [])
        min_required = max(35, int(self.feed.volume_lookback) + 3)
        if len(history) < min_required:
            return

        snapshot = self._build_snapshot_from_history(symbol=symbol, window=window, history=history)
        if snapshot is None:
            return

        events = evaluate_snapshot(snapshot, self.config)
        if not events:
            return
        for event in events:
            await self._process_realtime_event(
                event=event,
                snapshot=snapshot,
                trigger_source="ws",
                candle_state="closed",
            )

    def _append_history_row(self, key: Tuple[str, str], row: List[float]) -> None:
        history = self._agg_history.get(key)
        if history is None:
            history = []
            self._agg_history[key] = history

        row_ts = int(row[0])
        if history and int(history[-1][0]) == row_ts:
            history[-1] = row
        elif not history or row_ts > int(history[-1][0]):
            history.append(row)
        else:
            replaced = False
            for idx, item in enumerate(history):
                if int(item[0]) == row_ts:
                    history[idx] = row
                    replaced = True
                    break
            if not replaced:
                history.append(row)
                history.sort(key=lambda x: int(x[0]))

        overflow = len(history) - int(self.local_agg_history_limit)
        if overflow > 0:
            del history[:overflow]

    def _ensure_local_agg_bootstrap(self, symbol: str, window: str) -> None:
        key = (symbol, window)
        if key in self._agg_bootstrap_ready or key in self._agg_bootstrap_pending:
            return
        self._agg_bootstrap_pending.add(key)
        task = asyncio.create_task(self._bootstrap_local_agg_history(symbol=symbol, window=window))
        self._agg_bootstrap_tasks[key] = task

    async def _bootstrap_local_agg_history(self, symbol: str, window: str) -> None:
        key = (symbol, window)
        try:
            ccxt_symbol = self.feed.symbol_mapping.get(symbol)
            if not ccxt_symbol:
                return

            async with self._agg_bootstrap_semaphore:
                rows = await asyncio.to_thread(
                    self.feed.exchange.fetch_ohlcv,
                    ccxt_symbol,
                    window,
                    None,
                    int(self.local_agg_history_limit),
                )
            if not rows:
                return

            if self.feed.close_candle_only and len(rows) >= 2:
                rows = rows[:-1]

            normalized_rows: List[List[float]] = []
            for item in rows:
                if len(item) < 6:
                    continue
                ts = _safe_int(item[0])
                op = _safe_optional_float(item[1], default=None)
                hi = _safe_optional_float(item[2], default=None)
                lo = _safe_optional_float(item[3], default=None)
                cl = _safe_optional_float(item[4], default=None)
                vol = _safe_optional_float(item[5], default=None)
                if ts <= 0 or op is None or hi is None or lo is None or cl is None or vol is None:
                    continue
                normalized_rows.append([ts, op, hi, lo, cl, vol])
            if not normalized_rows:
                return

            history = self._agg_history.get(key, [])
            merged = normalized_rows + history
            merged_by_ts: Dict[int, List[float]] = {}
            for row in merged:
                merged_by_ts[int(row[0])] = row
            final_rows = [merged_by_ts[ts] for ts in sorted(merged_by_ts.keys())]
            if len(final_rows) > int(self.local_agg_history_limit):
                final_rows = final_rows[-int(self.local_agg_history_limit) :]
            self._agg_history[key] = final_rows
            self._agg_bootstrap_ready.add(key)
            logger.debug("Local agg bootstrap ready: %s %s rows=%s", symbol, window, len(final_rows))
        except Exception as exc:
            logger.debug("Local agg bootstrap failed: %s %s err=%s", symbol, window, exc)
        finally:
            self._agg_bootstrap_pending.discard(key)
            self._agg_bootstrap_tasks.pop(key, None)

    def _build_snapshot_from_history(
        self,
        symbol: str,
        window: str,
        history: List[List[float]],
    ) -> Optional[MarketSnapshot]:
        if not history:
            return None

        latest = history[-1]
        prior = history[:-1]
        recent_volume_rows = prior[-int(self.feed.volume_lookback) :] if prior else []
        avg_volume = 0.0
        if recent_volume_rows:
            avg_volume = sum(float(row[5]) for row in recent_volume_rows) / len(recent_volume_rows)

        long_rule = self.config.rule("long_anomaly")
        ma_period = int(long_rule.get("ma_period", self.feed.indicators_cfg.get("ma_period", 20)))
        std_period = int(long_rule.get("std_period", self.feed.indicators_cfg.get("std_period", ma_period)))
        indicators = calculate_latest_indicators(
            history,
            atr_period=int(self.config.atr.get("period", 14)),
            rsi_period=int(self.feed.indicators_cfg.get("rsi_period", 14)),
            bb_period=int(self.feed.indicators_cfg.get("bb_period", 20)),
            bb_std=float(self.feed.indicators_cfg.get("bb_std", 2.0)),
            macd_fast=int(self.feed.indicators_cfg.get("macd_fast", 12)),
            macd_slow=int(self.feed.indicators_cfg.get("macd_slow", 26)),
            macd_signal=int(self.feed.indicators_cfg.get("macd_signal", 9)),
            ma_period=ma_period,
            std_period=std_period,
            volume_lookback=int(self.feed.volume_lookback),
        )
        indicators.update(
            _rolling_startup_metrics(
                history,
                window=window,
                volume_lookback=int(self.feed.volume_lookback),
            )
        )

        event_time = datetime.fromtimestamp(int(latest[0]) / 1000.0)
        return MarketSnapshot(
            symbol=symbol,
            window=window,
            open_price=float(latest[1]),
            close_price=float(latest[4]),
            volume=float(latest[5]),
            avg_volume=avg_volume,
            event_time=event_time,
            indicators=indicators,
        )

    async def _process_realtime_event(
        self,
        event: Dict[str, Any],
        snapshot: MarketSnapshot,
        trigger_source: str,
        candle_state: str,
    ) -> None:
        event["trigger_source"] = trigger_source
        event["scan_source"] = trigger_source
        event["candle_state"] = candle_state
        event = await self.feed.enrich_event_trend(event=event, snapshot=snapshot)
        confidence_score = _safe_optional_float(event.get("confidence"), default=None)
        confidence_band = self.config.confidence_band_for(confidence_score)
        event["confidence_band"] = confidence_band.get("label")

        if self.strategy is not None and getattr(self.strategy, "enabled", False) and not getattr(
            self.strategy,
            "direct_tg_enabled",
            False,
        ):
            try:
                result = await self.strategy.process_event(event, source=trigger_source, notifier=self.notifier)
            except Exception as exc:
                logger.exception(
                    "Realtime strategy pipeline failed, event dropped: symbol=%s rule=%s window=%s err=%s",
                    event["symbol"],
                    event["rule_name"],
                    event["window"],
                    exc,
                )
                return
            logger.info(
                "Realtime strategy event: symbol=%s window=%s rule=%s score=%.1f risk=%.1f type=%s sent=%s reason=%s",
                event["symbol"],
                event["window"],
                event["rule_name"],
                result.candidate_score,
                result.risk_score,
                result.alert_type,
                result.alert_sent,
                result.reason,
            )
            return

        if self.config.confidence_enabled():
            min_push_score = self.config.confidence_min_push_score()
            if confidence_score is None or confidence_score < min_push_score:
                logger.info(
                    "Realtime confidence skip: %s %s %s score=%s threshold=%.1f",
                    event["symbol"],
                    event["rule_name"],
                    event["window"],
                    confidence_score,
                    min_push_score,
                )
                return

        cooldown_minutes = self.config.cooldown_for(event["window"])
        cooldown_minutes += int(confidence_band.get("cooldown_adjust_minutes", 0))
        cooldown_minutes = max(1, cooldown_minutes)
        allowed = await self.cooldown_mgr.allow_and_mark(
            symbol=event["symbol"],
            rule_name=event["rule_name"],
            window=event["window"],
            direction=event["direction"],
            cooldown_minutes=cooldown_minutes,
            level=str(event.get("level", "medium")),
            high_priority_extra_minutes=self.high_priority_extra_minutes,
        )
        if not allowed:
            return

        try:
            accepted = await self.notifier.notify_from_dict(event)
        except Exception as exc:
            logger.exception(
                "Realtime notifier raised unexpectedly, event dropped: symbol=%s rule=%s window=%s err=%s",
                event["symbol"],
                event["rule_name"],
                event["window"],
                exc,
            )
            accepted = False
        logger.info(
            "Realtime event: symbol=%s window=%s rule=%s level=%s change=%.2f conf=%s band=%s source=%s accepted=%s",
            event["symbol"],
            event["window"],
            event["rule_name"],
            event["level"],
            float(event["change_pct"]),
            confidence_score,
            event.get("confidence_band", "N/A"),
            trigger_source,
            accepted,
        )

    def _next_id(self) -> int:
        current = self._request_id
        self._request_id += 1
        return current

    def _raise_if_data_stalled(self, current_subs: Set[str]) -> None:
        if not current_subs or self.no_message_reconnect_sec <= 0:
            return
        last_ts = float(self._last_data_msg_ts or 0.0)
        if last_ts <= 0:
            self._last_data_msg_ts = time.time()
            return
        age = time.time() - last_ts
        if age < self.no_message_reconnect_sec:
            return
        raise RealtimeDataStalled(
            f"Realtime WS data stalled for {age:.1f}s with {len(current_subs)} subscriptions"
        )


def _normalize_prefilter(raw: Any, windows: List[str], default: float) -> Dict[str, float]:
    result: Dict[str, float] = {}
    if isinstance(raw, dict):
        for window in windows:
            result[window] = max(0.0, _safe_float(raw.get(window), default))
        return result

    scalar = max(0.0, _safe_float(raw, default))
    for window in windows:
        result[window] = scalar
    return result


def _chunked(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _rolling_startup_metrics(
    rows: List[List[float]],
    *,
    window: str,
    volume_lookback: int,
) -> Dict[str, float]:
    window_minutes = _window_minutes(window)
    if window_minutes <= 0 or not rows:
        return {}

    metrics: Dict[str, float] = {}
    for minutes in (15, 30):
        if minutes < window_minutes:
            continue
        candle_count = max(1, int(round(minutes / window_minutes)))
        if len(rows) < candle_count:
            continue

        recent = rows[-candle_count:]
        open_price = _safe_float(recent[0][1], 0.0)
        close_price = _safe_float(recent[-1][4], 0.0)
        if open_price <= 0 or close_price <= 0:
            continue

        volume_sum = sum(_safe_float(row[5], 0.0) for row in recent)
        previous = rows[: -candle_count]
        sample_count = max(1, int(volume_lookback or 1))
        baseline_rows = previous[-sample_count * candle_count :]
        baseline_sums: List[float] = []
        for start in range(0, len(baseline_rows), candle_count):
            chunk = baseline_rows[start : start + candle_count]
            if len(chunk) == candle_count:
                baseline_sums.append(sum(_safe_float(row[5], 0.0) for row in chunk))
        avg_volume_sum = sum(baseline_sums) / len(baseline_sums) if baseline_sums else 0.0

        metrics[f"rolling_{minutes}m_change_pct"] = (close_price - open_price) / open_price * 100.0
        metrics[f"rolling_{minutes}m_volume"] = volume_sum
        if avg_volume_sum > 0:
            metrics[f"rolling_{minutes}m_rvol"] = volume_sum / avg_volume_sum

    lookback_count = max(1, int(round(60 / window_minutes)))
    previous_rows = rows[-(lookback_count + 1) : -1]
    if previous_rows:
        prior_high = max(_safe_float(row[2], 0.0) for row in previous_rows)
        latest_close = _safe_float(rows[-1][4], 0.0)
        latest_high = _safe_float(rows[-1][2], 0.0)
        if prior_high > 0:
            metrics["breakout_60m_high"] = prior_high
            metrics["breakout_60m_close_distance_pct"] = (latest_close - prior_high) / prior_high * 100.0
            metrics["breakout_60m_high_distance_pct"] = (latest_high - prior_high) / prior_high * 100.0
    return metrics


def _window_minutes(window: str) -> int:
    text = str(window or "").strip().lower()
    if text.endswith("m"):
        return int(_safe_float(text[:-1], 0.0))
    if text.endswith("h"):
        return int(_safe_float(text[:-1], 0.0) * 60)
    return 0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_optional_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
