from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ccxt
try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:
    from candidates.storage_paths import resolve_runtime_dir
    from alert_config import AlertConfig
    from indicators import calculate_latest_indicators
    from rule_engine import MarketSnapshot
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir
    from apps.market_monitor.backend.alert_config import AlertConfig
    from apps.market_monitor.backend.indicators import calculate_latest_indicators
    from apps.market_monitor.backend.rule_engine import MarketSnapshot

logger = logging.getLogger(__name__)


class BinanceKlineDataFeed:
    def __init__(self, config: AlertConfig) -> None:
        self.config = config
        self.kline_limit = int(config.data_feed.get("kline_limit", 200))
        self.volume_lookback = int(config.data_feed.get("volume_lookback", 20))
        self.close_candle_only = bool(config.data_feed.get("close_candle_only", True))
        self.max_concurrent_requests = int(config.data_feed.get("max_concurrent_requests", 8))
        self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self._refresh_lock = asyncio.Lock()
        self.indicators_cfg = dict(config.indicators or {})
        self.universe_mode = str(config.data_feed.get("symbol_universe_mode", "configured")).strip().lower()
        self.universe_refresh_sec = int(config.data_feed.get("universe_refresh_sec", 60))
        self.include_symbols = {_normalize_raw_symbol(s) for s in config.data_feed.get("include_symbols", [])}
        self.exclude_symbols = {_normalize_raw_symbol(s) for s in config.data_feed.get("exclude_symbols", [])}
        self.min_quote_volume_usdt = float(config.data_feed.get("min_quote_volume_usdt", 0.0))
        self.max_quote_volume_usdt = float(config.data_feed.get("max_quote_volume_usdt", 0.0))
        self.exclude_top_quote_volume_enabled = bool(config.data_feed.get("exclude_top_quote_volume_enabled", False))
        self.exclude_top_quote_volume_n = int(config.data_feed.get("exclude_top_quote_volume_n", 0))
        self.max_symbols = int(config.data_feed.get("max_symbols", 0))
        self.symbol_quote_volume: Dict[str, float] = {}
        self.symbol_change_24h: Dict[str, float] = {}
        self._trend_cache: Dict[str, Tuple[float, Optional[float]]] = {}
        self._trend_cache_ttl_sec = 30.0
        self.redis_url = os.getenv("ALERT_REDIS_URL", "redis://redis:6379/0")
        self._redis = None
        self._redis_lock = asyncio.Lock()
        self._supply_cache: Dict[str, Tuple[float, Optional[float], Optional[float]]] = {}
        self._supply_cache_ttl_sec = 300.0
        self._last_rate_limit_log_ts = 0.0
        self._rate_limit_log_interval_sec = 30.0
        self._banned_until_ms = 0
        self._last_universe_refresh_ts = 0.0
        self.scan_coverage_file = str(config.data_feed.get("scan_coverage_file", "scan_coverage.json")).strip()
        self.scan_coverage_flush_sec = float(config.data_feed.get("scan_coverage_flush_sec", 15.0))
        self._coverage_path = self._resolve_runtime_file(self.scan_coverage_file)
        self._last_coverage_flush_ts = 0.0
        self._scan_coverage: Dict[str, Any] = {
            "updated_at": "",
            "symbols": {},
            "ws": {},
        }
        exchange_args = {
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        }

        # ccxt may not reliably consume env proxy in all environments.
        # Pass proxies explicitly for deterministic behavior.
        http_proxy = os.getenv("ALERT_HTTP_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        https_proxy = os.getenv("ALERT_HTTPS_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        if http_proxy or https_proxy:
            exchange_args["proxies"] = {
                "http": http_proxy or https_proxy,
                "https": https_proxy or http_proxy,
            }

        self.exchange = ccxt.binance(exchange_args)
        self.symbol_mapping: Dict[str, str] = {}

    async def initialize(self) -> None:
        await self.refresh_universe_if_due(force=True)

        sample_symbols = list(self.symbol_mapping.keys())[:12]
        logger.info(
            "Data feed ready: mode=%s selected=%s sample=%s max_concurrent_requests=%s",
            self.universe_mode,
            len(self.symbol_mapping),
            sample_symbols,
            self.max_concurrent_requests,
        )

    async def refresh_universe_if_due(self, force: bool = False) -> bool:
        async with self._refresh_lock:
            now = time.time()
            if not force and self.universe_refresh_sec > 0:
                if (now - self._last_universe_refresh_ts) < float(self.universe_refresh_sec):
                    return False

            try:
                await asyncio.to_thread(self.exchange.load_markets, True)
                if self.universe_mode == "all_usdt_perp":
                    mapping, quote_volume_map, change_24h_map = await asyncio.to_thread(self._build_full_universe_mapping)
                else:
                    mapping, quote_volume_map, change_24h_map = self._build_symbol_mapping(self.config.symbols), {}, {}
            except Exception as exc:
                logger.warning("Universe refresh failed: %s", exc)
                return False

            old_symbols = set(self.symbol_mapping.keys())
            new_symbols = set(mapping.keys())
            added = sorted(new_symbols - old_symbols)
            removed = sorted(old_symbols - new_symbols)

            self.symbol_mapping = mapping
            self.symbol_quote_volume = quote_volume_map
            self.symbol_change_24h = change_24h_map
            self._last_universe_refresh_ts = now

            if force or added or removed:
                logger.info(
                    "Universe refreshed: total=%s added=%s removed=%s sample_added=%s sample_removed=%s",
                    len(new_symbols),
                    len(added),
                    len(removed),
                    added[:8],
                    removed[:8],
                )
            return True

    async def fetch_all_snapshots(
        self,
        windows: Optional[List[str]] = None,
        symbols: Optional[List[str]] = None,
        scan_source: str = "poll",
    ) -> List[MarketSnapshot]:
        target_windows = self.config.windows if windows is None else windows
        target_symbols = symbols if symbols is not None else list(self.symbol_mapping.keys())
        tasks = []
        for alert_symbol in target_symbols:
            ccxt_symbol = self.symbol_mapping.get(alert_symbol)
            if not ccxt_symbol:
                continue
            for window in target_windows:
                tasks.append(self.fetch_snapshot(alert_symbol, ccxt_symbol, window, scan_source=scan_source))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        snapshots: List[MarketSnapshot] = []
        for item in results:
            if isinstance(item, Exception):
                logger.warning("Snapshot task failed: %s", item)
                continue
            if item is not None:
                snapshots.append(item)
        return snapshots

    async def enrich_event_trend(
        self,
        event: Dict[str, object],
        snapshot: Optional[MarketSnapshot] = None,
    ) -> Dict[str, object]:
        symbol = str(event.get("symbol") or "").upper().strip()
        if not symbol:
            return event

        ccxt_symbol = self.symbol_mapping.get(symbol)
        change_1h: Optional[float] = None
        change_24h: Optional[float] = self.symbol_change_24h.get(symbol)

        now = time.time()
        if snapshot is not None and snapshot.window == "1h":
            change_1h = snapshot.change_pct
            self._trend_cache[symbol] = (now, change_1h)
        else:
            cached = self._trend_cache.get(symbol)
            if cached and (now - cached[0]) < self._trend_cache_ttl_sec:
                change_1h = cached[1]
            elif ccxt_symbol:
                change_1h = await asyncio.to_thread(self._fetch_1h_change_pct_sync, ccxt_symbol)
                self._trend_cache[symbol] = (now, change_1h)

        # Every alert forces a fresh 24h pull to align with Binance UI as much as possible.
        if ccxt_symbol:
            latest_24h = await asyncio.to_thread(self._fetch_24h_change_pct_sync, ccxt_symbol)
            if latest_24h is not None:
                change_24h = latest_24h
                self.symbol_change_24h[symbol] = latest_24h

        price = _safe_float(event.get("price"), 0.0)
        mc, fdv = await self._compute_mc_fdv(symbol=symbol, price=price)

        event["change_1h_pct"] = change_1h
        event["change_24h_pct"] = change_24h
        event["mc"] = mc
        event["fdv"] = fdv
        return event

    async def fetch_snapshot(
        self,
        alert_symbol: str,
        ccxt_symbol: str,
        window: str,
        use_open_candle: bool = False,
        scan_source: str = "poll",
    ) -> Optional[MarketSnapshot]:
        if self._is_temporarily_banned():
            self.mark_scan_result(alert_symbol, window, scan_source, success=False, error="temporarily_banned")
            return None

        self.mark_scan_attempt(alert_symbol, window, scan_source)
        async with self._semaphore:
            try:
                snapshot = await asyncio.to_thread(
                    self._fetch_snapshot_sync,
                    alert_symbol,
                    ccxt_symbol,
                    window,
                    use_open_candle,
                )
                self.mark_scan_result(
                    alert_symbol,
                    window,
                    scan_source,
                    success=snapshot is not None,
                    event_time=snapshot.event_time if snapshot else None,
                )
                return snapshot
            except Exception as exc:
                message = str(exc)
                self.mark_scan_result(alert_symbol, window, scan_source, success=False, error=message[:200])
                if "-1003" in message or "I'm a teapot" in message:
                    self._update_ban_until_from_error(message)
                    now = time.time()
                    if (now - self._last_rate_limit_log_ts) >= self._rate_limit_log_interval_sec:
                        logger.warning("Binance rate limit triggered, requests are temporarily blocked: %s", message)
                        self._last_rate_limit_log_ts = now
                    else:
                        logger.debug("fetch_ohlcv rate-limited: symbol=%s window=%s", alert_symbol, window)
                else:
                    logger.warning("fetch_ohlcv failed: symbol=%s window=%s err=%s", alert_symbol, window, exc)
                return None

    def top_24h_gainer_symbols(self, limit: int, min_change_pct: float = 0.0) -> List[str]:
        ranked: List[Tuple[float, str]] = []
        for symbol in self.symbol_mapping.keys():
            change = _safe_float(self.symbol_change_24h.get(symbol), 0.0)
            if change < float(min_change_pct):
                continue
            ranked.append((change, symbol))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [symbol for _, symbol in ranked[: max(0, int(limit))]]

    def mark_ws_kline(
        self,
        symbol: str,
        window: str,
        *,
        candle_start_ms: int = 0,
        is_closed: bool = False,
    ) -> None:
        normalized = _normalize_raw_symbol(symbol)
        window_key = str(window or "").strip()
        if not normalized or not window_key:
            return
        now = time.time()
        ws_state = self._scan_coverage.setdefault("ws", {}).setdefault(normalized, {})
        ws_state[window_key] = {
            "last_msg_ts": now,
            "last_msg_at": _utc_iso(now),
            "last_candle_start_ms": int(candle_start_ms or 0),
            "last_closed": bool(is_closed),
        }
        self._maybe_flush_coverage()

    def ws_gap_status(self, symbol: str, window: str, min_silence_sec: float) -> Optional[Dict[str, Any]]:
        normalized = _normalize_raw_symbol(symbol)
        window_key = str(window or "").strip()
        if not normalized or not window_key:
            return None

        now = time.time()
        ws_state = self._scan_coverage.get("ws", {}).get(normalized, {}).get(window_key)
        if not isinstance(ws_state, dict):
            return {
                "symbol": normalized,
                "window": window_key,
                "reason": "no_ws_message_seen",
                "age_sec": None,
                "last_msg_at": None,
            }

        last_ts = _safe_float(ws_state.get("last_msg_ts"), 0.0)
        age_sec = now - last_ts if last_ts > 0 else None
        if age_sec is None or age_sec < float(min_silence_sec):
            return None
        return {
            "symbol": normalized,
            "window": window_key,
            "reason": "ws_silence",
            "age_sec": round(age_sec, 1),
            "last_msg_at": ws_state.get("last_msg_at"),
        }

    def mark_scan_attempt(self, symbol: str, window: str, source: str) -> None:
        self._update_scan_coverage(symbol=symbol, window=window, source=source, success=None)

    def mark_scan_result(
        self,
        symbol: str,
        window: str,
        source: str,
        *,
        success: bool,
        event_time: Optional[datetime] = None,
        error: str = "",
    ) -> None:
        self._update_scan_coverage(
            symbol=symbol,
            window=window,
            source=source,
            success=bool(success),
            event_time=event_time,
            error=error,
        )

    def _update_scan_coverage(
        self,
        *,
        symbol: str,
        window: str,
        source: str,
        success: Optional[bool],
        event_time: Optional[datetime] = None,
        error: str = "",
    ) -> None:
        normalized = _normalize_raw_symbol(symbol)
        window_key = str(window or "").strip()
        if not normalized or not window_key:
            return

        now = time.time()
        symbol_state = self._scan_coverage.setdefault("symbols", {}).setdefault(normalized, {})
        by_window = symbol_state.setdefault("last_scan_by_window", {})
        item = by_window.setdefault(window_key, {})
        item["last_scan_ts"] = now
        item["last_scan_at"] = _utc_iso(now)
        item["source"] = str(source or "poll")
        if success is not None:
            item["success"] = bool(success)
            if event_time is not None:
                item["event_time"] = _datetime_iso(event_time)
            if error:
                item["error"] = str(error)
            else:
                item.pop("error", None)
        symbol_state["last_scan_ts"] = now
        symbol_state["last_scan_at"] = _utc_iso(now)
        self._maybe_flush_coverage()

    def _maybe_flush_coverage(self, force: bool = False) -> None:
        if not self._coverage_path:
            return
        now = time.time()
        if not force and self.scan_coverage_flush_sec > 0:
            if (now - self._last_coverage_flush_ts) < self.scan_coverage_flush_sec:
                return
        self._last_coverage_flush_ts = now
        self._scan_coverage["updated_at"] = _utc_iso(now)
        try:
            self._coverage_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._coverage_path.with_suffix(self._coverage_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(self._scan_coverage, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(self._coverage_path)
        except Exception:
            logger.debug("Failed to flush scan coverage: %s", self._coverage_path, exc_info=True)

    def _resolve_runtime_file(self, filename: str) -> Optional[Path]:
        clean = str(filename or "").strip()
        if not clean:
            return None
        path = Path(clean)
        if path.is_absolute():
            return path
        runtime_dir = resolve_runtime_dir(self.config.alert_strategy)
        return runtime_dir / clean

    def _is_temporarily_banned(self) -> bool:
        if self._banned_until_ms <= 0:
            return False
        return int(time.time() * 1000) < self._banned_until_ms

    def _update_ban_until_from_error(self, message: str) -> None:
        match = re.search(r"banned until (\d+)", message)
        if not match:
            return
        try:
            self._banned_until_ms = max(self._banned_until_ms, int(match.group(1)))
        except ValueError:
            return

    def _fetch_snapshot_sync(
        self,
        alert_symbol: str,
        ccxt_symbol: str,
        window: str,
        use_open_candle: bool = False,
    ) -> Optional[MarketSnapshot]:
        candles = self.exchange.fetch_ohlcv(ccxt_symbol, timeframe=window, limit=self.kline_limit)
        if not candles or len(candles) < max(35, self.volume_lookback + 3):
            return None

        if use_open_candle:
            closed = candles
        else:
            closed = candles[:-1] if self.close_candle_only and len(candles) >= 2 else candles
        if len(closed) < self.volume_lookback + 2:
            return None

        latest = closed[-1]
        prior = closed[:-1]
        recent_volume_rows = prior[-self.volume_lookback :] if prior else []
        avg_volume = 0.0
        if recent_volume_rows:
            avg_volume = sum(float(row[5]) for row in recent_volume_rows) / len(recent_volume_rows)

        long_rule = self.config.rule("long_anomaly")
        ma_period = int(long_rule.get("ma_period", self.indicators_cfg.get("ma_period", 20)))
        std_period = int(long_rule.get("std_period", self.indicators_cfg.get("std_period", ma_period)))
        indicators = calculate_latest_indicators(
            closed,
            atr_period=int(self.config.atr.get("period", 14)),
            rsi_period=int(self.indicators_cfg.get("rsi_period", 14)),
            bb_period=int(self.indicators_cfg.get("bb_period", 20)),
            bb_std=float(self.indicators_cfg.get("bb_std", 2.0)),
            macd_fast=int(self.indicators_cfg.get("macd_fast", 12)),
            macd_slow=int(self.indicators_cfg.get("macd_slow", 26)),
            macd_signal=int(self.indicators_cfg.get("macd_signal", 9)),
            ma_period=ma_period,
            std_period=std_period,
            volume_lookback=self.volume_lookback,
        )

        event_time = datetime.fromtimestamp(int(latest[0]) / 1000.0)
        return MarketSnapshot(
            symbol=alert_symbol,
            window=window,
            open_price=float(latest[1]),
            close_price=float(latest[4]),
            volume=float(latest[5]),
            avg_volume=avg_volume,
            event_time=event_time,
            indicators=indicators,
        )

    def _build_symbol_mapping(self, symbols: List[str]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        markets = self.exchange.markets or {}

        for raw_symbol in symbols:
            matched = None
            for candidate in _symbol_candidates(raw_symbol):
                if candidate not in markets:
                    continue
                market = markets[candidate]
                if market.get("quote") == "USDT":
                    matched = candidate
                    break

            if matched:
                mapping[raw_symbol] = matched
            else:
                logger.warning("Symbol not found in exchange markets, skipped: %s", raw_symbol)

        return mapping

    def _build_full_universe_mapping(self) -> Tuple[Dict[str, str], Dict[str, float], Dict[str, float]]:
        markets = self.exchange.markets or {}
        try:
            tickers = self.exchange.fetch_tickers()
        except Exception as exc:
            logger.warning("fetch_tickers failed in full-universe mode, fallback to no-volume-filter: %s", exc)
            tickers = {}

        top_quote_volume_excluded: set[str] = set()
        if self.exclude_top_quote_volume_enabled and self.exclude_top_quote_volume_n > 0 and not self.include_symbols:
            ranked: List[Tuple[float, str]] = []
            for ccxt_symbol, market in markets.items():
                if not isinstance(market, dict):
                    continue
                if market.get("quote") != "USDT":
                    continue
                if not market.get("active", True):
                    continue
                if not (market.get("swap") or market.get("contract")):
                    continue

                raw_symbol = _normalize_raw_symbol(str(market.get("id") or ""))
                if not raw_symbol.endswith("USDT"):
                    continue

                quote_volume = _safe_float((tickers.get(ccxt_symbol) or {}).get("quoteVolume"), 0.0)
                if quote_volume <= 0:
                    continue
                ranked.append((quote_volume, raw_symbol))

            ranked.sort(key=lambda item: (-item[0], item[1]))
            for _, raw_symbol in ranked:
                top_quote_volume_excluded.add(raw_symbol)
                if len(top_quote_volume_excluded) >= self.exclude_top_quote_volume_n:
                    break

        candidates: List[Tuple[str, str, float, float]] = []
        for ccxt_symbol, market in markets.items():
            if not isinstance(market, dict):
                continue
            if market.get("quote") != "USDT":
                continue
            if not market.get("active", True):
                continue
            if not (market.get("swap") or market.get("contract")):
                continue

            raw_symbol = _normalize_raw_symbol(str(market.get("id") or ""))
            if not raw_symbol.endswith("USDT"):
                continue
            if self.include_symbols and raw_symbol not in self.include_symbols:
                continue
            if raw_symbol in self.exclude_symbols:
                continue
            if raw_symbol in top_quote_volume_excluded:
                continue

            quote_volume = _safe_float((tickers.get(ccxt_symbol) or {}).get("quoteVolume"), 0.0)
            if quote_volume < self.min_quote_volume_usdt:
                continue
            if self.max_quote_volume_usdt > 0 and quote_volume > self.max_quote_volume_usdt:
                continue
            change_24h = _safe_float((tickers.get(ccxt_symbol) or {}).get("percentage"), 0.0)
            candidates.append((raw_symbol, ccxt_symbol, quote_volume, change_24h))

        candidates.sort(key=lambda item: (-item[2], item[0]))
        if self.max_symbols > 0:
            candidates = candidates[: self.max_symbols]

        mapping: Dict[str, str] = {}
        quote_volume_map: Dict[str, float] = {}
        change_24h_map: Dict[str, float] = {}
        for raw_symbol, ccxt_symbol, quote_volume, change_24h in candidates:
            mapping[raw_symbol] = ccxt_symbol
            quote_volume_map[raw_symbol] = quote_volume
            change_24h_map[raw_symbol] = change_24h

        if not mapping:
            logger.warning(
                "Full-universe mapping empty after filters (min_qv=%s max_qv=%s max_symbols=%s include=%s exclude=%s).",
                self.min_quote_volume_usdt,
                self.max_quote_volume_usdt,
                self.max_symbols,
                len(self.include_symbols),
                len(self.exclude_symbols),
            )
        else:
            logger.info(
                "Full-universe filters: exclude_top_enabled=%s excluded_top_quote_volume_n=%s final_symbols=%s",
                self.exclude_top_quote_volume_enabled,
                len(top_quote_volume_excluded),
                len(mapping),
            )
        return mapping, quote_volume_map, change_24h_map

    def _fetch_1h_change_pct_sync(self, ccxt_symbol: str) -> Optional[float]:
        try:
            candles = self.exchange.fetch_ohlcv(ccxt_symbol, timeframe="1h", limit=2)
            if not candles:
                return None
            latest = candles[-1]
            open_price = _safe_float(latest[1], 0.0)
            close_price = _safe_float(latest[4], 0.0)
            if open_price <= 0:
                return None
            return (close_price - open_price) / open_price * 100.0
        except Exception:
            return None

    def _fetch_24h_change_pct_sync(self, ccxt_symbol: str) -> Optional[float]:
        try:
            ticker = self.exchange.fetch_ticker(ccxt_symbol) or {}
            if "percentage" not in ticker:
                return None
            return _safe_float(ticker.get("percentage"), 0.0)
        except Exception:
            return None

    async def _compute_mc_fdv(self, symbol: str, price: float) -> Tuple[Optional[float], Optional[float]]:
        if price <= 0:
            return None, None

        supply, max_supply = await self._get_supply_values(symbol=symbol)
        mc = (price * supply) if (supply is not None and supply > 0) else None
        fdv = (price * max_supply) if (max_supply is not None and max_supply > 0) else None
        return mc, fdv

    async def _get_supply_values(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        normalized_symbol = _normalize_raw_symbol(symbol)
        now = time.time()
        cached = self._supply_cache.get(normalized_symbol)
        if cached and (now - cached[0]) < self._supply_cache_ttl_sec:
            return cached[1], cached[2]

        supply, max_supply = await self._fetch_supply_from_redis(normalized_symbol)
        self._supply_cache[normalized_symbol] = (now, supply, max_supply)
        return supply, max_supply

    async def _fetch_supply_from_redis(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        client = await self._ensure_redis_client()
        if client is None:
            return None, None

        try:
            values = await client.mget([f"supply:{symbol}", f"max_supply:{symbol}"])
        except Exception:
            logger.debug("Redis mget supply failed for symbol=%s", symbol, exc_info=True)
            return None, None

        if not isinstance(values, list) or len(values) != 2:
            return None, None

        supply_val = _safe_float(values[0], 0.0)
        max_supply_val = _safe_float(values[1], 0.0)
        supply = supply_val if supply_val > 0 else None
        max_supply = max_supply_val if max_supply_val > 0 else None
        return supply, max_supply

    async def _ensure_redis_client(self):
        if self._redis is not None:
            return self._redis
        if redis is None:
            return None

        async with self._redis_lock:
            if self._redis is not None:
                return self._redis
            try:
                client = redis.from_url(self.redis_url, decode_responses=True)
                await client.ping()
                self._redis = client
            except Exception:
                logger.debug("Redis connect failed for data_feed supply lookup: %s", self.redis_url, exc_info=True)
                self._redis = None
        return self._redis


def _symbol_candidates(raw_symbol: str) -> List[str]:
    s = raw_symbol.strip().upper()
    if not s:
        return []

    if "/" in s:
        candidates = [s]
        if ":" not in s and s.endswith("/USDT"):
            candidates.append(f"{s}:USDT")
        return list(dict.fromkeys(candidates))

    if s.endswith("USDT"):
        base = s[:-4]
        return [f"{base}/USDT:USDT", f"{base}/USDT", s]

    return [s]


def _normalize_raw_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("/", "")


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()


def _datetime_iso(value: datetime) -> str:
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()
