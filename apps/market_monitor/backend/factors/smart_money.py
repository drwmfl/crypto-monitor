from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None

try:
    from candidates.storage_paths import resolve_runtime_dir
    from factors.factor_models import FactorSnapshot, recent_health_entry
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir
    from apps.market_monitor.backend.factors.factor_models import FactorSnapshot, recent_health_entry


logger = logging.getLogger(__name__)


class SmartMoneyProvider:
    """Collect Binance Smart Money BAPI data as an optional shadow factor."""

    OVERVIEW_PATH = "/bapi/futures/v1/public/future/smart-money/signal/overview"
    STATS_PATH = "/bapi/futures/v1/public/future/smart-money/signal/details/stats"

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.base_url = str(self.settings.get("base_url") or "https://www.binance.com").rstrip("/")
        self.timeout_sec = max(0.5, _safe_float(self.settings.get("timeout_sec"), 8.0))
        self.poll_interval_sec = max(30, _safe_int(self.settings.get("poll_interval_sec"), 300))
        self.stale_after_sec = max(
            self.poll_interval_sec,
            _safe_int(self.settings.get("stale_after_sec"), 900),
        )
        self.track_ttl_sec = max(300, _safe_int(self.settings.get("track_ttl_sec"), 1800))
        self.max_active_symbols = max(1, _safe_int(self.settings.get("max_active_symbols"), 12))
        self.concurrency = max(1, min(8, _safe_int(self.settings.get("concurrency"), 1)))
        self.request_spacing_sec = max(
            0.0,
            _safe_float(self.settings.get("request_spacing_sec"), 1.0),
        )
        self.rate_limit_cooldown_sec = max(
            60.0,
            _safe_float(self.settings.get("rate_limit_cooldown_sec"), 3600.0),
        )
        raw_ranges = self.settings.get("stats_time_ranges", ["30m", "1h"])
        if not isinstance(raw_ranges, list):
            raw_ranges = ["30m", "1h"]
        self.stats_time_ranges = [
            value
            for value in (str(item or "").strip().lower() for item in raw_ranges)
            if value in {"30m", "1h", "24h", "7d", "all"}
        ] or ["30m", "1h"]
        self.max_history_samples = max(60, _safe_int(self.settings.get("max_history_samples"), 360))
        self.min_sample_interval_sec = max(
            15.0,
            _safe_float(self.settings.get("min_sample_interval_sec"), 45.0),
        )
        self.save_interval_sec = max(1.0, _safe_float(self.settings.get("save_interval_sec"), 10.0))
        runtime_dir = resolve_runtime_dir(self.settings)
        self.state_path = runtime_dir / str(
            self.settings.get("state_file") or "smart_money_state.json"
        )
        self.history_path = runtime_dir / str(
            self.settings.get("history_file") or "smart_money_history.jsonl"
        )
        self._tracked_symbols: Dict[str, float] = {}
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, Deque[Dict[str, Any]]] = {}
        self._refreshing: set[str] = set()
        self._last_save_ts = 0.0
        self._task: Optional[asyncio.Task] = None
        self._refresh_tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._last_request_monotonic = 0.0
        self._blocked_until = 0.0
        self._proxy = (
            os.getenv("ALERT_HTTPS_PROXY")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("https_proxy")
            or os.getenv("ALERT_HTTP_PROXY")
            or os.getenv("HTTP_PROXY")
            or os.getenv("http_proxy")
        )
        self._load_state()

    async def fetch(
        self,
        symbol: str,
        base_asset: str,
        *,
        price: Optional[float] = None,
    ) -> FactorSnapshot:
        snapshot = FactorSnapshot.empty(symbol=symbol, base_asset=base_asset)
        if not self.enabled:
            snapshot.source_health["smart_money"] = recent_health_entry("smart_money", True, "disabled")
            return snapshot
        if aiohttp is None:
            snapshot.source_health["smart_money"] = recent_health_entry(
                "smart_money",
                False,
                "aiohttp_missing",
            )
            return snapshot

        symbol_key = str(symbol or "").upper().strip()
        await self._touch(symbol_key)
        self._ensure_started()
        cached, history = await self._cached_state(symbol_key)
        if cached is None:
            self._schedule_refresh(symbol_key)
            message = "rate_limited" if time.time() < self._blocked_until else "warming_up"
            snapshot.source_health["smart_money"] = recent_health_entry(
                "smart_money",
                message != "rate_limited",
                message,
            )
            return snapshot

        observed_at_ms = _safe_int(cached.get("observed_at_ms"), 0)
        age_sec = max(0.0, (int(time.time() * 1000) - observed_at_ms) / 1000.0) if observed_at_ms else 1e9
        snapshot.smart_money.update(
            build_smart_money_metrics(
                cached,
                price=_safe_float(price, 0.0),
                history=history,
            )
        )
        snapshot.smart_money["data_age_sec"] = round(age_sec, 2)
        snapshot.smart_money["is_fresh"] = age_sec <= self.stale_after_sec
        message = "available" if age_sec <= self.stale_after_sec else "stale"
        snapshot.source_health["smart_money"] = recent_health_entry(
            "smart_money",
            age_sec <= self.stale_after_sec,
            message,
        )
        if age_sec >= self.poll_interval_sec:
            self._schedule_refresh(symbol_key)
        return snapshot

    async def prewarm(self, symbol: str) -> bool:
        if not self.enabled or aiohttp is None:
            return False
        symbol_key = str(symbol or "").upper().strip()
        if not symbol_key:
            return False
        await self._touch(symbol_key)
        self._ensure_started()
        return await self._refresh_symbol(symbol_key)

    async def run_forever(self) -> None:
        while self.enabled:
            try:
                symbols = await self._active_symbols()
                if symbols:
                    semaphore = asyncio.Semaphore(self.concurrency)

                    async def refresh_one(symbol: str) -> None:
                        async with semaphore:
                            await self._refresh_symbol(symbol)

                    await asyncio.gather(*(refresh_one(symbol) for symbol in symbols), return_exceptions=True)
                await asyncio.sleep(self.poll_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("Smart money polling failed: %s", exc)
                await asyncio.sleep(min(30, self.poll_interval_sec))

    def _ensure_started(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run_forever(), name="smart_money_poll")

    def start(self) -> None:
        if self.enabled and aiohttp is not None:
            self._save_state(force=True)
            self._ensure_started()

    async def close(self) -> None:
        task = self._task
        self._task = None
        tasks = list(self._refresh_tasks)
        self._refresh_tasks.clear()
        if task is not None and not task.done():
            task.cancel()
            tasks.append(task)
        for refresh_task in tasks:
            if not refresh_task.done():
                refresh_task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.enabled:
            async with self._lock:
                self._save_state(force=True)

    async def _touch(self, symbol: str) -> None:
        if not symbol:
            return
        async with self._lock:
            self._tracked_symbols[symbol] = time.time()

    async def _cached_state(self, symbol: str) -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        async with self._lock:
            value = self._cache.get(symbol)
            cached = copy.deepcopy(value) if isinstance(value, dict) else None
            history = [copy.deepcopy(row) for row in self._history.get(symbol, ())]
            return cached, history

    async def _active_symbols(self) -> List[str]:
        now = time.time()
        async with self._lock:
            self._tracked_symbols = {
                symbol: touched_at
                for symbol, touched_at in self._tracked_symbols.items()
                if now - touched_at <= self.track_ttl_sec
            }
            ordered = sorted(self._tracked_symbols.items(), key=lambda item: item[1], reverse=True)
            result: List[str] = []
            for symbol, _ in ordered[: self.max_active_symbols]:
                cached = self._cache.get(symbol) or {}
                observed_at_ms = _safe_int(cached.get("observed_at_ms"), 0)
                if not observed_at_ms or now - observed_at_ms / 1000.0 >= self.poll_interval_sec:
                    result.append(symbol)
            return result

    def _schedule_refresh(self, symbol: str) -> None:
        if not symbol or symbol in self._refreshing:
            return

        async def runner() -> None:
            await self._refresh_symbol(symbol)

        task = asyncio.create_task(runner(), name=f"smart_money_refresh_{symbol}")
        self._refresh_tasks.add(task)
        task.add_done_callback(self._refresh_tasks.discard)

    async def _refresh_symbol(self, symbol: str) -> bool:
        async with self._lock:
            if symbol in self._refreshing:
                return False
            self._refreshing.add(symbol)
        try:
            rows = await self._request_symbol(symbol)
            if not rows:
                return False
            await self._record(symbol, rows)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Smart money refresh failed: symbol=%s err=%s", symbol, exc)
            return False
        finally:
            async with self._lock:
                self._refreshing.discard(symbol)

    async def _request_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        if time.time() < self._blocked_until:
            return None
        timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
        headers = {
            "Accept": "application/json",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "clienttype": "web",
            "lang": "zh-CN",
        }
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            overview = await self._get_data(
                session,
                self.OVERVIEW_PATH,
                {"symbol": symbol},
            )
            if not overview:
                return None
            stat_tasks = {
                time_range: self._get_data(
                    session,
                    self.STATS_PATH,
                    {"symbol": symbol, "timeRange": time_range},
                )
                for time_range in self.stats_time_ranges
            }
            results = await asyncio.gather(*stat_tasks.values(), return_exceptions=True)
        stats: Dict[str, Dict[str, Any]] = {}
        for time_range, value in zip(stat_tasks.keys(), results):
            if isinstance(value, dict):
                stats[time_range] = value
        return {
            "symbol": symbol,
            "observed_at_ms": int(time.time() * 1000),
            "overview": overview,
            "stats": stats,
            "stats_requested": list(self.stats_time_ranges),
            "source": "binance_smart_money_bapi",
        }

    async def _get_data(
        self,
        session: Any,
        path: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with self._request_lock:
            now = time.time()
            if now < self._blocked_until:
                raise SmartMoneyRateLimited("smart_money_circuit_open")
            elapsed = time.monotonic() - self._last_request_monotonic
            delay = self.request_spacing_sec - elapsed
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_request_monotonic = time.monotonic()
            async with session.get(url, params=params, proxy=self._proxy) as response:
                if int(response.status) in {403, 418, 429}:
                    retry_after = _safe_float(response.headers.get("Retry-After"), 0.0)
                    cooldown = max(self.rate_limit_cooldown_sec, retry_after)
                    self._blocked_until = time.time() + cooldown
                    raise SmartMoneyRateLimited(f"smart_money_http_{response.status}")
                response.raise_for_status()
                payload = await response.json(content_type=None)
        if not isinstance(payload, dict) or payload.get("success") is not True:
            return {}
        data = payload.get("data")
        return dict(data) if isinstance(data, dict) else {}

    async def _record(self, symbol: str, sample: Dict[str, Any]) -> None:
        async with self._lock:
            self._cache[symbol] = dict(sample)
            history = self._history.setdefault(symbol, deque(maxlen=self.max_history_samples))
            observed_at_ms = _safe_int(sample.get("observed_at_ms"), 0)
            previous_ts = _safe_int(history[-1].get("observed_at_ms"), 0) if history else 0
            if not previous_ts or observed_at_ms - previous_ts >= self.min_sample_interval_sec * 1000:
                history.append(dict(sample))
                self._append_history(sample)
            self._save_state(force=False)

    def _append_history(self, sample: Dict[str, Any]) -> None:
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        compact = {
            "symbol": sample.get("symbol"),
            "observed_at_ms": sample.get("observed_at_ms"),
            "overview": sample.get("overview"),
            "stats": sample.get("stats"),
            "stats_requested": sample.get("stats_requested"),
            "source": sample.get("source"),
        }
        with self.history_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(compact, ensure_ascii=False, separators=(",", ":")) + "\n")

    def _load_state(self) -> None:
        if not self.enabled or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        cache = payload.get("cache")
        if isinstance(cache, dict):
            self._cache = {str(key).upper(): dict(value) for key, value in cache.items() if isinstance(value, dict)}
        history = payload.get("history")
        if isinstance(history, dict):
            for symbol, rows in history.items():
                if not isinstance(rows, list):
                    continue
                buffer: Deque[Dict[str, Any]] = deque(maxlen=self.max_history_samples)
                for row in rows[-self.max_history_samples :]:
                    if isinstance(row, dict):
                        buffer.append(dict(row))
                if buffer:
                    self._history[str(symbol).upper()] = buffer

    def _save_state(self, *, force: bool) -> None:
        now = time.time()
        if not force and now - self._last_save_ts < self.save_interval_sec:
            return
        self._last_save_ts = now
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "updated_at_ms": int(now * 1000),
            "cache": self._cache,
            "history": {symbol: list(rows) for symbol, rows in self._history.items()},
        }
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(self.state_path)


def build_smart_money_metrics(
    sample: Dict[str, Any],
    *,
    price: float,
    history: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    overview = sample.get("overview") if isinstance(sample.get("overview"), dict) else {}
    stats = sample.get("stats") if isinstance(sample.get("stats"), dict) else {}
    observed_at_ms = _safe_int(sample.get("observed_at_ms"), 0)
    result: Dict[str, Any] = {
        "source": str(sample.get("source") or "binance_smart_money_bapi"),
        "symbol": str(sample.get("symbol") or overview.get("symbol") or "").upper(),
        "observed_at_ms": observed_at_ms,
        "cohort_update_time_ms": _safe_int(overview.get("updateTime"), 0),
        "price": max(0.0, _safe_float(price, 0.0)),
        "total_positions_raw": _safe_float(overview.get("totalPositions"), 0.0),
        "total_traders": _safe_int(overview.get("totalTraders"), 0),
        "long_short_ratio": _safe_float(overview.get("longShortRatio"), 0.0),
    }
    requested_ranges = sample.get("stats_requested")
    if not isinstance(requested_ranges, list):
        requested_ranges = []
    available_ranges = sorted(str(key) for key, value in stats.items() if isinstance(value, dict) and value)
    result["stats_time_ranges_available"] = available_ranges
    result["stats_complete"] = bool(available_ranges) and all(
        str(value) in available_ranges for value in requested_ranges
    )
    result.update(_cohort_metrics(overview, price=price, prefix="trader", whale=False))
    result.update(_cohort_metrics(overview, price=price, prefix="whale", whale=True))
    result["trader_total_notional_usdt"] = _safe_float(
        result.get("trader_long_notional_usdt"),
        0.0,
    ) + _safe_float(result.get("trader_short_notional_usdt"), 0.0)
    result["whale_total_notional_usdt"] = _safe_float(
        result.get("whale_long_notional_usdt"),
        0.0,
    ) + _safe_float(result.get("whale_short_notional_usdt"), 0.0)
    for time_range, raw in stats.items():
        if not isinstance(raw, dict):
            continue
        label = _time_range_label(time_range)
        buy_usdt = _safe_float(raw.get("longPositions"), 0.0)
        sell_usdt = _safe_float(raw.get("shortPositions"), 0.0)
        whale_buy = _safe_float(raw.get("longWhalePositions"), 0.0)
        whale_sell = _safe_float(raw.get("shortWhalePositions"), 0.0)
        result[f"flow_buy_usdt_{label}"] = buy_usdt
        result[f"flow_sell_usdt_{label}"] = sell_usdt
        result[f"flow_imbalance_{label}"] = _signed_share(buy_usdt, sell_usdt)
        result[f"flow_buy_traders_{label}"] = _safe_int(raw.get("longTraders"), 0)
        result[f"flow_sell_traders_{label}"] = _safe_int(raw.get("shortTraders"), 0)
        result[f"whale_flow_buy_usdt_{label}"] = whale_buy
        result[f"whale_flow_sell_usdt_{label}"] = whale_sell
        result[f"whale_flow_imbalance_{label}"] = _signed_share(whale_buy, whale_sell)
    _append_history_changes(result, history or [])
    result["data_available"] = bool(
        result.get("total_traders")
        and (result.get("trader_long_qty", 0.0) > 0 or result.get("trader_short_qty", 0.0) > 0)
    )
    return result


class SmartMoneyRateLimited(RuntimeError):
    pass


def _cohort_metrics(
    overview: Dict[str, Any],
    *,
    price: float,
    prefix: str,
    whale: bool,
) -> Dict[str, Any]:
    base = "Whales" if whale else "Traders"
    long_count = _safe_int(overview.get(f"long{base}"), 0)
    short_count = _safe_int(overview.get(f"short{base}"), 0)
    long_qty = _safe_float(overview.get(f"long{base}Qty"), 0.0)
    short_qty = _safe_float(overview.get(f"short{base}Qty"), 0.0)
    long_avg = _safe_float(overview.get(f"long{base}AvgEntryPrice"), 0.0)
    short_avg = _safe_float(overview.get(f"short{base}AvgEntryPrice"), 0.0)
    long_profit = _safe_int(overview.get(f"longProfit{base}"), 0)
    short_profit = _safe_int(overview.get(f"shortProfit{base}"), 0)
    mark = max(0.0, _safe_float(price, 0.0))
    long_notional = long_qty * mark if mark > 0 else 0.0
    short_notional = short_qty * mark if mark > 0 else 0.0
    total_notional = long_notional + short_notional
    return {
        f"{prefix}_long_count": long_count,
        f"{prefix}_short_count": short_count,
        f"{prefix}_long_qty": long_qty,
        f"{prefix}_short_qty": short_qty,
        f"{prefix}_long_avg_entry": long_avg,
        f"{prefix}_short_avg_entry": short_avg,
        f"{prefix}_long_notional_usdt": long_notional,
        f"{prefix}_short_notional_usdt": short_notional,
        f"{prefix}_long_share": (long_notional / total_notional) if total_notional > 0 else 0.0,
        f"{prefix}_short_share": (short_notional / total_notional) if total_notional > 0 else 0.0,
        f"{prefix}_long_unrealized_pnl_usdt": (mark - long_avg) * long_qty if mark > 0 and long_avg > 0 else 0.0,
        f"{prefix}_short_unrealized_pnl_usdt": (short_avg - mark) * short_qty if mark > 0 and short_avg > 0 else 0.0,
        f"{prefix}_long_entry_gap_pct": ((long_avg - mark) / long_avg) if mark > 0 and long_avg > 0 else 0.0,
        f"{prefix}_short_entry_gap_pct": ((mark - short_avg) / short_avg) if mark > 0 and short_avg > 0 else 0.0,
        f"{prefix}_long_profit_ratio": (long_profit / long_count) if long_count > 0 else 0.0,
        f"{prefix}_short_profit_ratio": (short_profit / short_count) if short_count > 0 else 0.0,
    }


def _append_history_changes(result: Dict[str, Any], history: List[Dict[str, Any]]) -> None:
    now_ms = _safe_int(result.get("observed_at_ms"), 0)
    cohort_time = _safe_int(result.get("cohort_update_time_ms"), 0)
    if now_ms <= 0:
        return
    for minutes in (5, 15, 30):
        previous_sample = _sample_at_or_before(history, now_ms - minutes * 60 * 1000)
        if not previous_sample:
            continue
        previous = build_smart_money_metrics(previous_sample, price=_safe_float(result.get("price"), 0.0), history=[])
        if cohort_time and _safe_int(previous.get("cohort_update_time_ms"), 0) != cohort_time:
            continue
        for cohort in ("trader", "whale"):
            for side in ("long", "short"):
                key = f"{cohort}_{side}_qty"
                previous_qty = _safe_float(previous.get(key), 0.0)
                current_qty = _safe_float(result.get(key), 0.0)
                if previous_qty > 0:
                    result[f"{key}_change_pct_{minutes}m"] = (current_qty - previous_qty) / previous_qty
            for side in ("long", "short"):
                key = f"{cohort}_{side}_profit_ratio"
                result[f"{key}_change_{minutes}m"] = _safe_float(result.get(key), 0.0) - _safe_float(
                    previous.get(key),
                    0.0,
                )


def _sample_at_or_before(history: List[Dict[str, Any]], target_ms: int) -> Optional[Dict[str, Any]]:
    candidates = [
        row
        for row in history
        if isinstance(row, dict) and _safe_int(row.get("observed_at_ms"), 0) <= target_ms
    ]
    return max(candidates, key=lambda row: _safe_int(row.get("observed_at_ms"), 0)) if candidates else None


def _signed_share(buy: float, sell: float) -> float:
    total = buy + sell
    return (buy - sell) / total if total > 0 else 0.0


def _time_range_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"30m": "30m", "1h": "1h", "24h": "24h", "7d": "7d", "all": "all"}.get(text, text)


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default
