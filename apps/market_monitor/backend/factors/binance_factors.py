from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from typing import Any, Dict, Optional

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None

try:
    from candidates.storage_paths import resolve_runtime_dir
    from factors.derivatives_history import (
        DerivativesHistoryStore,
        classify_derivatives_shadow,
    )
    from factors.factor_models import FactorSnapshot, recent_health_entry
    from factors.oi_history import OIHistoryStore, classify_oi_regime, classify_oi_regime_shadow
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir
    from apps.market_monitor.backend.factors.derivatives_history import (
        DerivativesHistoryStore,
        classify_derivatives_shadow,
    )
    from apps.market_monitor.backend.factors.factor_models import FactorSnapshot, recent_health_entry
    from apps.market_monitor.backend.factors.oi_history import (
        OIHistoryStore,
        classify_oi_regime,
        classify_oi_regime_shadow,
    )

logger = logging.getLogger(__name__)


class BinanceFactorProvider:
    def __init__(self, settings: Dict[str, Any]) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.base_url = str(self.settings.get("base_url") or "https://fapi.binance.com").rstrip("/")
        self.timeout_sec = max(0.5, _safe_float(self.settings.get("timeout_sec"), 3.0))
        self.depth_limit = max(5, _safe_int(self.settings.get("depth_limit"), 100))
        self.depth_levels = max(1, _safe_int(self.settings.get("depth_levels"), 20))
        self.fetch_open_interest = _parse_bool(self.settings.get("fetch_open_interest"), True)
        self.fetch_funding = _parse_bool(self.settings.get("fetch_funding"), True)
        self.fetch_long_short_ratio = _parse_bool(self.settings.get("fetch_long_short_ratio"), False)
        self.fetch_taker_buy_sell = _parse_bool(self.settings.get("fetch_taker_buy_sell"), True)
        self.fetch_open_interest_hist = _parse_bool(self.settings.get("fetch_open_interest_hist"), True)
        self.fetch_basis_history = _parse_bool(self.settings.get("fetch_basis_history"), True)
        self.fetch_orderbook = _parse_bool(self.settings.get("fetch_orderbook"), True)
        self.fetch_liquidations = _parse_bool(self.settings.get("fetch_liquidations"), False)
        self.liquidation_lookback_minutes = max(1, _safe_int(self.settings.get("liquidation_lookback_minutes"), 5))
        self.open_interest_hist_period = str(self.settings.get("open_interest_hist_period") or "5m").strip() or "5m"
        self.open_interest_hist_limit = max(2, _safe_int(self.settings.get("open_interest_hist_limit"), 576))
        self.basis_history_period = str(self.settings.get("basis_history_period") or "5m").strip() or "5m"
        self.basis_history_limit = max(20, min(500, _safe_int(self.settings.get("basis_history_limit"), 500)))
        self.taker_buy_sell_period = str(self.settings.get("taker_buy_sell_period") or "5m").strip() or "5m"
        self.default_funding_interval_hours = max(
            1.0,
            _safe_float(self.settings.get("default_funding_interval_hours"), 8.0),
        )
        self.funding_info_refresh_sec = max(
            300,
            _safe_int(self.settings.get("funding_info_refresh_sec"), 3600),
        )
        self.request_diagnostics_enabled = _parse_bool(self.settings.get("request_diagnostics_enabled"), True)
        request_error_file = str(self.settings.get("request_error_file") or "factor_source_errors.jsonl").strip()
        self.request_error_path = resolve_runtime_dir(self.settings) / (request_error_file or "factor_source_errors.jsonl")
        oi_history_settings = self.settings.get("oi_history", {}) if isinstance(self.settings.get("oi_history"), dict) else {}
        if self.settings.get("runtime_dir") and not oi_history_settings.get("runtime_dir"):
            oi_history_settings = dict(oi_history_settings)
            oi_history_settings["runtime_dir"] = self.settings.get("runtime_dir")
        self.oi_history = OIHistoryStore(oi_history_settings)
        derivatives_history_settings = (
            dict(self.settings.get("derivatives_history", {}) or {})
            if isinstance(self.settings.get("derivatives_history"), dict)
            else {}
        )
        if self.settings.get("runtime_dir") and not derivatives_history_settings.get("runtime_dir"):
            derivatives_history_settings["runtime_dir"] = self.settings.get("runtime_dir")
        self.derivatives_history = DerivativesHistoryStore(derivatives_history_settings)
        self.oi_signal_thresholds = dict(
            self.settings.get("oi_signal_thresholds") or oi_history_settings.get("signal_thresholds") or {}
        )
        self.derivatives_shadow_thresholds = dict(
            self.settings.get("derivatives_shadow_thresholds")
            or derivatives_history_settings.get("signal_thresholds")
            or {}
        )
        self._funding_interval_by_symbol: Dict[str, float] = {}
        self._funding_info_loaded_at = 0.0
        self._funding_info_lock = asyncio.Lock()
        self._proxy = (
            os.getenv("ALERT_HTTPS_PROXY")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("https_proxy")
            or os.getenv("ALERT_HTTP_PROXY")
            or os.getenv("HTTP_PROXY")
            or os.getenv("http_proxy")
        )

    async def fetch(
        self,
        symbol: str,
        base_asset: str,
        price: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> FactorSnapshot:
        snapshot = FactorSnapshot.empty(symbol=symbol, base_asset=base_asset)
        if not self.enabled:
            snapshot.source_health["binance"] = recent_health_entry("binance", True, "disabled")
            return snapshot
        if aiohttp is None:
            snapshot.source_health["binance"] = recent_health_entry("binance", False, "aiohttp_missing")
            return snapshot

        tasks = []
        if (
            self.fetch_open_interest
            or self.fetch_funding
            or self.fetch_long_short_ratio
            or self.fetch_taker_buy_sell
            or self.fetch_open_interest_hist
        ):
            tasks.append(
                self._fetch_derivatives(
                    symbol=symbol,
                    price=price,
                    context=context or {},
                )
            )
        if self.fetch_orderbook:
            tasks.append(self._fetch_orderbook(symbol=symbol))
        if self.fetch_liquidations:
            tasks.append(self._fetch_liquidations(symbol=symbol))

        if not tasks:
            snapshot.source_health["binance"] = recent_health_entry("binance", True, "no_factor_enabled")
            return snapshot

        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok_count = 0
        request_results: list[Dict[str, Any]] = []
        for item in results:
            if isinstance(item, Exception):
                logger.debug("Binance factor task failed: symbol=%s err=%s", symbol, item)
                request_results.append(
                    {
                        "endpoint": "task",
                        "ok": False,
                        "error_type": type(item).__name__,
                        "error": str(item)[:180],
                    }
                )
                continue
            if not isinstance(item, dict):
                continue
            item_diagnostics = item.get("diagnostics")
            if isinstance(item_diagnostics, list):
                request_results.extend(row for row in item_diagnostics if isinstance(row, dict))
            if item.get("kind") == "derivatives":
                snapshot.derivatives.update(item.get("data") or {})
                if item.get("ok"):
                    ok_count += 1
            elif item.get("kind") == "orderbook":
                snapshot.orderbook.update(item.get("data") or {})
                if item.get("ok"):
                    ok_count += 1
            elif item.get("kind") == "liquidation":
                snapshot.liquidation.update(item.get("data") or {})
                if item.get("ok"):
                    ok_count += 1

        failed_endpoints = [
            str(row.get("endpoint") or "")
            for row in request_results
            if isinstance(row, dict) and row.get("ok") is False and str(row.get("endpoint") or "")
        ]
        health = recent_health_entry("binance", ok_count > 0, f"completed={ok_count}/{len(tasks)}")
        if self.request_diagnostics_enabled:
            health.update(
                {
                    "ok_count": ok_count,
                    "task_count": len(tasks),
                    "failed_endpoints": failed_endpoints[:8],
                    "details": request_results[-16:],
                }
            )
        snapshot.source_health["binance"] = health
        self._annotate_derivatives_shadow(snapshot, context=context or {})
        return snapshot

    async def prewarm(
        self,
        symbol: str,
        base_asset: str,
        *,
        price: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not self.enabled or aiohttp is None:
            return False
        diagnostics: list[Dict[str, Any]] = []
        premium_payload: Dict[str, Any] = {}
        async with self._client_session() as session:
            if self.fetch_basis_history and self.derivatives_history.should_bootstrap_basis(symbol):
                try:
                    payload = await self._get_json(
                        session,
                        "/futures/data/basis",
                        {
                            "pair": symbol,
                            "contractType": "PERPETUAL",
                            "period": self.basis_history_period,
                            "limit": self.basis_history_limit,
                        },
                        diagnostics=diagnostics,
                        endpoint="basis_history",
                    )
                    if isinstance(payload, list):
                        self.derivatives_history.merge_basis_rows(symbol, payload)
                        self.derivatives_history.mark_basis_bootstrapped(symbol)
                except Exception:
                    pass
            if self.fetch_funding:
                try:
                    payload = await self._get_json(
                        session,
                        "/fapi/v1/premiumIndex",
                        {"symbol": symbol},
                        diagnostics=diagnostics,
                        endpoint="premium_index_shadow_prewarm",
                    )
                    if isinstance(payload, dict):
                        premium_payload = payload
                except Exception:
                    pass
            interval_hours = await self._funding_interval_hours(
                session,
                symbol,
                diagnostics,
                refresh=True,
            )

        index_price = _safe_float(premium_payload.get("indexPrice"), 0.0)
        mark_price = _safe_float(premium_payload.get("markPrice"), 0.0)
        mark_basis_bps = (
            (mark_price - index_price) / index_price * 10000.0
            if mark_price > 0 and index_price > 0
            else None
        )
        funding_rate = _safe_optional_float(premium_payload.get("lastFundingRate"))
        if mark_basis_bps is not None or funding_rate is not None:
            self.derivatives_history.record_current(
                symbol,
                market_basis_bps=None,
                mark_basis_bps=mark_basis_bps,
                funding_rate=funding_rate,
                funding_interval_hours=interval_hours,
                index_price=index_price,
                mark_price=mark_price,
                timestamp_ms=_safe_int(premium_payload.get("time"), 0) or int(time.time() * 1000),
            )
        return _any_request_ok(diagnostics)

    async def _fetch_derivatives(
        self,
        symbol: str,
        price: Optional[float],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {"source": "binance_futures_rest", "updated_at_ms": int(time.time() * 1000)}
        diagnostics: list[Dict[str, Any]] = []
        async with self._client_session() as session:
            if self.fetch_open_interest:
                try:
                    payload = await self._get_json(
                        session,
                        "/fapi/v1/openInterest",
                        {"symbol": symbol},
                        diagnostics=diagnostics,
                        endpoint="open_interest",
                    )
                    oi_amount = _safe_float(payload.get("openInterest"), 0.0) if isinstance(payload, dict) else 0.0
                    data["oi_amount"] = oi_amount
                    if price and price > 0 and oi_amount > 0:
                        data["oi_usdt"] = oi_amount * float(price)
                except Exception as exc:
                    data["open_interest_error"] = str(exc)[:160]
            if self.fetch_open_interest_hist and self.oi_history.should_bootstrap(symbol):
                try:
                    payload = await self._get_json(
                        session,
                        "/futures/data/openInterestHist",
                        {
                            "symbol": symbol,
                            "period": self.open_interest_hist_period,
                            "limit": self.open_interest_hist_limit,
                        },
                        diagnostics=diagnostics,
                        endpoint="open_interest_hist",
                    )
                    if isinstance(payload, list):
                        self.oi_history.merge_historical_rows(symbol, payload)
                        self.oi_history.mark_bootstrapped(symbol)
                except Exception as exc:
                    data["oi_history_error"] = str(exc)[:160]
            if self.fetch_funding:
                try:
                    payload = await self._get_json(
                        session,
                        "/fapi/v1/premiumIndex",
                        {"symbol": symbol},
                        diagnostics=diagnostics,
                        endpoint="premium_index",
                    )
                    if isinstance(payload, dict):
                        data["funding_rate"] = _safe_float(payload.get("lastFundingRate"), 0.0)
                        data["mark_price"] = _safe_float(payload.get("markPrice"), 0.0)
                        data["index_price"] = _safe_float(payload.get("indexPrice"), 0.0)
                        data["estimated_settle_price"] = _safe_float(payload.get("estimatedSettlePrice"), 0.0)
                        data["next_funding_time_ms"] = _safe_int(payload.get("nextFundingTime"), 0)
                        data["premium_index_time_ms"] = _safe_int(payload.get("time"), 0)
                        data["funding_interval_hours"] = await self._funding_interval_hours(
                            session,
                            symbol,
                            diagnostics,
                            refresh=False,
                        )
                except Exception as exc:
                    data["funding_error"] = str(exc)[:160]
            if self.fetch_long_short_ratio:
                try:
                    payload = await self._get_json(
                        session,
                        "/futures/data/globalLongShortAccountRatio",
                        {"symbol": symbol, "period": "5m", "limit": 1},
                        diagnostics=diagnostics,
                        endpoint="long_short_ratio",
                    )
                    if isinstance(payload, list) and payload:
                        latest = payload[-1]
                        if isinstance(latest, dict):
                            data["long_short_ratio"] = _safe_float(latest.get("longShortRatio"), 0.0)
                            data["long_account_pct"] = _safe_float(latest.get("longAccount"), 0.0)
                            data["short_account_pct"] = _safe_float(latest.get("shortAccount"), 0.0)
                except Exception as exc:
                    data["long_short_ratio_error"] = str(exc)[:160]
            if self.fetch_taker_buy_sell:
                try:
                    payload = await self._get_json(
                        session,
                        "/futures/data/takerlongshortRatio",
                        {"symbol": symbol, "period": self.taker_buy_sell_period, "limit": 1},
                        diagnostics=diagnostics,
                        endpoint="taker_buy_sell",
                    )
                    if isinstance(payload, list) and payload:
                        latest = payload[-1]
                        if isinstance(latest, dict):
                            buy_vol = _safe_float(latest.get("buyVol"), 0.0)
                            sell_vol = _safe_float(latest.get("sellVol"), 0.0)
                            total_vol = buy_vol + sell_vol
                            data["taker_buy_vol"] = buy_vol
                            data["taker_sell_vol"] = sell_vol
                            data["taker_buy_sell_ratio"] = _safe_float(latest.get("buySellRatio"), 0.0)
                            if total_vol > 0:
                                data["taker_buy_ratio"] = buy_vol / total_vol
                            if latest.get("timestamp"):
                                data["taker_buy_sell_ts"] = _safe_int(latest.get("timestamp"), 0)
                except Exception as exc:
                    data["taker_buy_sell_error"] = str(exc)[:160]
        oi_amount = _safe_float(data.get("oi_amount"), 0.0)
        mark_price = _safe_float(data.get("mark_price"), 0.0)
        if oi_amount > 0 and mark_price > 0:
            data["oi_usdt"] = oi_amount * mark_price
        oi_usdt = _safe_float(data.get("oi_usdt"), 0.0)
        if oi_amount > 0 or oi_usdt > 0:
            self.oi_history.record_current(symbol, oi_amount=oi_amount, oi_usdt=oi_usdt, price=price)
        oi_metrics = self.oi_history.metrics(symbol)
        if oi_metrics:
            data.update(oi_metrics)
        data.update(classify_oi_regime(data, context=context, thresholds=self.oi_signal_thresholds))
        data.update(classify_oi_regime_shadow(data, context=context, thresholds=self.oi_signal_thresholds))
        return {"kind": "derivatives", "data": data, "ok": _any_request_ok(diagnostics), "diagnostics": diagnostics}

    def _annotate_derivatives_shadow(self, snapshot: FactorSnapshot, *, context: Dict[str, Any]) -> None:
        if not self.derivatives_history.enabled:
            return
        derivatives = snapshot.derivatives
        orderbook = snapshot.orderbook
        index_price = _safe_float(derivatives.get("index_price"), 0.0)
        mark_price = _safe_float(derivatives.get("mark_price"), 0.0)
        best_bid = _safe_float(orderbook.get("best_bid"), 0.0)
        best_ask = _safe_float(orderbook.get("best_ask"), 0.0)
        market_mid = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
        spread_bps = _safe_float(orderbook.get("spread_bps"), 0.0)
        max_spread_bps = max(0.0, _safe_float(self.settings.get("basis_max_spread_bps"), 30.0))

        market_basis_bps: Optional[float] = None
        mark_basis_bps: Optional[float] = None
        if index_price > 0 and market_mid > 0 and (max_spread_bps <= 0 or spread_bps <= max_spread_bps):
            market_basis_bps = (market_mid - index_price) / index_price * 10000.0
        if index_price > 0 and mark_price > 0:
            mark_basis_bps = (mark_price - index_price) / index_price * 10000.0

        derivatives["market_mid_price"] = market_mid
        derivatives["market_basis_bps"] = market_basis_bps
        derivatives["mark_basis_bps"] = mark_basis_bps
        derivatives["basis_market_valid"] = market_basis_bps is not None
        derivatives["basis_market_spread_bps"] = spread_bps
        funding_rate = _safe_optional_float(derivatives.get("funding_rate"))
        funding_interval_hours = max(
            1.0,
            _safe_float(derivatives.get("funding_interval_hours"), self.default_funding_interval_hours),
        )
        timestamp_ms = _safe_int(derivatives.get("premium_index_time_ms"), 0) or int(time.time() * 1000)
        self.derivatives_history.record_current(
            snapshot.symbol,
            market_basis_bps=market_basis_bps,
            mark_basis_bps=mark_basis_bps,
            funding_rate=funding_rate,
            funding_interval_hours=funding_interval_hours,
            index_price=index_price,
            mark_price=mark_price,
            market_mid_price=market_mid,
            timestamp_ms=timestamp_ms,
        )
        metrics = self.derivatives_history.metrics(snapshot.symbol)
        if metrics:
            derivatives.update(metrics)
        derivatives.update(
            classify_derivatives_shadow(
                derivatives,
                context=context,
                thresholds=self.derivatives_shadow_thresholds,
            )
        )

    async def _funding_interval_hours(
        self,
        session: Any,
        symbol: str,
        diagnostics: list[Dict[str, Any]],
        *,
        refresh: bool,
    ) -> float:
        now = time.time()
        if (now - self._funding_info_loaded_at) < self.funding_info_refresh_sec:
            return self._funding_interval_by_symbol.get(symbol, self.default_funding_interval_hours)
        if not refresh:
            return self._funding_interval_by_symbol.get(symbol, self.default_funding_interval_hours)
        async with self._funding_info_lock:
            now = time.time()
            if (now - self._funding_info_loaded_at) < self.funding_info_refresh_sec:
                return self._funding_interval_by_symbol.get(symbol, self.default_funding_interval_hours)
            try:
                payload = await self._get_json(
                    session,
                    "/fapi/v1/fundingInfo",
                    {},
                    diagnostics=diagnostics,
                    endpoint="funding_info",
                )
                intervals: Dict[str, float] = {}
                if isinstance(payload, list):
                    for item in payload:
                        if not isinstance(item, dict):
                            continue
                        item_symbol = str(item.get("symbol") or "").strip().upper()
                        interval = _safe_float(item.get("fundingIntervalHours"), 0.0)
                        if item_symbol and interval > 0:
                            intervals[item_symbol] = interval
                self._funding_interval_by_symbol = intervals
                self._funding_info_loaded_at = now
            except Exception:
                self._funding_info_loaded_at = now
            return self._funding_interval_by_symbol.get(symbol, self.default_funding_interval_hours)

    async def _fetch_orderbook(self, symbol: str) -> Dict[str, Any]:
        diagnostics: list[Dict[str, Any]] = []
        async with self._client_session() as session:
            try:
                payload = await self._get_json(
                    session,
                    "/fapi/v1/depth",
                    {"symbol": symbol, "limit": self.depth_limit},
                    diagnostics=diagnostics,
                    endpoint="orderbook_depth",
                )
            except Exception:
                return {"kind": "orderbook", "data": {}, "ok": False, "diagnostics": diagnostics}
        if not isinstance(payload, dict):
            return {"kind": "orderbook", "data": {}, "ok": False, "diagnostics": diagnostics}

        bids = _levels(payload.get("bids"), self.depth_levels)
        asks = _levels(payload.get("asks"), self.depth_levels)
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        mid = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
        bid_notional = sum(price * qty for price, qty in bids)
        ask_notional = sum(price * qty for price, qty in asks)
        total_notional = bid_notional + ask_notional
        imbalance = (bid_notional - ask_notional) / total_notional if total_notional > 0 else 0.0
        spread_bps = ((best_ask - best_bid) / mid * 10000.0) if mid > 0 else 0.0
        return {
            "kind": "orderbook",
            "ok": bool(bids and asks),
            "diagnostics": diagnostics,
            "data": {
                "source": "binance_futures_depth",
                "depth_limit": self.depth_limit,
                "depth_levels": self.depth_levels,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread_bps": spread_bps,
                "bid_notional": bid_notional,
                "ask_notional": ask_notional,
                "imbalance": imbalance,
                "bid_ask_ratio": (bid_notional / ask_notional) if ask_notional > 0 else 0.0,
                "updated_at_ms": int(time.time() * 1000),
            },
        }

    async def _fetch_liquidations(self, symbol: str) -> Dict[str, Any]:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - self.liquidation_lookback_minutes * 60 * 1000
        diagnostics: list[Dict[str, Any]] = []
        async with self._client_session() as session:
            try:
                payload = await self._get_json(
                    session,
                    "/fapi/v1/allForceOrders",
                    {"symbol": symbol, "startTime": start_ms, "endTime": now_ms, "limit": 100},
                    diagnostics=diagnostics,
                    endpoint="force_orders",
                )
            except Exception:
                return {"kind": "liquidation", "data": {}, "ok": False, "diagnostics": diagnostics}
        long_liq = 0.0
        short_liq = 0.0
        count = 0
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                avg_price = _safe_float(item.get("averagePrice") or item.get("price"), 0.0)
                qty = _safe_float(item.get("executedQty") or item.get("origQty"), 0.0)
                notional = avg_price * qty
                side = str(item.get("side") or "").upper()
                if side == "SELL":
                    long_liq += notional
                elif side == "BUY":
                    short_liq += notional
                count += 1
        return {
            "kind": "liquidation",
            "ok": isinstance(payload, list),
            "diagnostics": diagnostics,
            "data": {
                "source": "binance_force_orders",
                "lookback_minutes": self.liquidation_lookback_minutes,
                "long_liq_usdt": long_liq,
                "short_liq_usdt": short_liq,
                "net_short_minus_long_usdt": short_liq - long_liq,
                "order_count": count,
                "updated_at_ms": now_ms,
            },
        }

    async def _get_json(
        self,
        session: Any,
        path: str,
        params: Dict[str, Any],
        *,
        diagnostics: Optional[list[Dict[str, Any]]] = None,
        endpoint: Optional[str] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        started = time.time()
        endpoint_name = endpoint or path.strip("/") or "unknown"
        status: Optional[int] = None
        try:
            async with session.get(url, params=params, proxy=self._proxy) as response:
                status = int(response.status)
                response.raise_for_status()
                payload = await response.json()
                self._record_request_result(
                    diagnostics,
                    endpoint=endpoint_name,
                    path=path,
                    params=params,
                    ok=True,
                    status=status,
                    elapsed_ms=(time.time() - started) * 1000.0,
                    payload_shape=_payload_shape(payload),
                )
                return payload
        except Exception as exc:
            self._record_request_result(
                diagnostics,
                endpoint=endpoint_name,
                path=path,
                params=params,
                ok=False,
                status=status,
                elapsed_ms=(time.time() - started) * 1000.0,
                error_type=type(exc).__name__,
                error=str(exc)[:240],
            )
            raise

    def _record_request_result(
        self,
        diagnostics: Optional[list[Dict[str, Any]]],
        *,
        endpoint: str,
        path: str,
        params: Dict[str, Any],
        ok: bool,
        status: Optional[int],
        elapsed_ms: float,
        payload_shape: str = "",
        error_type: str = "",
        error: str = "",
    ) -> None:
        if not self.request_diagnostics_enabled:
            return
        row = {
            "ts_ms": int(time.time() * 1000),
            "endpoint": endpoint,
            "path": path,
            "symbol": str(params.get("symbol") or ""),
            "ok": bool(ok),
            "status": status,
            "elapsed_ms": round(float(elapsed_ms), 1),
            "payload_shape": payload_shape,
        }
        for key in ("period", "limit"):
            if key in params:
                row[key] = params.get(key)
        if error_type:
            row["error_type"] = error_type
        if error:
            row["error"] = error
        if diagnostics is not None:
            diagnostics.append(row)
        if not ok:
            self._append_request_error(row)

    def _append_request_error(self, row: Dict[str, Any]) -> None:
        try:
            self.request_error_path.parent.mkdir(parents=True, exist_ok=True)
            with self.request_error_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.debug("Failed to write Binance request error: err=%s", exc)

    def _client_session(self) -> Any:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
            connector=connector,
            trust_env=True,
        )


def _levels(raw: Any, limit: int) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    if not isinstance(raw, list):
        return result
    for row in raw[:limit]:
        if not isinstance(row, list) or len(row) < 2:
            continue
        price = _safe_float(row[0], 0.0)
        qty = _safe_float(row[1], 0.0)
        if price > 0 and qty > 0:
            result.append((price, qty))
    return result


def _any_request_ok(diagnostics: list[Dict[str, Any]]) -> bool:
    if not diagnostics:
        return False
    return any(bool(row.get("ok")) for row in diagnostics if isinstance(row, dict))


def _payload_shape(payload: Any) -> str:
    if isinstance(payload, list):
        return f"list:{len(payload)}"
    if isinstance(payload, dict):
        return f"dict:{len(payload)}"
    if payload is None:
        return "none"
    return type(payload).__name__


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


def _safe_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        number = float(value)
        if number != number or number in {float("inf"), float("-inf")}:
            return None
        return number
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
