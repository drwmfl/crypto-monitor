from __future__ import annotations

import asyncio
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
    from factors.factor_models import FactorSnapshot, recent_health_entry
    from factors.oi_history import OIHistoryStore, classify_oi_regime
except ModuleNotFoundError:
    from apps.market_monitor.backend.factors.factor_models import FactorSnapshot, recent_health_entry
    from apps.market_monitor.backend.factors.oi_history import OIHistoryStore, classify_oi_regime

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
        self.fetch_orderbook = _parse_bool(self.settings.get("fetch_orderbook"), True)
        self.fetch_liquidations = _parse_bool(self.settings.get("fetch_liquidations"), False)
        self.liquidation_lookback_minutes = max(1, _safe_int(self.settings.get("liquidation_lookback_minutes"), 5))
        self.open_interest_hist_period = str(self.settings.get("open_interest_hist_period") or "5m").strip() or "5m"
        self.open_interest_hist_limit = max(2, _safe_int(self.settings.get("open_interest_hist_limit"), 576))
        self.taker_buy_sell_period = str(self.settings.get("taker_buy_sell_period") or "5m").strip() or "5m"
        oi_history_settings = self.settings.get("oi_history", {}) if isinstance(self.settings.get("oi_history"), dict) else {}
        if self.settings.get("runtime_dir") and not oi_history_settings.get("runtime_dir"):
            oi_history_settings = dict(oi_history_settings)
            oi_history_settings["runtime_dir"] = self.settings.get("runtime_dir")
        self.oi_history = OIHistoryStore(oi_history_settings)
        self.oi_signal_thresholds = dict(
            self.settings.get("oi_signal_thresholds") or oi_history_settings.get("signal_thresholds") or {}
        )
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
            tasks.append(self._fetch_derivatives(symbol=symbol, price=price, context=context or {}))
        if self.fetch_orderbook:
            tasks.append(self._fetch_orderbook(symbol=symbol))
        if self.fetch_liquidations:
            tasks.append(self._fetch_liquidations(symbol=symbol))

        if not tasks:
            snapshot.source_health["binance"] = recent_health_entry("binance", True, "no_factor_enabled")
            return snapshot

        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok_count = 0
        for item in results:
            if isinstance(item, Exception):
                logger.debug("Binance factor task failed: symbol=%s err=%s", symbol, item)
                continue
            if not isinstance(item, dict):
                continue
            if item.get("kind") == "derivatives":
                snapshot.derivatives.update(item.get("data") or {})
                ok_count += 1
            elif item.get("kind") == "orderbook":
                snapshot.orderbook.update(item.get("data") or {})
                ok_count += 1
            elif item.get("kind") == "liquidation":
                snapshot.liquidation.update(item.get("data") or {})
                ok_count += 1

        snapshot.source_health["binance"] = recent_health_entry(
            "binance",
            ok_count > 0,
            f"completed={ok_count}/{len(tasks)}",
        )
        return snapshot

    async def _fetch_derivatives(self, symbol: str, price: Optional[float], context: Dict[str, Any]) -> Dict[str, Any]:
        data: Dict[str, Any] = {"source": "binance_futures_rest", "updated_at_ms": int(time.time() * 1000)}
        async with self._client_session() as session:
            if self.fetch_open_interest:
                payload = await self._get_json(session, "/fapi/v1/openInterest", {"symbol": symbol})
                oi_amount = _safe_float(payload.get("openInterest"), 0.0) if isinstance(payload, dict) else 0.0
                data["oi_amount"] = oi_amount
                if price and price > 0 and oi_amount > 0:
                    data["oi_usdt"] = oi_amount * float(price)
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
                    )
                    if isinstance(payload, list):
                        self.oi_history.merge_historical_rows(symbol, payload)
                        self.oi_history.mark_bootstrapped(symbol)
                except Exception as exc:
                    data["oi_history_error"] = str(exc)[:160]
            if self.fetch_funding:
                payload = await self._get_json(session, "/fapi/v1/premiumIndex", {"symbol": symbol})
                if isinstance(payload, dict):
                    data["funding_rate"] = _safe_float(payload.get("lastFundingRate"), 0.0)
                    data["mark_price"] = _safe_float(payload.get("markPrice"), 0.0)
            if self.fetch_long_short_ratio:
                payload = await self._get_json(
                    session,
                    "/futures/data/globalLongShortAccountRatio",
                    {"symbol": symbol, "period": "5m", "limit": 1},
                )
                if isinstance(payload, list) and payload:
                    latest = payload[-1]
                    if isinstance(latest, dict):
                        data["long_short_ratio"] = _safe_float(latest.get("longShortRatio"), 0.0)
                        data["long_account_pct"] = _safe_float(latest.get("longAccount"), 0.0)
                        data["short_account_pct"] = _safe_float(latest.get("shortAccount"), 0.0)
            if self.fetch_taker_buy_sell:
                try:
                    payload = await self._get_json(
                        session,
                        "/futures/data/takerlongshortRatio",
                        {"symbol": symbol, "period": self.taker_buy_sell_period, "limit": 1},
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
        return {"kind": "derivatives", "data": data}

    async def _fetch_orderbook(self, symbol: str) -> Dict[str, Any]:
        async with self._client_session() as session:
            payload = await self._get_json(session, "/fapi/v1/depth", {"symbol": symbol, "limit": self.depth_limit})
        if not isinstance(payload, dict):
            return {"kind": "orderbook", "data": {}}

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
        async with self._client_session() as session:
            payload = await self._get_json(
                session,
                "/fapi/v1/allForceOrders",
                {"symbol": symbol, "startTime": start_ms, "endTime": now_ms, "limit": 100},
            )
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

    async def _get_json(self, session: Any, path: str, params: Dict[str, Any]) -> Any:
        url = f"{self.base_url}{path}"
        async with session.get(url, params=params, proxy=self._proxy) as response:
            response.raise_for_status()
            return await response.json()

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
        return int(value)
    except (TypeError, ValueError):
        return default
