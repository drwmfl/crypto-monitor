from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None

try:
    from factors.factor_models import FactorSnapshot, recent_health_entry
except ModuleNotFoundError:
    from apps.market_monitor.backend.factors.factor_models import FactorSnapshot, recent_health_entry

logger = logging.getLogger(__name__)

WINDOW_LABELS = {
    10: "10s",
    30: "30s",
    60: "1m",
    180: "3m",
    300: "5m",
}

DEFAULT_SIGNAL_THRESHOLDS: Dict[str, float] = {
    "min_trade_notional_1m": 25_000.0,
    "min_trade_notional_5m": 100_000.0,
    "new_longs_cvd_1m": 10_000.0,
    "new_longs_cvd_5m": 35_000.0,
    "new_longs_buy_ratio_1m": 0.55,
    "new_longs_buy_ratio_5m": 0.53,
    "breakout_cvd_1m": 18_000.0,
    "breakout_cvd_3m": 45_000.0,
    "breakout_cvd_5m": 80_000.0,
    "breakout_buy_ratio_1m": 0.57,
    "breakout_buy_ratio_3m": 0.55,
    "breakout_buy_ratio_5m": 0.54,
    "breakout_short_liq_share_1m_max": 0.45,
    "absorption_trade_notional_30s": 8_000.0,
    "absorption_cvd_10s": 3_500.0,
    "absorption_cvd_30s": 7_500.0,
    "absorption_cvd_1m": 8_000.0,
    "absorption_buy_ratio_10s": 0.60,
    "absorption_buy_ratio_30s": 0.58,
    "absorption_buy_ratio_1m": 0.55,
    "absorption_cvd_5m_floor": -20_000.0,
    "absorption_buy_ratio_gap": 0.08,
    "absorption_oi_15m_min": -0.01,
    "short_cover_liq_1m": 50_000.0,
    "short_cover_liq_share_1m": 0.55,
    "short_cover_oi_15m_max": 0.03,
    "churn_abs_cvd_ratio_1m": 0.08,
    "churn_abs_cvd_ratio_5m": 0.12,
    "churn_buy_ratio_delta": 0.04,
    "oi_positive_15m": 0.015,
    "oi_positive_1h": 0.015,
}


def build_microstructure_metrics(
    trades: Iterable[Tuple[int, float, float, float]],
    liquidations: Iterable[Tuple[int, float, float]],
    *,
    as_of_ms: Optional[int] = None,
    windows_sec: Optional[Sequence[int]] = None,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    now_ms = int(as_of_ms or int(time.time() * 1000))
    metrics: Dict[str, Any] = {}
    liquidation_metrics: Dict[str, Any] = {}
    normalized_windows = _normalize_windows(windows_sec)

    trade_rows = list(trades or [])
    liq_rows = list(liquidations or [])

    for window_sec in normalized_windows:
        label = WINDOW_LABELS.get(int(window_sec), f"{int(window_sec)}s")
        cutoff_ms = now_ms - int(window_sec) * 1000

        buy_notional = 0.0
        sell_notional = 0.0
        signed_notional = 0.0
        trade_count = 0
        for row_ts, signed_value, buy_value, sell_value in trade_rows:
            if int(row_ts) < cutoff_ms:
                continue
            signed_notional += float(signed_value)
            buy_notional += float(buy_value)
            sell_notional += float(sell_value)
            trade_count += 1

        total_notional = buy_notional + sell_notional
        buy_ratio = (buy_notional / total_notional) if total_notional > 0 else 0.0
        sell_ratio = (sell_notional / total_notional) if total_notional > 0 else 0.0
        cvd_ratio = (signed_notional / total_notional) if total_notional > 0 else 0.0

        metrics[f"cvd_usdt_{label}"] = signed_notional
        metrics[f"trade_notional_usdt_{label}"] = total_notional
        metrics[f"buy_aggressor_ratio_{label}"] = buy_ratio
        metrics[f"sell_aggressor_ratio_{label}"] = sell_ratio
        metrics[f"cvd_ratio_{label}"] = cvd_ratio
        metrics[f"micro_trade_count_{label}"] = trade_count

        short_liq = 0.0
        long_liq = 0.0
        liq_count = 0
        for row_ts, short_value, long_value in liq_rows:
            if int(row_ts) < cutoff_ms:
                continue
            short_liq += float(short_value)
            long_liq += float(long_value)
            liq_count += 1

        liq_total = short_liq + long_liq
        short_share = (short_liq / liq_total) if liq_total > 0 else 0.0
        liq_imbalance = short_liq - long_liq

        liquidation_metrics[f"micro_short_liq_usdt_{label}"] = short_liq
        liquidation_metrics[f"micro_long_liq_usdt_{label}"] = long_liq
        liquidation_metrics[f"micro_liq_imbalance_usdt_{label}"] = liq_imbalance
        liquidation_metrics[f"micro_liq_total_usdt_{label}"] = liq_total
        liquidation_metrics[f"micro_short_liq_share_{label}"] = short_share
        liquidation_metrics[f"micro_liq_count_{label}"] = liq_count

        metrics[f"micro_short_liq_usdt_{label}"] = short_liq
        metrics[f"micro_long_liq_usdt_{label}"] = long_liq
        metrics[f"micro_liq_imbalance_usdt_{label}"] = liq_imbalance
        metrics[f"micro_liq_total_usdt_{label}"] = liq_total
        metrics[f"micro_short_liq_share_{label}"] = short_share
        metrics[f"micro_liq_count_{label}"] = liq_count

    return metrics, liquidation_metrics


def classify_microstructure_regime(
    data: Dict[str, Any],
    *,
    context: Optional[Dict[str, Any]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ctx = context or {}
    levels = dict(DEFAULT_SIGNAL_THRESHOLDS)
    if isinstance(thresholds, dict):
        for key, value in thresholds.items():
            if value is None:
                continue
            levels[key] = _safe_float(value, levels.get(key, 0.0))

    latest = ctx.get("latest") if isinstance(ctx.get("latest"), dict) else {}
    derivatives = ctx.get("derivatives") if isinstance(ctx.get("derivatives"), dict) else {}

    direction = str(
        ctx.get("direction")
        or latest.get("direction")
        or data.get("direction")
        or ""
    ).strip().lower()

    trade_30s = _safe_float(data.get("trade_notional_usdt_30s"), 0.0)
    trade_1m = _safe_float(data.get("trade_notional_usdt_1m"), 0.0)
    trade_5m = _safe_float(data.get("trade_notional_usdt_5m"), 0.0)

    cvd_10s = _safe_float(data.get("cvd_usdt_10s"), 0.0)
    cvd_30s = _safe_float(data.get("cvd_usdt_30s"), 0.0)
    cvd_1m = _safe_float(data.get("cvd_usdt_1m"), 0.0)
    cvd_3m = _safe_float(data.get("cvd_usdt_3m"), 0.0)
    cvd_5m = _safe_float(data.get("cvd_usdt_5m"), 0.0)

    cvd_ratio_1m = _safe_float(data.get("cvd_ratio_1m"), 0.0)
    cvd_ratio_5m = _safe_float(data.get("cvd_ratio_5m"), 0.0)

    buy_ratio_10s = _safe_float(data.get("buy_aggressor_ratio_10s"), 0.0)
    buy_ratio_30s = _safe_float(data.get("buy_aggressor_ratio_30s"), 0.0)
    buy_ratio_1m = _safe_float(data.get("buy_aggressor_ratio_1m"), 0.0)
    buy_ratio_3m = _safe_float(data.get("buy_aggressor_ratio_3m"), 0.0)
    buy_ratio_5m = _safe_float(data.get("buy_aggressor_ratio_5m"), 0.0)

    short_liq_1m = _safe_float(data.get("micro_short_liq_usdt_1m"), 0.0)
    short_liq_share_1m = _safe_float(data.get("micro_short_liq_share_1m"), 0.0)
    liq_imbalance_1m = _safe_float(data.get("micro_liq_imbalance_usdt_1m"), 0.0)

    oi_regime = str(derivatives.get("oi_regime") or data.get("oi_regime") or "").strip()
    oi_15m = _safe_float(derivatives.get("oi_change_pct_15m"), _safe_float(data.get("oi_change_pct_15m"), 0.0))
    oi_1h = _safe_float(derivatives.get("oi_change_pct_1h"), _safe_float(data.get("oi_change_pct_1h"), 0.0))

    oi_positive = (
        oi_regime in {"new_longs", "accumulation"}
        or oi_15m >= levels["oi_positive_15m"]
        or oi_1h >= levels["oi_positive_1h"]
    )
    has_trade_flow = trade_1m >= levels["min_trade_notional_1m"] or trade_5m >= levels["min_trade_notional_5m"]

    regime = "unknown"
    signal_level = "L0"
    confidence = 18.0 if has_trade_flow else 8.0
    reason_bits: List[str] = []

    if has_trade_flow:
        signal_level = "L1"

    if direction == "up":
        if (
            cvd_1m >= levels["breakout_cvd_1m"]
            and cvd_3m >= levels["breakout_cvd_3m"]
            and cvd_5m >= levels["breakout_cvd_5m"]
            and buy_ratio_1m >= levels["breakout_buy_ratio_1m"]
            and buy_ratio_3m >= levels["breakout_buy_ratio_3m"]
            and buy_ratio_5m >= levels["breakout_buy_ratio_5m"]
            and oi_positive
            and short_liq_share_1m <= levels["breakout_short_liq_share_1m_max"]
        ):
            regime = "breakout_continuation"
            signal_level = "L3"
            confidence = 84.0
            reason_bits = [
                f"CVD 1m {_fmt_signed_usd(cvd_1m)}",
                f"3m {_fmt_signed_usd(cvd_3m)}",
                f"5m {_fmt_signed_usd(cvd_5m)}",
                f"买方 {buy_ratio_1m * 100.0:.1f}%/{buy_ratio_3m * 100.0:.1f}%/{buy_ratio_5m * 100.0:.1f}%",
                f"OI {oi_regime or 'positive'}",
            ]
        elif (
            cvd_1m >= levels["new_longs_cvd_1m"]
            and cvd_5m >= levels["new_longs_cvd_5m"]
            and buy_ratio_1m >= levels["new_longs_buy_ratio_1m"]
            and buy_ratio_5m >= levels["new_longs_buy_ratio_5m"]
            and oi_positive
            and short_liq_share_1m < levels["short_cover_liq_share_1m"]
        ):
            regime = "new_longs"
            signal_level = "L3" if cvd_3m > (levels["new_longs_cvd_1m"] * 2.0) and buy_ratio_1m >= 0.58 else "L2"
            confidence = 78.0 if signal_level == "L3" else 68.0
            reason_bits = [
                f"CVD 1m {_fmt_signed_usd(cvd_1m)}",
                f"5m {_fmt_signed_usd(cvd_5m)}",
                f"买方 {buy_ratio_1m * 100.0:.1f}%/{buy_ratio_5m * 100.0:.1f}%",
                f"OI {oi_regime or 'positive'}",
            ]
        elif (
            trade_30s >= levels["absorption_trade_notional_30s"]
            and cvd_10s >= levels["absorption_cvd_10s"]
            and cvd_30s >= levels["absorption_cvd_30s"]
            and cvd_1m >= levels["absorption_cvd_1m"]
            and buy_ratio_10s >= levels["absorption_buy_ratio_10s"]
            and buy_ratio_30s >= levels["absorption_buy_ratio_30s"]
            and buy_ratio_1m >= levels["absorption_buy_ratio_1m"]
            and cvd_5m >= levels["absorption_cvd_5m_floor"]
            and oi_15m >= levels["absorption_oi_15m_min"]
            and (
                cvd_5m <= levels["new_longs_cvd_5m"]
                or (buy_ratio_30s - buy_ratio_5m) >= levels["absorption_buy_ratio_gap"]
            )
        ):
            regime = "absorption_reversal"
            signal_level = "L2" if cvd_30s >= (levels["absorption_cvd_30s"] * 1.5) else "L1"
            confidence = 70.0 if signal_level == "L2" else 60.0
            reason_bits = [
                f"CVD 10s {_fmt_signed_usd(cvd_10s)}",
                f"30s {_fmt_signed_usd(cvd_30s)}",
                f"1m {_fmt_signed_usd(cvd_1m)}",
                f"买方 {buy_ratio_10s * 100.0:.1f}%/{buy_ratio_30s * 100.0:.1f}%/{buy_ratio_1m * 100.0:.1f}%",
                f"5m {_fmt_signed_usd(cvd_5m)}",
            ]
        elif (
            short_liq_1m >= levels["short_cover_liq_1m"]
            and short_liq_share_1m >= levels["short_cover_liq_share_1m"]
            and (not oi_positive or oi_15m <= levels["short_cover_oi_15m_max"])
        ):
            regime = "short_cover"
            signal_level = "L2" if liq_imbalance_1m > (levels["short_cover_liq_1m"] * 1.6) else "L1"
            confidence = 64.0 if signal_level == "L2" else 54.0
            reason_bits = [
                f"短空强平 {_fmt_signed_usd(short_liq_1m)}",
                f"占比 {short_liq_share_1m * 100.0:.1f}%",
                f"CVD 1m {_fmt_signed_usd(cvd_1m)}",
                f"OI 15m {oi_15m * 100.0:+.2f}%",
            ]
        elif (
            has_trade_flow
            and abs(cvd_ratio_1m) <= levels["churn_abs_cvd_ratio_1m"]
            and abs(cvd_ratio_5m) <= levels["churn_abs_cvd_ratio_5m"]
            and abs(buy_ratio_1m - 0.5) <= levels["churn_buy_ratio_delta"]
            and abs(buy_ratio_5m - 0.5) <= levels["churn_buy_ratio_delta"]
        ):
            regime = "churn"
            signal_level = "L1"
            confidence = 46.0
            reason_bits = [
                f"CVD比 {cvd_ratio_1m:+.2f}/{cvd_ratio_5m:+.2f}",
                f"买方 {buy_ratio_1m * 100.0:.1f}%/{buy_ratio_5m * 100.0:.1f}%",
                f"OI 15m {oi_15m * 100.0:+.2f}%",
            ]
    elif has_trade_flow and abs(cvd_ratio_1m) <= levels["churn_abs_cvd_ratio_1m"]:
        regime = "churn"
        signal_level = "L1"
        confidence = 42.0
        reason_bits = [
            f"CVD比 {cvd_ratio_1m:+.2f}",
            f"买方 {buy_ratio_1m * 100.0:.1f}%",
        ]

    reason = f"{signal_level}:{regime}"
    if reason_bits:
        reason = f"{reason}; " + " | ".join(reason_bits)

    return {
        "micro_regime": regime,
        "micro_signal_level": signal_level,
        "micro_regime_confidence": round(confidence, 2),
        "micro_reason": reason,
        "micro_has_trade_flow": has_trade_flow,
        "micro_oi_positive": oi_positive,
    }


class MicrostructureProvider:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.enabled = _parse_bool(self.settings.get("enabled"), True)
        self.ws_url = str(self.settings.get("ws_url") or "wss://fstream.binance.com/ws").strip() or "wss://fstream.binance.com/ws"
        self.rest_base_url = str(self.settings.get("rest_base_url") or "https://fapi.binance.com").strip().rstrip("/")
        self.track_ttl_sec = max(60, _safe_int(self.settings.get("track_ttl_sec"), 900))
        self.subscription_refresh_sec = max(1, _safe_int(self.settings.get("subscription_refresh_sec"), 5))
        self.reconnect_sec = max(1, _safe_int(self.settings.get("reconnect_sec"), 3))
        self.max_active_symbols = max(1, _safe_int(self.settings.get("max_active_symbols"), 40))
        self.max_trades_per_symbol = max(500, _safe_int(self.settings.get("max_trades_per_symbol"), 5000))
        self.max_liquidations_per_symbol = max(100, _safe_int(self.settings.get("max_liquidations_per_symbol"), 1000))
        self.windows_sec = _normalize_windows(self.settings.get("windows_sec"))
        self.bootstrap_agg_trades_enabled = _parse_bool(self.settings.get("bootstrap_agg_trades_enabled"), True)
        self.bootstrap_lookback_sec = max(
            max(self.windows_sec),
            _safe_int(self.settings.get("bootstrap_lookback_sec"), max(self.windows_sec)),
        )
        self.bootstrap_min_interval_sec = max(5, _safe_int(self.settings.get("bootstrap_min_interval_sec"), 30))
        self.bootstrap_limit = max(100, min(1000, _safe_int(self.settings.get("bootstrap_limit"), 1000)))
        self.signal_thresholds = dict(DEFAULT_SIGNAL_THRESHOLDS)
        raw_thresholds = self.settings.get("signal_thresholds")
        if isinstance(raw_thresholds, dict):
            for key, value in raw_thresholds.items():
                if value is None:
                    continue
                self.signal_thresholds[key] = _safe_float(value, self.signal_thresholds.get(key, 0.0))

        self._proxy = (
            os.getenv("ALERT_HTTPS_PROXY")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("https_proxy")
            or os.getenv("ALERT_HTTP_PROXY")
            or os.getenv("HTTP_PROXY")
            or os.getenv("http_proxy")
        )
        self._task: Optional[asyncio.Task] = None
        self._request_id = 1
        self._last_sub_refresh_ts = 0.0
        self._tracked_symbols: Dict[str, float] = {}
        self._trade_buffers: Dict[str, Deque[Tuple[int, float, float, float]]] = {}
        self._liq_buffers: Dict[str, Deque[Tuple[int, float, float]]] = {}
        self._last_trade_ts: Dict[str, int] = {}
        self._last_liq_ts: Dict[str, int] = {}
        self._last_bootstrap_ts: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def fetch(
        self,
        symbol: str,
        base_asset: str,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> FactorSnapshot:
        snapshot = FactorSnapshot.empty(symbol=symbol, base_asset=base_asset)
        if not self.enabled:
            snapshot.source_health["microstructure"] = recent_health_entry("microstructure", True, "disabled")
            return snapshot
        if aiohttp is None:
            snapshot.source_health["microstructure"] = recent_health_entry("microstructure", False, "aiohttp_missing")
            return snapshot

        await self._touch(symbol)
        self._ensure_started()
        await self._bootstrap_agg_trades_if_needed(symbol)

        derivatives, liquidation = await self._build_symbol_snapshot(symbol, context=context or {})
        snapshot.derivatives.update(derivatives)
        snapshot.liquidation.update(liquidation)

        has_data = bool(derivatives.get("micro_last_trade_ts") or liquidation.get("micro_last_liq_ts"))
        message = "tracking" if has_data else "warming_up"
        snapshot.source_health["microstructure"] = recent_health_entry("microstructure", True, message)
        return snapshot

    def _ensure_started(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run_forever(), name="microstructure_ws")

    async def _touch(self, symbol: str) -> None:
        async with self._lock:
            self._tracked_symbols[str(symbol or "").upper()] = time.time()

    async def _build_symbol_snapshot(
        self,
        symbol: str,
        *,
        context: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        symbol_upper = str(symbol or "").upper()
        async with self._lock:
            self._prune_stale_locked(now_ms=now_ms)
            trades = list(self._trade_buffers.get(symbol_upper, deque()))
            liquidations = list(self._liq_buffers.get(symbol_upper, deque()))
            last_trade_ts = int(self._last_trade_ts.get(symbol_upper, 0))
            last_liq_ts = int(self._last_liq_ts.get(symbol_upper, 0))

        derivatives: Dict[str, Any] = {
            "micro_source": "binance_microstructure_ws",
            "micro_updated_at_ms": now_ms,
            "micro_last_trade_ts": last_trade_ts,
            "micro_last_liq_ts": last_liq_ts,
            "micro_data_age_sec": _compute_age_seconds(now_ms, last_trade_ts, last_liq_ts),
            "micro_symbol_active": symbol_upper in self._tracked_symbols,
        }
        liquidation_metrics: Dict[str, Any] = {
            "micro_last_liq_ts": last_liq_ts,
            "micro_updated_at_ms": now_ms,
        }

        if trades or liquidations:
            micro_metrics, liq_metrics = build_microstructure_metrics(
                trades,
                liquidations,
                as_of_ms=now_ms,
                windows_sec=self.windows_sec,
            )
            derivatives.update(micro_metrics)
            liquidation_metrics.update(liq_metrics)
            derivatives.update(
                classify_microstructure_regime(
                    derivatives,
                    context=context,
                    thresholds=self.signal_thresholds,
                )
            )
        else:
            derivatives.update(
                {
                    "micro_regime": "unknown",
                    "micro_signal_level": "L0",
                    "micro_regime_confidence": 0.0,
                    "micro_reason": "L0:unknown",
                    "micro_has_trade_flow": False,
                }
            )

        return derivatives, liquidation_metrics

    async def _bootstrap_agg_trades_if_needed(self, symbol: str) -> None:
        if not self.bootstrap_agg_trades_enabled or aiohttp is None:
            return

        now_ms = int(time.time() * 1000)
        symbol_upper = str(symbol or "").upper().strip()
        if not symbol_upper:
            return

        async with self._lock:
            last_bootstrap = float(self._last_bootstrap_ts.get(symbol_upper, 0.0))
            last_trade_ts = int(self._last_trade_ts.get(symbol_upper, 0))
            has_recent_data = last_trade_ts >= now_ms - min(60_000, int(self.bootstrap_lookback_sec) * 1000)
            if has_recent_data or (time.time() - last_bootstrap) < float(self.bootstrap_min_interval_sec):
                return
            self._last_bootstrap_ts[symbol_upper] = time.time()

        params = {
            "symbol": symbol_upper,
            "limit": str(self.bootstrap_limit),
        }
        url = f"{self.rest_base_url}/fapi/v1/aggTrades"

        try:
            timeout = aiohttp.ClientTimeout(total=8, connect=3, sock_read=5)
            async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                async with session.get(url, params=params, proxy=self._proxy) as response:
                    response.raise_for_status()
                    payload = await response.json()
        except Exception as exc:
            logger.debug("Microstructure aggTrade bootstrap failed: symbol=%s err=%s", symbol_upper, exc)
            return

        if not isinstance(payload, list):
            return

        rows: List[Tuple[int, float, float, float]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            row = _agg_trade_to_row(item)
            if row is not None:
                rows.append(row)
        if not rows:
            return

        async with self._lock:
            existing = list(self._trade_buffers.get(symbol_upper, deque()))
            combined: Dict[Tuple[int, float, float, float], Tuple[int, float, float, float]] = {}
            for row in existing:
                combined[_trade_row_key(row)] = row
            for row in rows:
                combined[_trade_row_key(row)] = row
            merged_rows = sorted(combined.values(), key=lambda row: int(row[0]))
            if len(merged_rows) > self.max_trades_per_symbol:
                merged_rows = merged_rows[-self.max_trades_per_symbol :]
            self._trade_buffers[symbol_upper] = deque(merged_rows, maxlen=self.max_trades_per_symbol)
            self._last_trade_ts[symbol_upper] = max(int(row[0]) for row in merged_rows)
            self._prune_symbol_locked(symbol_upper, now_ms=now_ms)

    async def run_forever(self) -> None:
        if not self.enabled or aiohttp is None:
            return

        logger.info(
            "Microstructure WS enabled: ttl=%ss refresh=%ss max_active=%s url=%s proxy=%s",
            self.track_ttl_sec,
            self.subscription_refresh_sec,
            self.max_active_symbols,
            self.ws_url,
            bool(self._proxy),
        )
        while True:
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Microstructure WS loop failed, reconnecting soon.")
            await asyncio.sleep(float(self.reconnect_sec))

    async def _run_connection(self) -> None:
        timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.ws_connect(self.ws_url, proxy=self._proxy, heartbeat=20) as ws:
                logger.info("Microstructure WS connected%s", f" via proxy {self._proxy}" if self._proxy else "")
                current_subs: set[str] = set()
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
                        continue

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                        current_subs = await self._sync_subscriptions(
                            send_payload=lambda payload: ws.send_str(json.dumps(payload)),
                            current_subs=current_subs,
                            force=False,
                        )
                        continue
                    if msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong()
                        continue
                    if msg.type == aiohttp.WSMsgType.PONG:
                        continue
                    if msg.type in {
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.ERROR,
                    }:
                        raise RuntimeError(f"microstructure ws closed: {msg.type}")

    async def _sync_subscriptions(
        self,
        send_payload,
        current_subs: set[str],
        force: bool,
    ) -> set[str]:
        now = time.time()
        if not force and (now - self._last_sub_refresh_ts) < float(self.subscription_refresh_sec):
            return current_subs

        async with self._lock:
            self._prune_stale_locked(now_ms=int(now * 1000))
            active_symbols = self._active_symbols_locked(now)

        target_subs: set[str] = set()
        for symbol in active_symbols:
            symbol_lower = symbol.lower()
            target_subs.add(f"{symbol_lower}@aggTrade")
            target_subs.add(f"{symbol_lower}@forceOrder")

        added = sorted(target_subs - current_subs)
        removed = sorted(current_subs - target_subs)

        for chunk in _chunked(added, 200):
            await send_payload({"method": "SUBSCRIBE", "params": chunk, "id": self._next_id()})
        for chunk in _chunked(removed, 200):
            await send_payload({"method": "UNSUBSCRIBE", "params": chunk, "id": self._next_id()})

        self._last_sub_refresh_ts = now
        if force or added or removed:
            logger.info(
                "Microstructure WS subscriptions synced: active=%s add=%s remove=%s",
                len(active_symbols),
                len(added),
                len(removed),
            )
        return target_subs

    async def _handle_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        if "stream" in payload and "data" in payload and isinstance(payload.get("data"), dict):
            payload = payload["data"]

        event_type = str(payload.get("e") or "").strip()
        if event_type == "aggTrade":
            await self._on_agg_trade(payload)
        elif event_type == "forceOrder":
            order = payload.get("o")
            await self._on_force_order(order if isinstance(order, dict) else payload)

    async def _on_agg_trade(self, payload: Dict[str, Any]) -> None:
        symbol = str(payload.get("s") or "").upper().strip()
        trade_ts = _safe_int(payload.get("T") or payload.get("E"), 0)
        price = _safe_float(payload.get("p"), 0.0)
        qty = _safe_float(payload.get("q"), 0.0)
        notional = price * qty
        if not symbol or trade_ts <= 0 or notional <= 0:
            return

        is_sell_aggressor = bool(payload.get("m"))
        buy_notional = 0.0 if is_sell_aggressor else notional
        sell_notional = notional if is_sell_aggressor else 0.0
        signed_notional = buy_notional - sell_notional

        async with self._lock:
            buffer = self._trade_buffers.setdefault(symbol, deque(maxlen=self.max_trades_per_symbol))
            buffer.append((trade_ts, signed_notional, buy_notional, sell_notional))
            self._last_trade_ts[symbol] = trade_ts
            self._prune_symbol_locked(symbol, now_ms=trade_ts)

    async def _on_force_order(self, payload: Dict[str, Any]) -> None:
        symbol = str(payload.get("s") or "").upper().strip()
        event_ts = _safe_int(payload.get("T") or payload.get("E") or payload.get("oT"), 0)
        price = _safe_float(payload.get("ap") or payload.get("p"), 0.0)
        qty = _safe_float(payload.get("z") or payload.get("l") or payload.get("q"), 0.0)
        side = str(payload.get("S") or payload.get("side") or "").upper().strip()
        notional = price * qty
        if not symbol or event_ts <= 0 or notional <= 0:
            return

        short_liq = notional if side == "BUY" else 0.0
        long_liq = notional if side == "SELL" else 0.0
        if short_liq <= 0 and long_liq <= 0:
            return

        async with self._lock:
            buffer = self._liq_buffers.setdefault(symbol, deque(maxlen=self.max_liquidations_per_symbol))
            buffer.append((event_ts, short_liq, long_liq))
            self._last_liq_ts[symbol] = event_ts
            self._prune_symbol_locked(symbol, now_ms=event_ts)

    def _active_symbols_locked(self, now: float) -> List[str]:
        active = [
            (symbol, last_seen)
            for symbol, last_seen in self._tracked_symbols.items()
            if (now - float(last_seen)) <= float(self.track_ttl_sec)
        ]
        active.sort(key=lambda item: item[1], reverse=True)
        return [symbol for symbol, _ in active[: self.max_active_symbols]]

    def _prune_stale_locked(self, *, now_ms: int) -> None:
        now_sec = now_ms / 1000.0
        stale_symbols = [
            symbol
            for symbol, last_seen in self._tracked_symbols.items()
            if (now_sec - float(last_seen)) > float(self.track_ttl_sec)
        ]
        for symbol in stale_symbols:
            self._tracked_symbols.pop(symbol, None)
            self._prune_symbol_locked(symbol, now_ms=now_ms, drop_all=True)

    def _prune_symbol_locked(self, symbol: str, *, now_ms: int, drop_all: bool = False) -> None:
        max_window_ms = max(self.windows_sec) * 1000
        cutoff_ms = now_ms - max_window_ms

        trade_buffer = self._trade_buffers.get(symbol)
        if trade_buffer is not None:
            while trade_buffer and (drop_all or int(trade_buffer[0][0]) < cutoff_ms):
                trade_buffer.popleft()
            if not trade_buffer:
                self._trade_buffers.pop(symbol, None)
                self._last_trade_ts.pop(symbol, None)

        liq_buffer = self._liq_buffers.get(symbol)
        if liq_buffer is not None:
            while liq_buffer and (drop_all or int(liq_buffer[0][0]) < cutoff_ms):
                liq_buffer.popleft()
            if not liq_buffer:
                self._liq_buffers.pop(symbol, None)
                self._last_liq_ts.pop(symbol, None)

    def _next_id(self) -> int:
        current = self._request_id
        self._request_id += 1
        return current


def _normalize_windows(raw: Any) -> List[int]:
    if not isinstance(raw, list):
        raw = [10, 30, 60, 180, 300]
    values: List[int] = []
    for item in raw:
        seconds = _safe_int(item, 0)
        if seconds > 0 and seconds not in values:
            values.append(seconds)
    if not values:
        values = [10, 30, 60, 180, 300]
    values.sort()
    return values


def _compute_age_seconds(now_ms: int, last_trade_ts: int, last_liq_ts: int) -> float:
    last_ts = max(int(last_trade_ts or 0), int(last_liq_ts or 0))
    if last_ts <= 0:
        return -1.0
    return max(0.0, (now_ms - last_ts) / 1000.0)


def _chunked(items: Sequence[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [list(items)]
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _agg_trade_to_row(payload: Dict[str, Any]) -> Optional[Tuple[int, float, float, float]]:
    trade_ts = _safe_int(payload.get("T") or payload.get("E"), 0)
    price = _safe_float(payload.get("p"), 0.0)
    qty = _safe_float(payload.get("q"), 0.0)
    notional = price * qty
    if trade_ts <= 0 or notional <= 0:
        return None

    is_sell_aggressor = bool(payload.get("m"))
    buy_notional = 0.0 if is_sell_aggressor else notional
    sell_notional = notional if is_sell_aggressor else 0.0
    signed_notional = buy_notional - sell_notional
    return trade_ts, signed_notional, buy_notional, sell_notional


def _trade_row_key(row: Tuple[int, float, float, float]) -> Tuple[int, float, float, float]:
    return (
        int(row[0]),
        round(float(row[1]), 8),
        round(float(row[2]), 8),
        round(float(row[3]), 8),
    )


def _fmt_signed_usd(value: Any) -> str:
    numeric = _safe_float(value, 0.0)
    if abs(numeric) >= 1_000_000:
        return f"{numeric / 1_000_000:+.2f}M"
    if abs(numeric) >= 1_000:
        return f"{numeric / 1_000:+.1f}K"
    return f"{numeric:+.0f}"


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
