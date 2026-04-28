from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

try:
    from alert_config import load_alert_config
    from candidates.candidate_models import Candidate
    from factors.microstructure import build_microstructure_metrics, classify_microstructure_regime
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_alert_config
    from apps.market_monitor.backend.candidates.candidate_models import Candidate
    from apps.market_monitor.backend.factors.microstructure import build_microstructure_metrics, classify_microstructure_regime

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ["SIRENUSDT", "RAVEUSDT", "币安人生USDT", "GIGGLEUSDT", "龙虾USDT"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay microstructure regime around recent candidate events.")
    parser.add_argument(
        "--symbols",
        type=str,
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbols. Default replays SIREN/RAVE/币安人生/GIGGLE/龙虾.",
    )
    parser.add_argument(
        "--candidate-file",
        type=str,
        default="",
        help="Optional candidates.json path. Defaults to runtime candidates file from config.",
    )
    parser.add_argument("--max-events", type=int, default=6, help="Replay up to N recent events per symbol.")
    parser.add_argument("--lookback-minutes", type=int, default=5, help="Trade/liquidation lookback minutes per event.")
    parser.add_argument("--oi-lookback-hours", type=int, default=6, help="OI history lookback hours per event.")
    parser.add_argument("--config-path", type=str, default="")
    parser.add_argument("--export-json", type=str, default="", help="Optional export JSON path.")
    return parser.parse_args()


async def _fetch_json(
    session: aiohttp.ClientSession,
    base_url: str,
    path: str,
    params: Dict[str, Any],
    *,
    proxy: Optional[str] = None,
) -> Any:
    try:
        async with session.get(f"{base_url}{path}", params=params, proxy=proxy) as response:
            response.raise_for_status()
            return await response.json()
    except Exception:
        if proxy:
            async with session.get(f"{base_url}{path}", params=params, proxy=None) as response:
                response.raise_for_status()
                return await response.json()
        raise


async def _fetch_agg_trades(
    session: aiohttp.ClientSession,
    base_url: str,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
    proxy: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    cursor = int(start_ms)
    while cursor < end_ms:
        payload = await _fetch_json(
            session,
            base_url,
            "/fapi/v1/aggTrades",
            {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": limit},
            proxy=proxy,
        )
        if not isinstance(payload, list) or not payload:
            break
        appended = 0
        last_ts = cursor
        last_id = None
        for item in payload:
            if not isinstance(item, dict):
                continue
            trade_ts = _safe_int(item.get("T"), 0)
            if trade_ts <= 0:
                continue
            trade_id = item.get("a")
            if rows and trade_ts == last_ts and trade_id == last_id:
                continue
            rows.append(item)
            appended += 1
            last_ts = trade_ts
            last_id = trade_id
        if appended <= 0:
            break
        cursor = max(last_ts + 1, cursor + 1)
        if len(payload) < limit:
            break
    return rows


async def _fetch_force_orders(
    session: aiohttp.ClientSession,
    base_url: str,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    limit: int = 100,
    proxy: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    cursor = int(start_ms)
    while cursor < end_ms:
        try:
            payload = await _fetch_json(
                session,
                base_url,
                "/fapi/v1/allForceOrders",
                {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": limit},
                proxy=proxy,
            )
        except aiohttp.ClientResponseError as exc:
            if int(exc.status) == 400:
                logger.debug("forceOrder replay unavailable for %s: %s", symbol, exc)
                return rows
            raise
        if not isinstance(payload, list) or not payload:
            break
        appended = 0
        last_ts = cursor
        for item in payload:
            if not isinstance(item, dict):
                continue
            event_ts = _safe_int(item.get("time") or item.get("updatedTime"), 0)
            if event_ts <= 0:
                continue
            rows.append(item)
            appended += 1
            last_ts = max(last_ts, event_ts)
        if appended <= 0:
            break
        cursor = max(last_ts + 1, cursor + 1)
        if len(payload) < limit:
            break
    return rows


async def _fetch_oi_context(
    session: aiohttp.ClientSession,
    base_url: str,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    proxy: Optional[str] = None,
) -> Dict[str, Any]:
    payload = await _fetch_json(
        session,
        base_url,
        "/futures/data/openInterestHist",
        {
            "symbol": symbol,
            "period": "5m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 200,
        },
        proxy=proxy,
    )
    rows = [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
    if not rows:
        return {}

    normalized: List[tuple[int, float]] = []
    for item in rows:
        ts = _safe_int(item.get("timestamp"), 0)
        price = _safe_float(item.get("close") or item.get("sumOpenInterestValue"), 0.0)
        oi_value = _safe_float(item.get("sumOpenInterestValue"), 0.0)
        oi_amount = _safe_float(item.get("sumOpenInterest"), 0.0)
        if ts <= 0:
            continue
        if oi_value > 0:
            normalized.append((ts, oi_value))
        elif oi_amount > 0 and price > 0:
            normalized.append((ts, oi_amount * price))
    normalized.sort(key=lambda item: item[0])
    if not normalized:
        return {}

    latest_ts, latest_oi = normalized[-1]

    def _pct_change(steps_back: int) -> float:
        if len(normalized) <= steps_back:
            return 0.0
        prev = float(normalized[-(steps_back + 1)][1])
        if prev <= 0:
            return 0.0
        return (latest_oi - prev) / prev

    oi_15m = _pct_change(3)
    oi_1h = _pct_change(12)
    oi_regime = "neutral"
    if oi_15m >= 0.05 or oi_1h >= 0.10:
        oi_regime = "new_longs"
    elif oi_15m <= 0.0 and oi_1h <= 0.03:
        oi_regime = "short_cover"

    return {
        "oi_usdt": latest_oi,
        "oi_change_pct_15m": oi_15m,
        "oi_change_pct_1h": oi_1h,
        "oi_regime": oi_regime,
        "oi_hist_latest_ts": latest_ts,
    }


def _candidate_path_from_config(config: Dict[str, Any]) -> Path:
    runtime_dir_value = str(config.get("alert_strategy", {}).get("runtime_dir") or "").strip()
    runtime_dir = Path(runtime_dir_value).expanduser() if runtime_dir_value else Path("E:/cursor/crypto-monitor/data/runtime/market_monitor")
    candidate_file = str(config.get("alert_strategy", {}).get("candidate_file") or "candidates.json")
    return runtime_dir / candidate_file


def _load_candidates(candidate_path: Path) -> Dict[str, Candidate]:
    payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    rows = payload.get("candidates", payload) if isinstance(payload, dict) else {}
    candidates: Dict[str, Candidate] = {}
    if isinstance(rows, dict):
        for key, value in rows.items():
            if isinstance(value, dict):
                candidate = Candidate.from_dict(value)
                candidates[str(key)] = candidate
    return candidates


def _to_trade_rows(items: List[Dict[str, Any]]) -> List[tuple[int, float, float, float]]:
    rows: List[tuple[int, float, float, float]] = []
    for item in items:
        ts = _safe_int(item.get("T"), 0)
        price = _safe_float(item.get("p"), 0.0)
        qty = _safe_float(item.get("q"), 0.0)
        notional = price * qty
        if ts <= 0 or notional <= 0:
            continue
        is_sell_aggressor = bool(item.get("m"))
        buy_notional = 0.0 if is_sell_aggressor else notional
        sell_notional = notional if is_sell_aggressor else 0.0
        rows.append((ts, buy_notional - sell_notional, buy_notional, sell_notional))
    return rows


def _to_liq_rows(items: List[Dict[str, Any]]) -> List[tuple[int, float, float]]:
    rows: List[tuple[int, float, float]] = []
    for item in items:
        ts = _safe_int(item.get("time") or item.get("updatedTime"), 0)
        price = _safe_float(item.get("averagePrice") or item.get("price"), 0.0)
        qty = _safe_float(item.get("executedQty") or item.get("origQty"), 0.0)
        side = str(item.get("side") or "").upper()
        notional = price * qty
        if ts <= 0 or notional <= 0:
            continue
        short_liq = notional if side == "BUY" else 0.0
        long_liq = notional if side == "SELL" else 0.0
        rows.append((ts, short_liq, long_liq))
    return rows


async def _replay_symbol(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    symbol: str,
    candidate: Candidate,
    max_events: int,
    lookback_minutes: int,
    oi_lookback_hours: int,
    proxy: Optional[str],
) -> Dict[str, Any]:
    events = sorted(candidate.recent_events or [], key=lambda item: str(item.get("event_time") or ""))[-max_events:]
    results: List[Dict[str, Any]] = []
    for event in events:
        event_time = _parse_time_ms(event.get("event_time"))
        if event_time <= 0:
            continue
        start_ms = event_time - max(1, lookback_minutes) * 60_000
        oi_start_ms = event_time - max(1, oi_lookback_hours) * 3_600_000
        agg_items, liq_items, oi_context = await asyncio.gather(
            _fetch_agg_trades(session, base_url, symbol, start_ms=start_ms, end_ms=event_time, proxy=proxy),
            _fetch_force_orders(session, base_url, symbol, start_ms=start_ms, end_ms=event_time, proxy=proxy),
            _fetch_oi_context(session, base_url, symbol, start_ms=oi_start_ms, end_ms=event_time, proxy=proxy),
        )

        trade_rows = _to_trade_rows(agg_items)
        liq_rows = _to_liq_rows(liq_items)
        micro_metrics, liq_metrics = build_microstructure_metrics(
            trade_rows,
            liq_rows,
            as_of_ms=event_time,
            windows_sec=[10, 30, 60, 180, 300],
        )
        derivatives = dict(oi_context)
        derivatives.update(micro_metrics)
        derivatives.update(liq_metrics)
        classification = classify_microstructure_regime(
            derivatives,
            context={
                "direction": str(event.get("direction") or ""),
                "latest": {
                    "direction": str(event.get("direction") or ""),
                    "change_pct": _safe_float(event.get("change_pct"), 0.0),
                },
                "derivatives": oi_context,
            },
        )
        derivatives.update(classification)
        results.append(
            {
                "event_time": event.get("event_time"),
                "window": event.get("window"),
                "direction": event.get("direction"),
                "change_pct": _safe_float(event.get("change_pct"), 0.0),
                "micro_signal_level": derivatives.get("micro_signal_level"),
                "micro_regime": derivatives.get("micro_regime"),
                "micro_reason": derivatives.get("micro_reason"),
                "cvd_usdt_10s": derivatives.get("cvd_usdt_10s"),
                "cvd_usdt_30s": derivatives.get("cvd_usdt_30s"),
                "cvd_usdt_1m": derivatives.get("cvd_usdt_1m"),
                "cvd_usdt_3m": derivatives.get("cvd_usdt_3m"),
                "cvd_usdt_5m": derivatives.get("cvd_usdt_5m"),
                "buy_aggressor_ratio_10s": derivatives.get("buy_aggressor_ratio_10s"),
                "buy_aggressor_ratio_30s": derivatives.get("buy_aggressor_ratio_30s"),
                "buy_aggressor_ratio_1m": derivatives.get("buy_aggressor_ratio_1m"),
                "buy_aggressor_ratio_3m": derivatives.get("buy_aggressor_ratio_3m"),
                "buy_aggressor_ratio_5m": derivatives.get("buy_aggressor_ratio_5m"),
                "micro_liq_imbalance_usdt_1m": derivatives.get("micro_liq_imbalance_usdt_1m"),
                "oi_change_pct_15m": derivatives.get("oi_change_pct_15m"),
                "oi_change_pct_1h": derivatives.get("oi_change_pct_1h"),
            }
        )

    regime_counts: Dict[str, int] = {}
    for item in results:
        regime = str(item.get("micro_regime") or "unknown")
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
    return {"symbol": symbol, "events": results, "regime_counts": regime_counts}


async def _run() -> int:
    args = parse_args()
    config = load_alert_config(config_path=args.config_path or None).raw
    candidate_path = Path(args.candidate_file).expanduser() if args.candidate_file else _candidate_path_from_config(config)
    if not candidate_path.exists():
        print(f"candidate file not found: {candidate_path}")
        return 2

    candidates = _load_candidates(candidate_path)
    base_url = str(
        config.get("alert_strategy", {})
        .get("confirmation_factors", {})
        .get("binance", {})
        .get("base_url", "https://fapi.binance.com")
    ).rstrip("/")

    symbols = [item.strip() for item in str(args.symbols or "").split(",") if item.strip()]
    if not symbols:
        symbols = list(DEFAULT_SYMBOLS)

    timeout = aiohttp.ClientTimeout(total=60)
    proxy = (
        os.getenv("ALERT_HTTPS_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("https_proxy")
        or os.getenv("ALERT_HTTP_PROXY")
        or os.getenv("HTTP_PROXY")
        or os.getenv("http_proxy")
    )
    results: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
        for symbol in symbols:
            candidate = candidates.get(symbol)
            if candidate is None:
                print(f"[skip] {symbol}: not found in candidates file")
                continue
            try:
                symbol_result = await _replay_symbol(
                    session,
                    base_url=base_url,
                    symbol=symbol,
                    candidate=candidate,
                    max_events=max(1, int(args.max_events)),
                    lookback_minutes=max(1, int(args.lookback_minutes)),
                    oi_lookback_hours=max(1, int(args.oi_lookback_hours)),
                    proxy=proxy,
                )
                results.append(symbol_result)
            except Exception as exc:
                print(f"[error] {symbol}: {exc}")

    print("========== MICROSTRUCTURE REPLAY ==========")
    for symbol_result in results:
        print(f"\n{symbol_result['symbol']}")
        print(f"regime_counts: {symbol_result['regime_counts']}")
        for item in symbol_result["events"]:
            print(
                f"- {item['event_time']} | {item['window']} | {item['direction']} | "
                f"chg={float(item['change_pct']):+.2f}% | {item['micro_signal_level']} {item['micro_regime']} | "
                f"CVD10s={_fmt_signed(item['cvd_usdt_10s'])} | CVD30s={_fmt_signed(item['cvd_usdt_30s'])} | "
                f"CVD1m={_fmt_signed(item['cvd_usdt_1m'])} | CVD3m={_fmt_signed(item['cvd_usdt_3m'])} | "
                f"CVD5m={_fmt_signed(item['cvd_usdt_5m'])} | "
                f"buy10s={_fmt_ratio(item['buy_aggressor_ratio_10s'])} | buy30s={_fmt_ratio(item['buy_aggressor_ratio_30s'])} | "
                f"buy1m={_fmt_ratio(item['buy_aggressor_ratio_1m'])} | buy3m={_fmt_ratio(item['buy_aggressor_ratio_3m'])} | "
                f"buy5m={_fmt_ratio(item['buy_aggressor_ratio_5m'])} | "
                f"liqΔ1m={_fmt_signed(item['micro_liq_imbalance_usdt_1m'])} | "
                f"OI15m={_fmt_pct(item['oi_change_pct_15m'])} | OI1h={_fmt_pct(item['oi_change_pct_1h'])}"
            )
            print(f"  {item['micro_reason']}")
    print("\n===========================================")

    if args.export_json:
        export_path = Path(str(args.export_json)).expanduser()
        export_path.parent.mkdir(parents=True, exist_ok=True)
        export_path.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "candidate_file": str(candidate_path),
                    "symbols": symbols,
                    "results": results,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\nexported: {export_path}")
    return 0


def _parse_time_ms(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


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


def _fmt_ratio(value: Any) -> str:
    numeric = _safe_float(value, 0.0)
    return f"{numeric * 100.0:.1f}%"


def _fmt_signed(value: Any) -> str:
    numeric = _safe_float(value, 0.0)
    if abs(numeric) >= 1_000_000:
        return f"{numeric / 1_000_000:+.2f}M"
    if abs(numeric) >= 1_000:
        return f"{numeric / 1_000:+.1f}K"
    return f"{numeric:+.0f}"


def _fmt_pct(value: Any) -> str:
    numeric = _safe_float(value, 0.0)
    return f"{numeric * 100.0:+.2f}%"


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
