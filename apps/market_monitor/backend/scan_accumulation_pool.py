from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None

try:
    from alert_config import load_alert_config
    from candidates.raw_event import base_asset_from_symbol, normalize_symbol
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_alert_config
    from apps.market_monitor.backend.candidates.raw_event import base_asset_from_symbol, normalize_symbol
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


logger = logging.getLogger(__name__)

EXCLUDED_BASE_ASSETS = {
    "USDC",
    "USDP",
    "TUSD",
    "FDUSD",
    "BUSD",
    "BTCDOM",
    "DEFI",
    "USDM",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the daily accumulation pool from Binance daily candles.")
    parser.add_argument("--config-path", type=str, default="")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols for a limited scan.")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--min-score", type=float, default=None)
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Print top results without writing runtime files.")
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


async def main_async() -> int:
    if aiohttp is None:
        print("aiohttp is required")
        return 2

    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level or "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    config = load_alert_config(config_path=args.config_path or None).raw
    factor_settings = (
        config.get("alert_strategy", {})
        .get("confirmation_factors", {})
        .get("accumulation_pool", {})
    )
    if not isinstance(factor_settings, dict):
        factor_settings = {}
    scan_settings = factor_settings.get("scan", {})
    if not isinstance(scan_settings, dict):
        scan_settings = {}
    if not _parse_bool(scan_settings.get("enabled"), True):
        logger.info("Accumulation pool scan is disabled.")
        return 0

    settings = _normalized_scan_settings(factor_settings, scan_settings)
    if args.max_symbols is not None:
        settings["max_symbols"] = max(0, int(args.max_symbols))
    if args.min_score is not None:
        settings["min_score"] = max(0.0, float(args.min_score))
    if args.concurrency is not None:
        settings["concurrency"] = max(1, int(args.concurrency))

    runtime_dir = resolve_runtime_dir(factor_settings)
    pool_path = _runtime_path(runtime_dir, str(factor_settings.get("pool_file") or "accumulation_pool.json"))
    history_path = _runtime_path(
        runtime_dir,
        str(factor_settings.get("history_file") or "accumulation_pool_history.jsonl"),
    )

    symbols = [normalize_symbol(item) for item in str(args.symbols or "").split(",") if item.strip()]
    proxy = _proxy_url()
    timeout = aiohttp.ClientTimeout(total=30, connect=8, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        if not symbols:
            symbols = await _fetch_symbols(session, settings["base_url"], proxy=proxy)
        if settings["max_symbols"] > 0:
            symbols = symbols[: settings["max_symbols"]]

        market_caps = {}
        if settings["market_cap_api_enabled"]:
            market_caps = await _fetch_market_caps(session, proxy=proxy)

        logger.info("Scanning accumulation pool: symbols=%s concurrency=%s", len(symbols), settings["concurrency"])
        results = await _scan_symbols(session, symbols, settings, market_caps, proxy=proxy)

    results.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "generated_at": generated_at,
        "source": "binance_futures_daily_klines",
        "version": 1,
        "settings": _public_settings(settings),
        "count": len(results),
        "symbols": {item["symbol"]: item for item in results},
        "top": [item["symbol"] for item in results[:50]],
    }

    _print_summary(results)
    if args.dry_run:
        logger.info("Dry run enabled; runtime files were not written.")
        return 0

    pool_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = pool_path.with_suffix(pool_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(pool_path)

    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_row = {
        "generated_at": generated_at,
        "count": len(results),
        "top": [
            {
                "symbol": item["symbol"],
                "status": item["status"],
                "score": item["score"],
                "sideways_days": item["sideways_days"],
                "recent_vol_ratio_7d": item["recent_vol_ratio_7d"],
            }
            for item in results[:30]
        ],
    }
    with history_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(history_row, ensure_ascii=False, separators=(",", ":")) + "\n")

    logger.info("Wrote accumulation pool: %s count=%s", pool_path, len(results))
    return 0


async def _scan_symbols(
    session: Any,
    symbols: Sequence[str],
    settings: Dict[str, Any],
    market_caps: Dict[str, float],
    *,
    proxy: Optional[str],
) -> List[Dict[str, Any]]:
    semaphore = asyncio.Semaphore(int(settings["concurrency"]))
    results: List[Dict[str, Any]] = []
    completed = 0

    async def _worker(symbol: str) -> None:
        nonlocal completed
        async with semaphore:
            await asyncio.sleep(float(settings["request_delay_sec"]))
            try:
                klines = await _get_json(
                    session,
                    settings["base_url"],
                    "/fapi/v1/klines",
                    {"symbol": symbol, "interval": "1d", "limit": int(settings["lookback_days"])},
                    proxy=proxy,
                )
                if isinstance(klines, list):
                    item = analyze_accumulation_symbol(symbol, klines, settings, market_caps)
                    if item:
                        results.append(item)
            except Exception as exc:
                logger.debug("Accumulation scan failed: symbol=%s err=%s", symbol, exc)
            completed += 1
            if completed % 50 == 0:
                logger.info("Accumulation scan progress: %s/%s found=%s", completed, len(symbols), len(results))

    await asyncio.gather(*[_worker(symbol) for symbol in symbols])
    return results


def analyze_accumulation_symbol(
    symbol: str,
    klines: Sequence[Any],
    settings: Dict[str, Any],
    market_caps: Dict[str, float],
) -> Optional[Dict[str, Any]]:
    rows = _parse_daily_klines(klines)
    if len(rows) < int(settings["min_data_days"]):
        return None

    symbol = normalize_symbol(symbol)
    base_asset = base_asset_from_symbol(symbol)
    if base_asset in EXCLUDED_BASE_ASSETS:
        return None

    recent_days = min(7, max(3, len(rows) // 8))
    recent = rows[-recent_days:]
    prior = rows[:-recent_days]
    if len(prior) < int(settings["min_sideways_days"]):
        return None

    recent_avg_close = _avg(item["close"] for item in recent)
    prior_avg_close = _avg(item["close"] for item in prior)
    if prior_avg_close > 0:
        pump_pct = (recent_avg_close - prior_avg_close) / prior_avg_close * 100.0
        if pump_pct > float(settings["max_recent_pump_pct"]):
            return None

    best: Optional[Dict[str, Any]] = None
    min_window = int(settings["min_sideways_days"])
    for window in range(min_window, len(prior) + 1):
        window_rows = prior[-window:]
        candidate = _score_sideways_window(symbol, base_asset, window_rows, recent, rows[-1], settings, market_caps)
        if not candidate:
            continue
        if best is None or float(candidate["score"]) > float(best["score"]):
            best = candidate

    if not best or float(best["score"]) < float(settings["min_score"]):
        return None
    return best


def _score_sideways_window(
    symbol: str,
    base_asset: str,
    window_rows: Sequence[Dict[str, float]],
    recent_rows: Sequence[Dict[str, float]],
    latest: Dict[str, float],
    settings: Dict[str, Any],
    market_caps: Dict[str, float],
) -> Optional[Dict[str, Any]]:
    lows = [item["low"] for item in window_rows]
    highs = [item["high"] for item in window_rows]
    closes = [item["close"] for item in window_rows if item["close"] > 0]
    quote_vols = [item["quote_vol"] for item in window_rows]
    if not lows or not highs or len(closes) < 2:
        return None

    range_low = _quantile(lows, 0.05)
    range_high = _quantile(highs, 0.95)
    if range_low <= 0 or range_high <= range_low:
        return None

    range_pct = (range_high - range_low) / range_low * 100.0
    max_range_pct = float(settings["max_range_pct"])
    if range_pct > max_range_pct:
        return None

    avg_quote_vol = _avg(quote_vols)
    if avg_quote_vol > float(settings["max_avg_quote_vol_usdt"]):
        return None

    slope_pct = _log_slope_pct(closes)
    max_slope_pct = float(settings["max_slope_pct"])
    if abs(slope_pct) > max_slope_pct:
        return None

    recent_avg_vol = _avg(item["quote_vol"] for item in recent_rows)
    recent_vol_ratio = recent_avg_vol / avg_quote_vol if avg_quote_vol > 0 else 0.0
    current_price = latest["close"]
    range_position = (current_price - range_low) / (range_high - range_low) if range_high > range_low else 0.0
    range_position = max(-1.0, min(2.0, range_position))

    market_cap, market_cap_source = _market_cap_for(base_asset, market_caps, current_price, avg_quote_vol)
    scores = _component_scores(
        sideways_days=len(window_rows),
        range_pct=range_pct,
        avg_quote_vol=avg_quote_vol,
        slope_pct=slope_pct,
        recent_vol_ratio=recent_vol_ratio,
        market_cap=market_cap,
        market_cap_source=market_cap_source,
        settings=settings,
    )
    total_score = round(sum(scores.values()), 2)
    status = _pool_status(
        recent_vol_ratio=recent_vol_ratio,
        range_position=range_position,
        current_price=current_price,
        range_low=range_low,
        range_high=range_high,
        settings=settings,
    )
    if status == "invalid":
        return None

    return {
        "symbol": symbol,
        "base_asset": base_asset,
        "status": status,
        "score": total_score,
        "component_scores": {key: round(value, 2) for key, value in scores.items()},
        "sideways_days": len(window_rows),
        "range_pct": round(range_pct, 4),
        "slope_pct": round(slope_pct, 4),
        "range_low": range_low,
        "range_high": range_high,
        "range_position": round(range_position, 4),
        "current_price": current_price,
        "avg_quote_vol_usdt": round(avg_quote_vol, 2),
        "recent_avg_quote_vol_usdt_7d": round(recent_avg_vol, 2),
        "recent_vol_ratio_7d": round(recent_vol_ratio, 4),
        "market_cap": round(market_cap, 2) if market_cap > 0 else 0.0,
        "market_cap_source": market_cap_source,
        "data_quality": "ok" if market_cap_source != "estimated" and avg_quote_vol >= float(settings["min_avg_quote_vol_usdt"]) else "estimated",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _component_scores(
    *,
    sideways_days: int,
    range_pct: float,
    avg_quote_vol: float,
    slope_pct: float,
    recent_vol_ratio: float,
    market_cap: float,
    market_cap_source: str,
    settings: Dict[str, Any],
) -> Dict[str, float]:
    max_range_pct = float(settings["max_range_pct"])
    max_slope_pct = float(settings["max_slope_pct"])
    min_avg_vol = float(settings["min_avg_quote_vol_usdt"])
    max_avg_vol = float(settings["max_avg_quote_vol_usdt"])

    days_score = min(sideways_days / 120.0, 1.0) * 20.0
    range_score = max(0.0, 1.0 - range_pct / max_range_pct) * 20.0
    slope_score = max(0.0, 1.0 - abs(slope_pct) / max_slope_pct) * 15.0
    if avg_quote_vol < min_avg_vol:
        volume_score = max(0.0, avg_quote_vol / max(min_avg_vol, 1.0)) * 8.0
    else:
        volume_score = max(0.0, 1.0 - (avg_quote_vol - min_avg_vol) / max(max_avg_vol - min_avg_vol, 1.0)) * 15.0
    impulse_score = min(recent_vol_ratio / max(float(settings["ready_vol_ratio"]), 1.0), 1.0) * 15.0
    mcap_score = _market_cap_score(market_cap, market_cap_source)
    return {
        "sideways_days": days_score,
        "range_compression": range_score,
        "flatness": slope_score,
        "quiet_volume": volume_score,
        "market_cap": mcap_score,
        "volume_impulse": impulse_score,
    }


def _market_cap_score(market_cap: float, source: str) -> float:
    if market_cap <= 0:
        return 0.0
    if market_cap < 50_000_000:
        score = 15.0
    elif market_cap < 100_000_000:
        score = 12.0
    elif market_cap < 200_000_000:
        score = 10.0
    elif market_cap < 500_000_000:
        score = 6.0
    elif market_cap < 1_000_000_000:
        score = 3.0
    else:
        score = 0.0
    if source == "estimated":
        score *= 0.55
    return score


def _pool_status(
    *,
    recent_vol_ratio: float,
    range_position: float,
    current_price: float,
    range_low: float,
    range_high: float,
    settings: Dict[str, Any],
) -> str:
    if current_price < range_low * 0.88:
        return "invalid"
    if current_price > range_high * 2.0:
        return "invalid"
    if recent_vol_ratio >= float(settings["ready_vol_ratio"]) and range_position >= 0.55:
        return "ready"
    if recent_vol_ratio >= float(settings["warming_vol_ratio"]):
        return "warming"
    return "dormant"


async def _fetch_symbols(session: Any, base_url: str, *, proxy: Optional[str]) -> List[str]:
    payload = await _get_json(session, base_url, "/fapi/v1/exchangeInfo", {}, proxy=proxy)
    symbols: List[str] = []
    if not isinstance(payload, dict):
        return symbols
    for item in payload.get("symbols") or []:
        if not isinstance(item, dict):
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("status") != "TRADING":
            continue
        symbol = normalize_symbol(str(item.get("symbol") or ""))
        if symbol:
            symbols.append(symbol)
    return symbols


async def _fetch_market_caps(session: Any, *, proxy: Optional[str]) -> Dict[str, float]:
    try:
        try:
            async with session.get(
                "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list",
                proxy=proxy,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except Exception:
            if not proxy:
                raise
            async with session.get(
                "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list",
                proxy=None,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
    except Exception as exc:
        logger.info("Market cap API unavailable: %s", exc)
        return {}

    result: Dict[str, float] = {}
    if not isinstance(payload, dict):
        return result
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").upper().strip()
        cap = _safe_float(item.get("marketCap"), 0.0)
        if name and cap > 0:
            result[name] = cap
    return result


async def _get_json(
    session: Any,
    base_url: str,
    path: str,
    params: Dict[str, Any],
    *,
    proxy: Optional[str],
) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    for attempt in range(3):
        try:
            async with session.get(url, params=params, proxy=proxy) as response:
                if response.status == 429:
                    await asyncio.sleep(2.0 + attempt)
                    continue
                response.raise_for_status()
                return await response.json()
        except Exception:
            if not proxy:
                raise
            async with session.get(url, params=params, proxy=None) as response:
                if response.status == 429:
                    await asyncio.sleep(2.0 + attempt)
                    continue
                response.raise_for_status()
                return await response.json()
    raise RuntimeError(f"request failed after retries: {path}")


def _parse_daily_klines(klines: Sequence[Any]) -> List[Dict[str, float]]:
    now_ms = int(time.time() * 1000)
    rows: List[Dict[str, float]] = []
    for item in klines:
        if not isinstance(item, list) or len(item) < 8:
            continue
        close_time = _safe_int(item[6], 0)
        if close_time > now_ms:
            continue
        row = {
            "open_time": _safe_float(item[0], 0.0),
            "open": _safe_float(item[1], 0.0),
            "high": _safe_float(item[2], 0.0),
            "low": _safe_float(item[3], 0.0),
            "close": _safe_float(item[4], 0.0),
            "quote_vol": _safe_float(item[7], 0.0),
            "close_time": float(close_time),
        }
        if row["high"] > 0 and row["low"] > 0 and row["close"] > 0:
            rows.append(row)
    rows.sort(key=lambda item: item["open_time"])
    return rows


def _market_cap_for(base_asset: str, market_caps: Dict[str, float], price: float, avg_quote_vol: float) -> tuple[float, str]:
    candidates = [base_asset]
    for prefix in ("1000000", "1000"):
        if base_asset.startswith(prefix) and len(base_asset) > len(prefix):
            candidates.append(base_asset[len(prefix) :])
    for candidate in candidates:
        cap = _safe_float(market_caps.get(candidate), 0.0)
        if cap > 0:
            return cap, "binance"
    estimated = max(0.0, avg_quote_vol * 12.0)
    return estimated, "estimated" if estimated > 0 else "unknown"


def _log_slope_pct(closes: Sequence[float]) -> float:
    values = [math.log(value) for value in closes if value > 0]
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    denominator = sum((idx - x_mean) ** 2 for idx in range(n))
    if denominator <= 0:
        return 0.0
    slope = sum((idx - x_mean) * (value - y_mean) for idx, value in enumerate(values)) / denominator
    return (math.exp(slope * (n - 1)) - 1.0) * 100.0


def _quantile(values: Sequence[float], q: float) -> float:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * min(1.0, max(0.0, q))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _avg(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    return sum(items) / len(items) if items else 0.0


def _normalized_scan_settings(factor_settings: Dict[str, Any], scan_settings: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "base_url": str(scan_settings.get("base_url") or "https://fapi.binance.com").rstrip("/"),
        "lookback_days": max(80, _safe_int(scan_settings.get("lookback_days"), 210)),
        "min_data_days": max(50, _safe_int(scan_settings.get("min_data_days"), 60)),
        "min_sideways_days": max(30, _safe_int(scan_settings.get("min_sideways_days"), 45)),
        "max_range_pct": max(10.0, _safe_float(scan_settings.get("max_range_pct"), 80.0)),
        "max_slope_pct": max(5.0, _safe_float(scan_settings.get("max_slope_pct"), 20.0)),
        "min_avg_quote_vol_usdt": max(0.0, _safe_float(scan_settings.get("min_avg_quote_vol_usdt"), 500000.0)),
        "max_avg_quote_vol_usdt": max(1.0, _safe_float(scan_settings.get("max_avg_quote_vol_usdt"), 20000000.0)),
        "warming_vol_ratio": max(1.0, _safe_float(scan_settings.get("warming_vol_ratio"), 1.5)),
        "ready_vol_ratio": max(1.0, _safe_float(scan_settings.get("ready_vol_ratio"), 2.5)),
        "max_recent_pump_pct": max(50.0, _safe_float(scan_settings.get("max_recent_pump_pct"), 250.0)),
        "min_score": max(0.0, _safe_float(scan_settings.get("min_score"), 60.0)),
        "max_symbols": max(0, _safe_int(scan_settings.get("max_symbols"), 0)),
        "concurrency": max(1, _safe_int(scan_settings.get("concurrency"), 2)),
        "request_delay_sec": max(0.0, _safe_float(scan_settings.get("request_delay_sec"), 0.15)),
        "market_cap_api_enabled": _parse_bool(scan_settings.get("market_cap_api_enabled"), True),
        "pool_file": str(factor_settings.get("pool_file") or "accumulation_pool.json"),
    }


def _public_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in settings.items() if key not in {"base_url"}}


def _runtime_path(runtime_dir: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else runtime_dir / path


def _print_summary(results: Sequence[Dict[str, Any]]) -> None:
    print("========== ACCUMULATION POOL ==========")
    print(f"count={len(results)}")
    for item in results[:20]:
        print(
            f"{item['symbol']:>18} {item['status']:<7} score={item['score']:>5.1f} "
            f"days={item['sideways_days']:>3} range={item['range_pct']:>5.1f}% "
            f"slope={item['slope_pct']:>+5.1f}% vol={item['recent_vol_ratio_7d']:>4.1f}x "
            f"mcap={_fmt_usd(item.get('market_cap'))}"
        )
    print("=======================================")


def _fmt_usd(value: Any) -> str:
    numeric = _safe_float(value, 0.0)
    if numeric >= 1_000_000_000:
        return f"${numeric / 1_000_000_000:.2f}B"
    if numeric >= 1_000_000:
        return f"${numeric / 1_000_000:.1f}M"
    if numeric >= 1_000:
        return f"${numeric / 1_000:.0f}K"
    return f"${numeric:.0f}"


def _proxy_url() -> Optional[str]:
    return (
        os.getenv("ALERT_HTTPS_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("https_proxy")
        or os.getenv("ALERT_HTTP_PROXY")
        or os.getenv("HTTP_PROXY")
        or os.getenv("http_proxy")
    )


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


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
