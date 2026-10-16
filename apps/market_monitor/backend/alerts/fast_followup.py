from __future__ import annotations

import os
import socket
from typing import Any, Dict, List, Optional

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None

Bar = List[float]  # [open_time_ms, open, high, low, close]


class FastFollowUpClient:
    """Fetches the 1m candles printed since a fast alert went out."""

    def __init__(self, *, base_url: str = "https://fapi.binance.com", timeout_sec: float = 5.0) -> None:
        self.base_url = str(base_url or "https://fapi.binance.com").rstrip("/")
        self.timeout_sec = max(1.0, float(timeout_sec or 5.0))
        self._proxy = (
            os.getenv("ALERT_HTTPS_PROXY")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("https_proxy")
            or os.getenv("ALERT_HTTP_PROXY")
            or os.getenv("HTTP_PROXY")
            or os.getenv("http_proxy")
        )

    async def fetch_1m_bars(self, symbol: str, *, start_ms: int, limit: int) -> List[Bar]:
        if aiohttp is None:
            raise RuntimeError("aiohttp is not installed")
        params = {"symbol": symbol, "interval": "1m", "startTime": int(start_ms), "limit": max(1, int(limit))}
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
            connector=connector,
            trust_env=True,
        ) as session:
            async with session.get(f"{self.base_url}/fapi/v1/klines", params=params, proxy=self._proxy) as response:
                response.raise_for_status()
                payload = await response.json()
        bars: List[Bar] = []
        for row in payload if isinstance(payload, list) else []:
            try:
                bars.append([int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])])
            except (TypeError, ValueError, IndexError):
                continue
        return bars


def summarize_followup(
    bars: List[Bar],
    *,
    entry_price: float,
    direction: str,
    hold_pct: float = 1.0,
) -> Optional[Dict[str, Any]]:
    if entry_price <= 0 or not bars:
        return None
    last_price = bars[-1][4]
    high = max(bar[2] for bar in bars)
    low = min(bar[3] for bar in bars)
    sign = -1.0 if direction == "down" else 1.0
    change_pct = (last_price - entry_price) / entry_price * 100.0
    high_pct = (high - entry_price) / entry_price * 100.0
    low_pct = (low - entry_price) / entry_price * 100.0
    directional_pct = sign * change_pct
    favorable_pct = -low_pct if direction == "down" else high_pct
    if directional_pct <= -hold_pct:
        status = "reversed"
    elif favorable_pct >= 2 * hold_pct and directional_pct <= favorable_pct / 2:
        # Ran further after the alert but has already given back more than half of it.
        status = "faded"
    elif directional_pct >= hold_pct:
        status = "continued"
    else:
        status = "flat"
    return {
        "entry_price": entry_price,
        "last_price": last_price,
        "change_pct": change_pct,
        "high": high,
        "high_pct": high_pct,
        "low": low,
        "low_pct": low_pct,
        "favorable_pct": favorable_pct,
        "status": status,
    }


def format_fast_followup(*, token: str, symbol: str, direction: str, minutes: int, summary: Dict[str, Any]) -> str:
    move = "上涨" if direction == "up" else "下跌" if direction == "down" else "异动"
    status_text = {
        "continued": f"✅ 状态：{move}延续",
        "reversed": "↩️ 状态：已回落" if direction == "up" else "↩️ 状态：已反弹" if direction == "down" else "↩️ 状态：已回撤",
        "faded": (
            f"⚠️ 状态：{'冲高回落' if direction == 'up' else '探底回升'}"
            f"（期间最多{'涨' if direction == 'up' else '跌'} {abs(summary.get('favorable_pct') or 0.0):.2f}%，已回吐过半）"
        ),
        "flat": "➖ 状态：横盘震荡",
    }.get(str(summary.get("status")), "➖ 状态：横盘震荡")
    return "\n".join(
        [
            f"**⏱ 急速异动跟进 | {token}（{minutes}分钟后）**",
            (
                f"💵 推送价 {_fmt_price(summary['entry_price'])} → 现价 {_fmt_price(summary['last_price'])}"
                f"（{summary['change_pct']:+.2f}%）"
            ),
            (
                f"↕️ 期间最高 {_fmt_price(summary['high'])}（{summary['high_pct']:+.2f}%）"
                f" | 最低 {_fmt_price(summary['low'])}（{summary['low_pct']:+.2f}%）"
            ),
            status_text,
            f"🔗 https://www.binance.com/futures/{symbol}",
        ]
    )


def _fmt_price(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/A"
    return f"{number:.8f}".rstrip("0").rstrip(".")
