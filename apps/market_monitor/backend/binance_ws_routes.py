from __future__ import annotations

from typing import Iterable


FUTURES_MARKET_STREAM_URL = "wss://fstream.binance.com/market/stream"
_FUTURES_WS_ROOT = "wss://fstream.binance.com"


def normalize_market_stream_url(value: str) -> str:
    url = str(value or "").strip() or FUTURES_MARKET_STREAM_URL
    if url == f"{_FUTURES_WS_ROOT}/ws":
        return FUTURES_MARKET_STREAM_URL
    if url.startswith(f"{_FUTURES_WS_ROOT}/ws/"):
        return url.replace(f"{_FUTURES_WS_ROOT}/ws/", f"{_FUTURES_WS_ROOT}/market/ws/", 1)
    if url == f"{_FUTURES_WS_ROOT}/stream" or url.startswith(
        f"{_FUTURES_WS_ROOT}/stream?"
    ):
        return url.replace(
            f"{_FUTURES_WS_ROOT}/stream",
            f"{_FUTURES_WS_ROOT}/market/stream",
            1,
        )
    return url


def combined_market_stream_url(streams: Iterable[str]) -> str:
    normalized = [
        str(stream or "").strip()
        for stream in streams
        if str(stream or "").strip()
    ]
    if not normalized:
        return FUTURES_MARKET_STREAM_URL
    return f"{FUTURES_MARKET_STREAM_URL}?streams={'/'.join(normalized)}"


FUTURES_TICKER_MARK_PRICE_STREAM_URL = combined_market_stream_url(
    ("!ticker@arr", "!markPrice@arr")
)
