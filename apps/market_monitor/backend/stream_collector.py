import asyncio
import json
import logging
import os
import time

import websockets

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:
    from binance_ws_routes import (
        FUTURES_TICKER_MARK_PRICE_STREAM_URL,
        normalize_market_stream_url,
    )
except ModuleNotFoundError:
    from apps.market_monitor.backend.binance_ws_routes import (
        FUTURES_TICKER_MARK_PRICE_STREAM_URL,
        normalize_market_stream_url,
    )

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        value = os.getenv(name)
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


REDIS_URL = "redis://redis:6379/0"
BINANCE_WS_URL = normalize_market_stream_url(
    str(os.getenv("BINANCE_STREAM_WS_URL") or FUTURES_TICKER_MARK_PRICE_STREAM_URL).strip()
    or FUTURES_TICKER_MARK_PRICE_STREAM_URL
)
NO_MESSAGE_RECONNECT_SEC = max(
    5.0,
    _env_float("BINANCE_STREAM_NO_MESSAGE_RECONNECT_SEC", 45.0),
)
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
WS_PROXY = HTTPS_PROXY or HTTP_PROXY


class StreamerDataStalled(RuntimeError):
    pass


async def connect_redis():
    if redis is None:
        raise RuntimeError("redis package is required for stream collector")
    return await redis.from_url(REDIS_URL, decode_responses=True)


async def process_message(r, message) -> bool:
    try:
        payload = json.loads(message)
        if not isinstance(payload, dict) or "data" not in payload:
            return False

        stream_name = payload.get("stream")
        data = payload.get("data")
        if not data:
            return False

        if "ticker@arr" in stream_name:
            symbols = [item["s"] for item in data]
            supply_keys = [f"supply:{s}" for s in symbols]
            max_supply_keys = [f"max_supply:{s}" for s in symbols]
            supplies_list = await r.mget(supply_keys)
            max_supplies_list = await r.mget(max_supply_keys)
            supply_map = {sym: val for sym, val in zip(symbols, supplies_list)}
            max_supply_map = {sym: val for sym, val in zip(symbols, max_supplies_list)}

            pipe = r.pipeline()
            for item in data:
                symbol = item["s"]
                price = float(item["c"])
                change_pct = item["P"]
                volume = float(item["q"])

                supply_str = supply_map.get(symbol)
                realtime_mc = 0.0
                if supply_str:
                    try:
                        realtime_mc = price * float(supply_str)
                    except ValueError:
                        realtime_mc = 0.0

                max_supply_str = max_supply_map.get(symbol)
                realtime_fdv = 0.0
                if max_supply_str:
                    try:
                        realtime_fdv = price * float(max_supply_str)
                    except ValueError:
                        realtime_fdv = 0.0

                key = f"market_data:{symbol}"
                pipe.hset(
                    key,
                    mapping={
                        "price": price,
                        "change_24h": change_pct,
                        "volume_24h": volume,
                        "mc": realtime_mc,
                        "fdv": realtime_fdv,
                        "updated_at": item["E"],
                    },
                )
            await pipe.execute()
            return True

        if "markPrice@arr" in stream_name:
            pipe = r.pipeline()
            for item in data:
                symbol = item["s"]
                funding_rate = float(item["r"])
                key = f"market_data:{symbol}"
                pipe.hset(key, mapping={"funding_rate": funding_rate})
            await pipe.execute()
            return True
        return False
    except Exception:
        logger.exception("Failed to process websocket message")
        return False


async def _run_stream_loop_direct(r):
    async with websockets.connect(
        BINANCE_WS_URL,
        open_timeout=20,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
    ) as ws:
        logger.info("Binance websocket connected (direct mode).")
        valid_message_count = 0
        last_valid_message_at = time.monotonic()
        while True:
            remaining = NO_MESSAGE_RECONNECT_SEC - (time.monotonic() - last_valid_message_at)
            if remaining <= 0:
                raise StreamerDataStalled(
                    f"Binance websocket received no valid market data for {NO_MESSAGE_RECONNECT_SEC:.0f}s"
                )
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise StreamerDataStalled(
                    f"Binance websocket received no valid market data for {NO_MESSAGE_RECONNECT_SEC:.0f}s"
                ) from exc
            if await process_message(r, message):
                last_valid_message_at = time.monotonic()
                valid_message_count += 1
                if valid_message_count == 1 or valid_message_count % 300 == 0:
                    logger.info(
                        "Binance websocket data healthy: valid_messages=%s",
                        valid_message_count,
                    )


async def _run_stream_loop_proxy(r):
    if aiohttp is None:
        raise RuntimeError("aiohttp is required for proxy websocket mode")

    timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(BINANCE_WS_URL, proxy=WS_PROXY, heartbeat=20) as ws:
            logger.info("Binance websocket connected via proxy: %s", WS_PROXY)
            valid_message_count = 0
            last_valid_message_at = time.monotonic()
            while True:
                remaining = NO_MESSAGE_RECONNECT_SEC - (time.monotonic() - last_valid_message_at)
                if remaining <= 0:
                    raise StreamerDataStalled(
                        f"Binance websocket received no valid market data for {NO_MESSAGE_RECONNECT_SEC:.0f}s"
                    )
                try:
                    msg = await ws.receive(timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise StreamerDataStalled(
                        f"Binance websocket received no valid market data for {NO_MESSAGE_RECONNECT_SEC:.0f}s"
                    ) from exc
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if await process_message(r, msg.data):
                        last_valid_message_at = time.monotonic()
                        valid_message_count += 1
                        if valid_message_count == 1 or valid_message_count % 300 == 0:
                            logger.info(
                                "Binance websocket data healthy: valid_messages=%s",
                                valid_message_count,
                            )
                    continue
                if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                    break
                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(f"aiohttp websocket error: {ws.exception()}")


async def start_stream():
    r = await connect_redis()
    logger.info(
        "Redis connected. Preparing Binance stream: url=%s no_data_timeout=%ss",
        BINANCE_WS_URL,
        NO_MESSAGE_RECONNECT_SEC,
    )
    if WS_PROXY:
        logger.info("Streamer proxy mode enabled: %s", WS_PROXY)

    while True:
        try:
            if WS_PROXY:
                await _run_stream_loop_proxy(r)
            else:
                await _run_stream_loop_direct(r)
        except StreamerDataStalled as exc:
            logger.warning("%s; retry in 3 seconds...", exc)
            await asyncio.sleep(3)
        except (websockets.ConnectionClosed, asyncio.TimeoutError):
            logger.warning("Websocket closed, retry in 3 seconds...")
            await asyncio.sleep(3)
        except Exception:
            logger.exception("Streamer encountered an error")
            await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(start_stream())
    except KeyboardInterrupt:
        print("Streamer stopped")
