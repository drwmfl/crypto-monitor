import asyncio
import json
import logging
import os

import redis.asyncio as redis
import websockets

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REDIS_URL = "redis://redis:6379/0"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr"
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
WS_PROXY = HTTPS_PROXY or HTTP_PROXY


async def connect_redis():
    return await redis.from_url(REDIS_URL, decode_responses=True)


async def process_message(r, message):
    try:
        payload = json.loads(message)
        if not isinstance(payload, dict) or "data" not in payload:
            return

        stream_name = payload.get("stream")
        data = payload.get("data")
        if not data:
            return

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
            return

        if "markPrice@arr" in stream_name:
            pipe = r.pipeline()
            for item in data:
                symbol = item["s"]
                funding_rate = float(item["r"])
                key = f"market_data:{symbol}"
                pipe.hset(key, mapping={"funding_rate": funding_rate})
            await pipe.execute()
            return
    except Exception:
        logger.exception("Failed to process websocket message")


async def _run_stream_loop_direct(r):
    async with websockets.connect(
        BINANCE_WS_URL,
        open_timeout=20,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
    ) as ws:
        logger.info("Binance websocket connected (direct mode).")
        while True:
            message = await ws.recv()
            await process_message(r, message)


async def _run_stream_loop_proxy(r):
    if aiohttp is None:
        raise RuntimeError("aiohttp is required for proxy websocket mode")

    timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(BINANCE_WS_URL, proxy=WS_PROXY, heartbeat=20) as ws:
            logger.info("Binance websocket connected via proxy: %s", WS_PROXY)
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await process_message(r, msg.data)
                    continue
                if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                    break
                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(f"aiohttp websocket error: {ws.exception()}")


async def start_stream():
    r = await connect_redis()
    logger.info("Redis connected. Preparing Binance stream...")
    if WS_PROXY:
        logger.info("Streamer proxy mode enabled: %s", WS_PROXY)

    while True:
        try:
            if WS_PROXY:
                await _run_stream_loop_proxy(r)
            else:
                await _run_stream_loop_direct(r)
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
