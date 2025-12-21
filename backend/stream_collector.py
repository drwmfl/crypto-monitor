import asyncio
import json
import websockets
import redis.asyncio as redis
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis 配置
REDIS_URL = "redis://redis:6379/0"

# 🔥 [核心修改] 使用组合流地址，同时订阅 Ticker(价格) 和 MarkPrice(费率)
# 文档: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-to-streams
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr"

async def connect_redis():
    """连接 Redis"""
    return await redis.from_url(REDIS_URL, decode_responses=True)

async def process_message(r, message):
    """处理币安推送的消息 (支持多流模式)"""
    try:
        # 解析组合流消息结构: {"stream": "...", "data": [...]}
        payload = json.loads(message)
        
        # 容错：如果格式不对，直接跳过
        if not isinstance(payload, dict) or 'data' not in payload:
            return

        stream_name = payload.get('stream')
        data = payload.get('data')

        if not data: return

        # =========================================
        # 🟢 场景 A: 价格行情 (!ticker@arr) -> 计算市值 & FDV
        # =========================================
        if 'ticker@arr' in stream_name:
            # 🔥 [优化 1] 批量准备 Key
            symbols = [item['s'] for item in data]
            supply_keys = [f"supply:{s}" for s in symbols]
            max_supply_keys = [f"max_supply:{s}" for s in symbols]
            
            # 🔥 [优化 2] 使用 mget 批量获取
            supplies_list = await r.mget(supply_keys)
            max_supplies_list = await r.mget(max_supply_keys)
            
            # 建立映射字典
            supply_map = {sym: val for sym, val in zip(symbols, supplies_list)}
            max_supply_map = {sym: val for sym, val in zip(symbols, max_supplies_list)}

            pipe = r.pipeline()
            
            for item in data:
                symbol = item['s']
                price = float(item['c'])
                change_pct = item['P']
                volume = float(item['q'])
                
                # 🔥 [优化 3] 实时计算市值 (MC)
                supply_str = supply_map.get(symbol)
                realtime_mc = 0
                if supply_str:
                    try:
                        realtime_mc = price * float(supply_str)
                    except ValueError: realtime_mc = 0
                
                # 🔥 [新增] 实时计算全流通市值 (FDV)
                max_supply_str = max_supply_map.get(symbol)
                realtime_fdv = 0
                if max_supply_str:
                    try:
                        realtime_fdv = price * float(max_supply_str)
                    except ValueError: realtime_fdv = 0

                # 写入 Redis (注意：HSET 不会删除已有的 funding_rate 字段)
                key = f"market_data:{symbol}"
                pipe.hset(key, mapping={
                    "price": price,
                    "change_24h": change_pct,
                    "volume_24h": volume,
                    "mc": realtime_mc,
                    "fdv": realtime_fdv,
                    "updated_at": item['E']
                })
            
            await pipe.execute()

        # =========================================
        # 🔵 场景 B: 资金费率 (!markPrice@arr) -> 更新费率
        # =========================================
        elif 'markPrice@arr' in stream_name:
            pipe = r.pipeline()
            for item in data:
                symbol = item['s']
                # 'r' 是资金费率字段
                # 'p' 是标记价格，如果以后需要可以用
                funding_rate = float(item['r']) 
                
                key = f"market_data:{symbol}"
                # 单独更新 funding_rate 字段
                pipe.hset(key, mapping={
                    "funding_rate": funding_rate
                })
            await pipe.execute()
            # logger.info(f"⚡ 更新了 {len(data)} 个币种的实时费率")
        
    except Exception as e:
        logger.error(f"处理消息失败: {e}")

async def start_stream():
    """主循环"""
    r = await connect_redis()
    logger.info("Redis 连接成功，准备连接币安 (双流模式)...")
    
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                logger.info("🔌 币安 WebSocket 已连接！开始接收 [价格 + 费率] 数据流...")
                while True:
                    message = await ws.recv()
                    await process_message(r, message)
                    
        except (websockets.ConnectionClosed, asyncio.TimeoutError):
            logger.warning("⚠️ 连接断开，3秒后重连...")
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"❌ 发生错误: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(start_stream())
    except KeyboardInterrupt:
        print("停止采集")