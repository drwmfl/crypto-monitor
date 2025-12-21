import time
import threading
import json
import redis.asyncio as redis
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, text
from sqlalchemy.exc import OperationalError

# 1. 导入项目模块
from database import get_db, engine, Base, SessionLocal
import models
from models import MarketData
import collector

app = FastAPI()

# ==========================================
# ⚡️ Redis 配置
# ==========================================
# 定义 Redis 连接池 (全局变量)
redis_client = redis.from_url("redis://redis:6379/0", decode_responses=True)

# 2. 配置跨域
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 🛡️ 安全函数：等待数据库连接
# ==========================================
def wait_for_db_connection(max_retries=60, wait_interval=2):
    print(f"🔄 [System] 正在尝试连接数据库...")
    for i in range(max_retries):
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1"))
            db.close()
            print("✅ [System] 数据库连接成功！")
            return
        except OperationalError:
            print(f"⏳ [System] 数据库未就绪，等待 {wait_interval} 秒... ({i+1}/{max_retries})")
            time.sleep(wait_interval)
        except Exception as e:
            print(f"❌ [System] 数据库连接发生未知错误: {e}")
            time.sleep(wait_interval)
    
    print("❌ [System] 无法连接到数据库，系统可能会崩溃。")

# ==========================================
# 🔥 核心启动逻辑
# ==========================================
@app.on_event("startup")
async def startup_event():
    print("------ 系统初始化开始 ------")
    
    # 1. 等待数据库
    wait_for_db_connection()
    
    # 2. 创建表结构
    print("🛠️ 正在检查并创建数据库表...")
    models.Base.metadata.create_all(bind=engine)
    
    # 3. 启动后台采集 (Postgres 慢速全量数据)
    print("🚀 启动后台采集线程 (全量数据)...")
    try:
        # 这个线程负责每 5 分钟 (根据 collector.py 配置) 跑一次主循环
        t = threading.Thread(target=collector.run_collector, daemon=True)
        t.start()
    except Exception as e:
        print(f"❌ 采集器启动失败: {e}")
        
    print("✅ Redis 连接池已就绪")
    print("------ 系统启动完成 ------")

@app.on_event("shutdown")
async def shutdown_event():
    # 关闭 Redis 连接
    await redis_client.close()

# ==========================================
# API 接口定义
# ==========================================
@app.get("/")
def read_root():
    return {"status": "ok", "message": "Crypto Monitor Backend is Running"}

@app.get("/api/market-data")
def get_latest_market_data(db: Session = Depends(get_db)):
    """
    【慢车道】获取最新批次的所有数据 (Postgres)
    包含：OI、资金费率、以及那些变化不快的数据
    """
    try:
        latest_timestamp = db.query(func.max(MarketData.timestamp)).scalar()
        
        if not latest_timestamp:
            return []
        
        data = db.query(MarketData)\
            .filter(MarketData.timestamp == latest_timestamp)\
            .order_by(MarketData.mc.desc())\
            .all()
            
        return data
    except Exception as e:
        print(f"❌ 查询数据出错: {e}")
        return []

@app.get("/api/market-data/realtime")
async def get_realtime_data():
    """
    【快车道】从 Redis 获取实时价格 + 实时计算的市值 + FDV
    前端每 2 秒调用一次
    """
    try:
        # 1. 扫描所有以 market_data: 开头的 key
        keys = await redis_client.keys("market_data:*")
        
        if not keys:
            return []

        # 2. 使用 Pipeline 批量获取数据
        async with redis_client.pipeline() as pipe:
            for key in keys:
                pipe.hgetall(key)
            results = await pipe.execute()

        # 3. 格式化数据
        data = []
        for key, val in zip(keys, results):
            if val:
                symbol = key.split(":")[1]
                data.append({
                    "symbol": symbol,
                    "price": float(val.get("price", 0)),
                    "change_24h": float(val.get("change_24h", 0)),
                    "volume_24h": float(val.get("volume_24h", 0)),
                    "mc": float(val.get("mc", 0)), # 实时市值
                    "fdv": float(val.get("fdv", 0)), # ✅ 新增：实时全流通市值
                    # 🔥 [新增] 读取实时费率
                    # 默认值给 0 (或者你想要的其他默认值)
                    "funding_rate": float(val.get("funding_rate", 0))
                })
        
        return data

    except Exception as e:
        print(f"❌ Redis 读取错误: {e}")
        return []