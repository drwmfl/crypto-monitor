import time
import threading
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, text # ✅ 补全 sqlalchemy 工具
from sqlalchemy.exc import OperationalError

# 1. 导入项目模块
# 如果 database.py 里没有 wait_for_db_connection 也不怕，我们在下面自己定义了
from database import get_db, engine, Base, SessionLocal
import models
from models import MarketData
import collector # 确保导入采集器

app = FastAPI()

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
# (直接写在这里，避免 import 报错)
# ==========================================
def wait_for_db_connection(max_retries=60, wait_interval=2):
    print(f"🔄 [System] 正在尝试连接数据库...")
    for i in range(max_retries):
        try:
            # 尝试获取一个连接并执行简单查询
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
def startup_event():
    print("------ 系统初始化开始 ------")
    
    # 1. 等待数据库
    wait_for_db_connection()
    
    # 2. 创建表结构
    print("🛠️ 正在检查并创建数据库表...")
    models.Base.metadata.create_all(bind=engine)
    
    # 3. 启动后台采集
    print("🚀 启动后台采集线程...")
    try:
        t = threading.Thread(target=collector.run_collector, daemon=True)
        t.start()
    except Exception as e:
        print(f"❌ 采集器启动失败: {e}")
    
    print("------ 系统启动完成 ------")

# ==========================================
# API 接口定义
# ==========================================
@app.get("/")
def read_root():
    return {"status": "ok", "message": "Crypto Monitor Backend is Running"}

@app.get("/api/market-data")
def get_latest_market_data(db: Session = Depends(get_db)):
    """
    获取最新批次的所有数据
    优化：只查询最新时间戳的数据，解决加载卡顿问题
    """
    try:
        # 1. 查找最新时间 (使用 func.max 极速查询)
        latest_timestamp = db.query(func.max(MarketData.timestamp)).scalar()
        
        if not latest_timestamp:
            return [] # 没数据直接返回空
        
        # 2. 只拿那一刻的数据，并按市值排序
        data = db.query(MarketData)\
            .filter(MarketData.timestamp == latest_timestamp)\
            .order_by(MarketData.mc.desc())\
            .all()
            
        return data
    except Exception as e:
        print(f"❌ 查询数据出错: {e}")
        return []