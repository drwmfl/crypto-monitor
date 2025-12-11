from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import desc
import threading

# 导入数据库配置
# 🔥 注意：必须导入 wait_for_db_connection 这个函数
from database import get_db, engine, Base, wait_for_db_connection
from models import MarketData
from collector import run_collector

app = FastAPI()

# 配置跨域，允许前端 (localhost:5173) 访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 🔥 核心启动逻辑 (解决报错的关键)
# ==========================================
@app.on_event("startup")
def startup_event():
    print("------ 系统初始化开始 ------")
    
    # 1. ⛔️ 阻断式等待：数据库没连上之前，程序会卡在这里，不会报错退出
    # 只要这个函数不返回，下面的代码就不会执行
    wait_for_db_connection()
    
    # 2. 数据库连接成功后，检查并创建表结构
    print("🛠️ 正在检查并创建数据库表...")
    Base.metadata.create_all(bind=engine)
    
    # 3. 一切就绪，启动采集器后台线程
    print("🚀 启动后台采集线程...")
    t = threading.Thread(target=run_collector, daemon=True)
    t.start()
    
    print("------ 系统启动完成 ------")

# ==========================================
# API 接口定义
# ==========================================

@app.get("/")
def read_root():
    """健康检查接口"""
    return {"status": "ok", "message": "Crypto Monitor Backend is Running"}

@app.get("/api/market-data")
def get_latest_market_data(db: Session = Depends(get_db)):
    """获取最新批次的所有市场数据"""
    # 1. 查最新的时间戳
    last_record = db.query(MarketData).order_by(desc(MarketData.timestamp)).first()
    
    if not last_record:
        return [] # 数据库如果是空的，返回空列表
    
    # 2. 根据这个时间戳，把该批次几百个币的数据全查出来
    data = db.query(MarketData).filter(MarketData.timestamp == last_record.timestamp).all()
    return data