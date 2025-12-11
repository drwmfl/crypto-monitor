from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from database import Base

class MarketData(Base):
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    symbol = Column(String, index=True)
    price = Column(Float)
    
    change_1h = Column(Float)
    change_24h = Column(Float)
    
    # 🔥 新增字段
    mc = Column(Float)              # 流通市值 (MC)
    fdv = Column(Float)             # 全流通市值 (FDV)
    oi_change_val = Column(Float)   # OI 5分钟涨跌额
    oi_change_pct = Column(Float)   # OI 5分钟涨跌幅
    
    volume_24h = Column(Float)
    oi = Column(Float)
    funding_rate = Column(Float)
    oi_mc_ratio = Column(Float)
    oi_vol_ratio = Column(Float)
    listing_hours = Column(Integer)