from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# 从环境变量读取数据库地址，默认指向 Docker 内部地址
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/crypto_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 获取数据库会话的依赖函数
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# backend/database.py 的末尾添加：
from sqlalchemy import text # 记得在顶部导入 text
import time

def wait_for_db_connection():
    retries = 0
    max_retries = 60  # 最多等 60 次 (2分钟)
    
    print("🔄 [System] 正在尝试连接数据库...")
    while retries < max_retries:
        try:
            # 尝试建立一个原始连接
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print("✅ [System] 数据库连接成功！")
            return
        except Exception as e:
            retries += 1
            print(f"⏳ [System] 数据库未就绪，等待 2 秒... ({retries}/{max_retries})")
            time.sleep(2)
            
    raise Exception("❌ 连接超时，数据库一直没启动")