# 使用 Python 3.9 作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 先复制依赖清单（利用 Docker 缓存机制加速）
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制剩下所有的代码
COPY . .

# 暴露端口（虽然 docker-compose 会覆盖，但写上是好习惯）
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]