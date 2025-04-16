FROM python:3.12-slim
WORKDIR /app

# 安装所有依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝源码
COPY . .

# 明确暴露端口（可选）
EXPOSE 8080

# 用 shell 启动，让 Uvicorn 读取环境变量 PORT
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
