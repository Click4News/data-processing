# 使用官方 Python 3.9 slim 版本作為基底映像
FROM python:3.9-slim

# 設定工作目錄
WORKDIR /app

# 複製 requirements.txt 並安裝相依套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式程式碼到容器中
COPY . .

# 若有需要設定環境變數，建議在 Cloud Run 部署時設定，而非硬編碼於 Dockerfile
# 開放預設端口（Cloud Run 預設使用 8080）
EXPOSE 8080

# 指定啟動指令，假設是執行 news_json.py
CMD ["python", "news_json.py"]
