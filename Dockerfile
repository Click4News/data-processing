# 使用官方 Python 3.9-slim 基底映像
FROM python:3.9-slim

# 設定工作目錄
WORKDIR /app

# 複製 requirements.txt 並安裝相依套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製整個專案到容器
COPY . .

# 開放 Cloud Run 預設使用的 8080 埠口
EXPOSE 8080

# 指定啟動指令，執行 main.py（請依您檔案名稱調整）
CMD ["python", "main.py"]
