import os
import threading
from flask import Flask
from news_json import consume_messages  # 假設 consume_messages 為 SQS 消費者主函數

app = Flask(__name__)

@app.route('/')
def home():
    return "Service is up and running!", 200

def run_consumer():
    # 根據您的需求傳入相關參數，此處僅示範呼叫方式
    # 如果 consume_messages 是個無窮迴圈的消費者，就在此啟動
    consume_messages(queue_url="YOUR_QUEUE_URL")

if __name__ == "__main__":
    # 在另一個 thread 中執行 SQS 消費者，確保主執行緒能用來啟動 HTTP 服務
    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # 取得環境變數 PORT，Cloud Run 會自動設定為 8080
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
