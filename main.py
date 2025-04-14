from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import sys
import os

# Set path to import custom modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import original function with arguments
from news_json import consume_messages

app = FastAPI()
scheduler = BackgroundScheduler()
logging.basicConfig(level=logging.INFO)

# ✅ Wrapper to call your existing function with arguments
def scheduled_consumer():
    try:
        consume_messages(queue_name="test-queue", max_messages=5, wait_time=5, visibility_timeout=30)
    except Exception as e:
        logging.error(f"Scheduled consumer failed: {e}")

@app.on_event("startup")
def start_scheduler():
    logging.info("Starting background scheduler...")
    scheduler.add_job(scheduled_consumer, 'interval', minutes=2, id='news-consumer-job', replace_existing=True)
    scheduler.start()

@app.on_event("shutdown")
def shutdown_scheduler():
    logging.info("Shutting down scheduler...")
    scheduler.shutdown()

@app.get("/")
def health_check():
    return {"message": "FastAPI scheduler is running and using your consume_messages() function!"}
