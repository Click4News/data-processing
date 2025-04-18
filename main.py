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


def scheduled_consumer():
    try:
        logging.info("Running scheduled SQS consumer...")
        consume_messages(
            queue_name="test-queue",
            max_messages=20,
            wait_time=0,           
            visibility_timeout=30
        )
    except Exception as e:
        logging.error(f"Scheduled consumer failed: {e}")


@app.on_event("startup")
def start_scheduler():
    logging.info("Starting scheduler to run every 1 minute...")
    scheduler.add_job(
        scheduled_consumer,
        trigger='interval',
        minutes=1,
        id='news-consumer-job',
        replace_existing=True
    )
    scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    logging.info("Shutting down scheduler...")
    scheduler.shutdown()


@app.get("/")
def health_check():
    return {"message": "FastAPI scheduler is running and polling SQS every 1 minute!"}
