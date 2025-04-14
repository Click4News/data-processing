import json
import time
import logging
import os
from pymongo import MongoClient
from news_summary import extract_text_from_url, summarize_article, classify_news

# MongoDB connection
MONGO_URI = os.environ.get("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["sqsMessagesDB"]
collection = db["raw_messages"]

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            sns_message = record['Sns']
            message_str = sns_message['Message']
            message = json.loads(message_str)

            title = message.get("title", "Untitled News")
            url = message.get("url", "N/A")

            if url and isinstance(url, str):
                extracted_text = extract_text_from_url(url)
                summary = summarize_article(extracted_text)
                category = classify_news(summary)
            else:
                summary = "No summary available"
                category = "Uncategorized"
                url = "N/A"

            coordinates = [-74.006, 40.7128]  # Default location (NYC)

            geojson_news = {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": coordinates
                        },
                        "properties": {
                            "density": 5,
                            "message_id": sns_message.get('MessageId', 'unknown'),
                            "title": title,
                            "summary": summary,
                            "link": url,
                            "category": category,
                            "timestamp": time.time()
                        }
                    }
                ]
            }

            collection.insert_one(geojson_news)
            logger.info(f"Stored message {sns_message['MessageId']} in MongoDB")

        except Exception as e:
            logger.error(f"Failed to process SNS message: {e}")
