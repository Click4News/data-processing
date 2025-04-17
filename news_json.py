import boto3
import json
import logging
import time
from pymongo import MongoClient
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
from news_summary import  summarize_article, classify_news, translate_to_english
from langdetect import detect, LangDetectException
from urllib.parse import urlparse
# Load environment variables from .env
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Setup MongoDB
client = MongoClient(mongo_uri)
db = client["sqsMessagesDB1"]
collection = db["raw_messages"]
users_collection = db["users"]

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_queue_url(queue_name, create_if_not_exists=True):
    region_name = 'us-east-2'
    sqs = boto3.client('sqs', region_name=region_name)
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        logger.info(f"Found existing queue: {queue_name}")
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue' and create_if_not_exists:
            logger.info(f"Queue {queue_name} does not exist. Creating it...")
            response = sqs.create_queue(QueueName=queue_name)
            logger.info(f"Successfully created queue: {queue_name}")
            return response['QueueUrl']
        else:
            logger.error(f"Error getting queue URL: {e}")
            raise


def process_message(message):
    message_id = message.get('MessageId', 'unknown')
    receipt_handle = message.get('ReceiptHandle')

    try:
        body = message.get('Body', '{}')
        logger.info(f"Raw message body: {body}")

        # Attempt first decode
        try:
            body_json = json.loads(body)
            # If double-encoded (string inside string), decode again
            if isinstance(body_json, str):
                body_json = json.loads(body_json)
                news_type = body_json.get("type", "CREATE").upper()
                news_userid = body_json.get("userid")
                if not news_userid:
                    # fallback: infer from domain
                    parsed_url = urlparse(body_json.get("url", ""))
                    domain = parsed_url.netloc  # e.g. "www.nytimes.com"
                    if domain.startswith("www."):
                        domain = domain[4:]  # remove 'www.'

                    news_userid = domain.split(".")[0]  # "nytimes"
                    body_json["userid"] = news_userid

                import random
                body_json["likes"] = random.randint(10, 20)
                body_json["fakeflags"] = random.randint(0, 5)
                body_json["userid"] = news_userid  # Ensure userid exists

                if news_type in ["LIKED", "FAKEFLAGGED"]:
                    message_id = body_json.get("message_id")
                    actor_userid = body_json.get("userid")  # the liker or flagger

                    # Step 1: Fetch the news article
                    news_doc = collection.find_one({
                        "features.properties.message_id": message_id
                    })

                    if not news_doc:
                        logger.warning(f"News with message_id {message_id} not found.")
                        return "skip", receipt_handle

                    props = news_doc["features"][0]["properties"]
                    owner_userid = props.get("userid", "unknown")
                    likes = props.get("likes", 0)
                    fakeflags = props.get("fakeflags", 0)

                    # Step 2: Get actor credibility (default 50)
                    actor_doc = users_collection.find_one({"userid": actor_userid})
                    actor_cred = actor_doc.get("credibility_score", 50) if actor_doc else 50

                    # Step 3: Determine update based on type
                    if news_type == "LIKED":
                        likes += 1
                        collection.update_one(
                            {"features.properties.message_id": message_id},
                            {"$set": {"features.0.properties.likes": likes}}
                        )

                        boost = (0.4 * actor_cred + 0.3 * likes - 0.5 * fakeflags) / 2

                    elif news_type == "FAKEFLAGGED":
                        fakeflags += 1
                        collection.update_one(
                            {"features.properties.message_id": message_id},
                            {"$set": {"features.0.properties.fakeflags": fakeflags}}
                        )

                        boost = (-0.3 * actor_cred - 0.4 * fakeflags + 0.2 * likes) / 2  # Negative boost

                    # Step 4: Update owner's credibility
                    owner_doc = users_collection.find_one({"userid": owner_userid})
                    owner_score = owner_doc.get("credibility_score", 50) if owner_doc else 50
                    new_score = max(0, min(100, owner_score + boost))

                    users_collection.update_one(
                        {"userid": owner_userid},
                        {"$set": {"credibility_score": new_score}},
                        upsert=True
                    )

                    logger.info(f"{news_type} → Updated credibility of {owner_userid} to {new_score}")
                    return "skip", receipt_handle

                # Now fallback to normal processing (CREATE case)
        except json.JSONDecodeError:
            logger.warning(f"Skipping non-JSON message: {message_id}")
            return "skip", receipt_handle

        # If 'articles' exists and is a list, extract the first article
        if "articles" in body_json and isinstance(body_json["articles"], list) and len(body_json["articles"]) > 0:
            body_json = body_json["articles"][0]

        raw_title = body_json.get("title", "Untitled News")

        try:
            lang = detect(raw_title)
            if lang != 'en':
                title = translate_to_english(raw_title)
                if not title or "translation unavailable" in title.lower():
                    logger.warning(f"Message {message_id} skipped due to title translation failure.")
                    return "skip", receipt_handle
            else:
                title = raw_title  # Skip translation if already English
        except LangDetectException:
            logger.warning(f"Could not detect language for title: {raw_title}. Skipping message.")
            return "skip", receipt_handle
        url = body_json.get("url")
        coordinates = (
            body_json.get("geoJson", {})
            .get("geometry", {})
            .get("coordinates", None)
        )

        # Validate coordinates
        if not coordinates or not isinstance(coordinates, list) or len(coordinates) != 2:
            return "skip", receipt_handle

        # Validate URL
        if not url or not isinstance(url, str) or not url.startswith("http"):
            logger.warning(f"Message {message_id} skipped due to missing or invalid URL.")
            return "skip", receipt_handle

        # Extract or fallback to full body text
        extracted_text = body_json.get("body", "")
        if not extracted_text or len(extracted_text.strip()) < 20:
            logger.warning(f"Message {message_id} skipped due to empty or too short article body.")
            return "skip", receipt_handle

        # Translate
        translated_text = translate_to_english(extracted_text)
        if not translated_text or "translation unavailable" in translated_text.lower():
            logger.warning(f"Message {message_id} skipped due to translation failure.")
            return "skip", receipt_handle

        # Summarize
        summary = summarize_article(translated_text)
        if not summary or "summary unavailable" in summary.lower() or "failed to extract" in summary.lower():
            logger.warning(f"Message {message_id} skipped due to summarization failure.")
            return "skip", receipt_handle

        # Classify
        category = classify_news(summary)

        # Get additional metadata
        attributes = message.get('MessageAttributes', {})
        attribute_values = {k: v.get('StringValue') for k, v in attributes.items()}

        # Prepare GeoJSON object
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
                        "message_id": message_id,
                        "title": title,
                        "summary": summary,
                        "link": url,
                        "category": category,
                        "attributes": attribute_values,
                        "timestamp": time.time()
                    }
                }
            ]
        }

        collection.insert_one(geojson_news)
        logger.info(f"Stored message {message_id} in MongoDB")

        return geojson_news, receipt_handle

    except Exception as e:
        logger.error(f"Error processing message {message_id}: {e}")
        return None, receipt_handle
    message_id = message.get('MessageId', 'unknown')
    receipt_handle = message.get('ReceiptHandle')

    try:
        body = message.get('Body', '{}')
        attributes = message.get('MessageAttributes', {})
        attribute_values = {k: v.get('StringValue') for k, v in attributes.items()}

        try:
            body_json = json.loads(body)
            logger.info(f"Processing JSON message: {message_id}")
        except json.JSONDecodeError:
            logger.warning(f"Skipping non-JSON message: {message_id}")
            return None, receipt_handle

        title = body_json.get("title", "Untitled News")
        url = body_json.get("url")
        coordinates = (
    body_json.get("geoJson", {})
    .get("geometry", {})
    .get("coordinates", None)
)


        # Validate coordinates
        if not coordinates or not isinstance(coordinates, list) or len(coordinates) != 2:
            logger.warning(f"Skipping message {message_id} due to missing or invalid coordinates.")
            return None, receipt_handle

        # Skip messages without a valid URL
        if not url or not isinstance(url, str) or not url.startswith("http"):
            logger.warning(f"Message {message_id} skipped due to missing or invalid URL.")
            return None, receipt_handle

        extracted_text = body_json.get("body", "")
        if not extracted_text or len(extracted_text.strip()) < 20:
            logger.warning(f"Message {message_id} skipped due to empty or too short article body.")
            return None, receipt_handle


        translated_text = translate_to_english(extracted_text)

        if not translated_text or "translation unavailable" in translated_text.lower():
            logger.warning(f"Message {message_id} skipped due to translation failure.")
            return None, receipt_handle

        summary = summarize_article(translated_text)

        if not summary or "summary unavailable" in summary.lower() or "failed to extract" in summary.lower():
            logger.warning(f"Message {message_id} skipped due to summarization failure.")
            return None, receipt_handle

        category = classify_news(summary)

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
                        "message_id": message_id,
                        "title": title,
                        "summary": summary,
                        "link": url,
                        "category": category,
                        "attributes": attribute_values,
                        "timestamp": time.time()
                    }
                }
            ]
        }

        collection.insert_one(geojson_news)
        logger.info(f"Stored message {message_id} in MongoDB")

        return geojson_news, receipt_handle

    except Exception as e:
        logger.error(f"Error processing message {message_id}: {e}")
        return None, receipt_handle


def delete_message(queue_url, receipt_handle):
    region_name = 'us-east-2'
    sqs = boto3.client('sqs', region_name=region_name)
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        return True
    except ClientError as e:
        logger.error(f"Error deleting message: {e}")
        return False


def consume_messages(queue_name, max_messages=10, wait_time=20, visibility_timeout=30):
    queue_url = get_queue_url(queue_name)
    logger.info(f"Consuming messages from {queue_name}")

    region_name = 'us-east-2'
    sqs = boto3.client('sqs', region_name=region_name)

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=visibility_timeout,
                MessageAttributeNames=['All']
            )

            messages = response.get('Messages', [])
            if not messages:
                logger.info("No messages received. Continuing to poll...")
                continue

            logger.info(f"Received {len(messages)} messages")

            for message in messages:
                geojson_result, receipt_handle = process_message(message)
                if receipt_handle:
                    if geojson_result:  # processed and valid
                         deleted = delete_message(queue_url, receipt_handle)
                    elif geojson_result == "skip":  # non-JSON or intentionally skipped
                        deleted = delete_message(queue_url, receipt_handle)

                if deleted:
                    logger.info(f"Deleted message {message.get('MessageId')}")
                else:
                    logger.warning(f"Failed to delete message {message.get('MessageId')}")



        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            break

        except Exception as e:
            logger.error(f"Error in message consumption loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    QUEUE_NAME = 'test-queue'
    MAX_MESSAGES = 5
    WAIT_TIME = 20
    VISIBILITY_TIMEOUT = 60

    logger.info(f"Starting SQS consumer for queue {QUEUE_NAME}")
    consume_messages(
        queue_name=QUEUE_NAME,
        max_messages=MAX_MESSAGES,
        wait_time=WAIT_TIME,
        visibility_timeout=VISIBILITY_TIMEOUT
    )
