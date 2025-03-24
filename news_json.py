import json
import uuid
import boto3
from datetime import datetime
from pymongo import MongoClient
from news_summary import extract_text_from_url, summarize_article, classify_news

# AWS SQS Configuration
#AWS_REGION = "us-east-1"  # Change to your AWS region
#SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/your-account-id/your-queue-name"

#sqs = boto3.client("sqs", region_name=AWS_REGION)

# MongoDB Setup
MONGO_URI = f"mongodb+srv://vasa2949:sandy@cluster0.j5gm2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "newsDB"
COLLECTION_NAME = "news"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def process_news(news_event):
    """Processes a single news JSON object into GeoJSON format."""
    news_url = news_event.get("url")
    title = news_event.get("title", "Untitled News")

    extracted_text = extract_text_from_url(news_url)
    
    if extracted_text in ["Content extraction failed.", "Failed to fetch the article."]:
        print(f"Error processing URL: {news_url}")
        return None

    summary = summarize_article(extracted_text)
    category = classify_news(summary)

    # Generate a unique identifier for the news
    news_id = str(uuid.uuid4())

    # Placeholder coordinates (Replace with actual geolocation extraction)
    coordinates = [-74.006, 40.7128]  # Example coordinates (New York)

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
                    "title": title,
                    "summary": summary,
                    "link": news_url
                }
            }
        ]
    }

    # Store in MongoDB
    # Store in MongoDB
    inserted_doc = collection.insert_one(geojson_news)
    geojson_news["_id"] = str(inserted_doc.inserted_id)  # Convert ObjectId to string
    print(f"Stored news: {title}")

    return geojson_news


'''def consume_sqs_messages():
    """Continuously polls AWS SQS and processes messages."""
    print("AWS SQS Consumer is listening...")

    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,  # Process up to 10 messages at a time
            WaitTimeSeconds=5  # Long polling to reduce empty responses
        )

        if "Messages" not in response:
            continue  # No new messages, keep polling

        for message in response["Messages"]:
            try:
                # Parse JSON (Assuming multiple news items in one message)
                news_events = json.loads(message["Body"])

                if isinstance(news_events, list):  # If multiple JSON objects
                    for news_event in news_events:
                        process_news(news_event)
                else:  # Single JSON object
                    process_news(news_events)

                # Delete message after processing
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
                print(f"Deleted message: {message['MessageId']}")

            except Exception as e:
                print(f"Error processing message: {e}")'''

# Start consuming SQS messages
#consume_sqs_messages()
# Sample News JSON (Manually Provided)
sample_news_json = {
    "uri": "8595920420",
    "lang": "eng",
    "isDuplicate": True,
    "date": "2025-03-19",
    "time": "02:12:59",
    "dateTime": "2025-03-19T02:12:59Z",
    "dateTimePub": "2025-03-18T14:43:26Z",
    "dataType": "news",
    "sim": 0,
    "url": "https://www.delcotimes.com/2025/03/18/immigration-activist-detained/",
    "title": "Woman who had sought protection from deportation in Colorado churches is detained, advocates say",
    "body": "DENVER (AP) -- A woman who gained prominence after she took refuge in churches...",
    "source": {
        "uri": "delcotimes.com",
        "dataType": "news",
        "title": "The Delaware County Daily Times"
    },
    "authors": [
        {
            "uri": "associated_press@delcotimes.com",
            "name": "Associated Press",
            "type": "author",
            "isAgency": False
        }
    ],
    "image": "https://www.delcotimes.com/wp-content/uploads/2021/08/dtimes.jpg",
    "eventUri": None,
    "sentiment": -0.0431,
    "wgt": 480046379,
    "relevance": 1,
    "city": "Denver",
    "id": "67da36c6e891faf81adb701f",
    "geoJson": {
        "type": "Location",
        "geometry": {
            "type": "Point",
            "coordinates": [39.7392364, -104.984862]
        },
        "properties": {
            "name": "Denver"
        }
    }
}

# Test processing a single JSON
processed_news = process_news(sample_news_json)

# Print the processed GeoJSON
print(json.dumps(processed_news, indent=2))

