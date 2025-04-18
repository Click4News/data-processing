import random
from urllib.parse import urlparse
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load .env to access MONGO_URI
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client["sqsMessagesDB1"]
collection = db["raw_messages"]

def extract_userid_from_url(url):
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain.split(".")[0]
    except Exception:
        return "unknown"

def update_documents():
    updated_count = 0

    # Find documents where userid is missing in the first feature
    docs = collection.find({
        "$or": [
            {"features.0.properties.userid": {"$exists": False}},
            {"features.0.properties.likes": {"$exists": False}},
            {"features.0.properties.fakeflags": {"$exists": False}}
        ]
    })

    for doc in docs:
        try:
            props = doc["features"][0]["properties"]
            link = props.get("link")
            if not link:
                continue

            userid = extract_userid_from_url(link)
            likes = random.randint(10, 20)
            fakeflags = random.randint(1, 5)

            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "features.0.properties.userid": userid,
                        "features.0.properties.likes": likes,
                        "features.0.properties.fakeflags": fakeflags
                    }
                }
            )

            print(f"Updated doc: {doc['_id']} → userid={userid}, likes={likes}, fakeflags={fakeflags}")
            updated_count += 1
        except Exception as e:
            print(f"Error updating doc {doc.get('_id')}: {e}")

    print(f"\n✅ Total documents updated: {updated_count}")

if __name__ == "__main__":
    update_documents()
