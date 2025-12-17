import pathway as pw
import os
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import warnings
warnings.filterwarnings("ignore")


BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC_NAME", "news-stocks")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
CONSUMER_ID = os.getenv("CONSUMER_ID", "consumer-default")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "pathway-news-consumer-group")

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "news_database")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "news_collection")

os.makedirs(OUTPUT_DIR, exist_ok=True)


class NewsSchema(pw.Schema):
    symbol: str
    title: str
    description: str
    url: str
    published_at: str
    source: str
    category: str


print("=" * 70)
print(f"PATHWAY CONSUMER WITH MONGODB: {CONSUMER_ID}")
print("=" * 70)
print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
print(f"Topic: {TOPIC}")
print(f"Consumer group: {CONSUMER_GROUP}")
print(f"Output directory: {OUTPUT_DIR}")
print(f"MongoDB URI: {MONGODB_URI}")
print(f"MongoDB Database: {MONGODB_DATABASE}")
print(f"MongoDB Collection: {MONGODB_COLLECTION}")
print()

# Test MongoDB connection first
print(" Testing MongoDB connection...")
try:
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print(" MongoDB connection successful!")
    
    # Check if database and collection exist
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]
    count = collection.count_documents({})
    print(f" Current documents in collection: {count}")
    client.close()
except ConnectionFailure as e:
    print(f" MongoDB connection failed: {e}")
    print("⚠️  Consumer will continue but MongoDB writes may fail!")
except Exception as e:
    print(f" MongoDB error: {e}")

print()
print("Data Flow:")
print("  Kafka → Pathway → [CSV + JSONL + MongoDB]")
print()

# Read from Kafka
news_table = pw.io.kafka.read(
    rdkafka_settings={
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "true",
        "auto.commit.interval.ms": "1000",
    },
    topic=TOPIC,
    format="json",
    schema=NewsSchema,
    autocommit_duration_ms=1000
)

# Output file paths
csv_output = f"{OUTPUT_DIR}/streamed_news_data.csv"
json_output = f"{OUTPUT_DIR}/news_data.jsonl"

print(f"✓ Writing to:")
print(f"   CSV:     {csv_output}")
print(f"   JSONL:   {json_output}")
print(f"   MongoDB: {MONGODB_DATABASE}.{MONGODB_COLLECTION}")
print()

# Write to CSV
pw.io.csv.write(news_table, csv_output)

# Write to JSONL
pw.io.jsonlines.write(news_table, json_output)

# Write to MongoDB with explicit configuration
try:
    pw.io.mongodb.write(
        news_table,
        connection_string=MONGODB_URI,
        database=MONGODB_DATABASE,
        collection=MONGODB_COLLECTION,
    )
    print(" MongoDB writer configured")
except Exception as e:
    print(f" Failed to configure MongoDB writer: {e}")

# Run the computation graph
print("✓ Starting consumer... Press Ctrl+C to stop")
print("=" * 70)

try:
    pw.run()
except Exception as e:
    print(f" Error during execution: {e}")
    raise