import os
import json
import pathway as pw
from confluent_kafka import Producer
import threading

import warnings
warnings.filterwarnings("ignore")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS_NEWS = {
    "stocks": os.getenv("TOPIC_STOCKS_NEWS", "news-stocks"),
    #"crypto": os.getenv("TOPIC_CRYPTO_NEWS", "news-crypto"),
    #"commodities": os.getenv("TOPIC_COMMODITIES_NEWS", "news-commodities"),
    #"forex": os.getenv("TOPIC_FOREX_NEWS", "news-forex")
}

CSV_PATH = os.getenv("CSV_PATH", "/app/output/news_data.csv")
SPEEDUP = float(os.getenv("SPEEDUP", 1.0))


class NewsSchema(pw.Schema):
    symbol: str
    title: str
    description: str
    url: str
    published_at: str
    source: str
    category: str
    time: int
    diff: int


# Initialize Kafka producer globally
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'news-producer-replay',
    'compression.type': 'gzip',
    'linger.ms': 50,  # Increased for better batching
    'batch.size': 32768,  # Doubled batch size
    'acks': 'all',
    'enable.idempotence': True
})

# Statistics tracking
partition_stats = {}
total_messages = 0

# Shutdown event for graceful termination
shutdown_event = threading.Event()


def _background_poll():
    """
    Background thread for Kafka producer polling.
    Handles delivery callbacks without blocking message production.
    """
    while not shutdown_event.is_set():
        producer.poll(0.1)


# Start background polling thread
poll_thread = threading.Thread(target=_background_poll, daemon=True, name="kafka-poll")
poll_thread.start()
print("✅ Background Kafka polling thread started")


def delivery_callback(err, msg):
    """Callback for message delivery reports"""
    if err:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")


def determine_topic(category):
    """Map category to appropriate Kafka topic"""
    category_lower = category.lower()
    
    if "stock" in category_lower or category_lower == "general":
        return KAFKA_TOPICS_NEWS["stocks"]
    elif "crypto" in category_lower:
        return KAFKA_TOPICS_NEWS["crypto"]
    elif "commodit" in category_lower:
        return KAFKA_TOPICS_NEWS["commodities"]
    elif "forex" in category_lower:
        return KAFKA_TOPICS_NEWS["forex"]
    else:
        return KAFKA_TOPICS_NEWS["stocks"]


def publish_row_to_kafka(row):
    """Publish a single row to Kafka (non-blocking, background poll handles callbacks)"""
    global total_messages, partition_stats
    
    try:
        symbol = row["symbol"]
        category = row["category"]
        
        # Prepare message payload
        message = {
            "symbol": symbol,
            "title": row["title"],
            "description": row["description"],
            "url": row["url"],
            "published_at": row["published_at"],
            "source": row["source"],
            "category": category
        }
        
        # Determine topic and partition
        topic = determine_topic(category)
        partition = hash(symbol) % 3  # Consistent partitioning by symbol
        
        # Produce message (non-blocking, background thread handles poll)
        producer.produce(
            topic,
            key=symbol.encode("utf-8"),
            value=json.dumps(message).encode("utf-8"),
            partition=partition,
            callback=delivery_callback
        )
        
        # Update stats
        partition_stats[partition] = partition_stats.get(partition, 0) + 1
        total_messages += 1
        
        # Print stats every 10 messages
        if total_messages % 10 == 0:
            print(f"\nSTATISTICS (Total: {total_messages} messages)")
            print(f"   Partition distribution: {partition_stats}")
        
        # No poll() here - background thread handles delivery callbacks
        
    except Exception as e:
        print(f"Error publishing message: {e}")


def main():
    """Main function to replay CSV and stream to Kafka"""
    print("=" * 80)
    print("PATHWAY CSV REPLAY → KAFKA PRODUCER")
    print("=" * 80)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topics: {KAFKA_TOPICS_NEWS}")
    print(f"CSV path: {CSV_PATH}")
    print(f"Speedup: {SPEEDUP}x")
    print("=" * 80)
    print()
    
    print("Starting CSV replay stream...")
    print(f"Reading from: {CSV_PATH}")
    print()
    
    # Read CSV with time-based replay
    news_stream = pw.demo.replay_csv_with_time(
        path=CSV_PATH,
        schema=NewsSchema,
        time_column="time",
        unit="ms",
        autocommit_ms=100,
        speedup=SPEEDUP
    )
    
    # Use pw.io.subscribe to process each row
    def process_row(key, row, time, is_addition):
        """Callback function to process each row"""
        if is_addition:
            # Convert row to dictionary
            row_dict = {
                "symbol": row["symbol"],
                "title": row["title"],
                "description": row["description"],
                "url": row["url"],
                "published_at": row["published_at"],
                "source": row["source"],
                "category": row["category"],
                "time": row["time"],
                "diff": row["diff"]
            }
            publish_row_to_kafka(row_dict)
    
    # Subscribe to the stream and process each row
    pw.io.subscribe(news_stream, on_change=process_row)
    
    try:
        # Run Pathway computation
        pw.run()
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        # Signal background polling thread to stop
        shutdown_event.set()
        
        # Flush remaining messages
        producer.flush()
        
        print(f"\nFinal Statistics:")
        print(f"   Total messages sent: {total_messages}")
        print(f"   Partition distribution: {partition_stats}")


if __name__ == "__main__":
    main()