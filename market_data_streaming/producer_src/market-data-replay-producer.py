#!/usr/bin/env python3
import os
import json
import pathway as pw
from confluent_kafka import Producer
import threading
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
import signal
import warnings

warnings.filterwarnings("ignore")

# ---------- Config ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("TOPIC_NAME", "market-data-stocks")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
CSV_PATH = os.getenv("CSV_PATH", "/app/data/market_data.csv")
SPEEDUP = float(os.getenv("SPEEDUP", 1.0))
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "3"))

# ---------- Utilities ----------
def create_producer(client_id: str = "market-data-producer"):
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': client_id,
        'compression.type': 'gzip',
        'linger.ms': 10,
        'batch.size': 16384,
        'acks': 'all',
        'enable.idempotence': True
    })

def get_partition_key(symbol: str, num_partitions: int = NUM_PARTITIONS) -> int:
    hv = int(hashlib.md5(symbol.encode()).hexdigest(), 16)
    return hv % num_partitions

# ---------- Pathway Schema ----------
class MarketDataSchema(pw.Schema):
    timestamp: int
    symbol: str
    price: float
    volume: int
    bid: float
    ask: float
    high: float
    low: float
    open: float
    time: int
    diff: int

# ---------- Global objects ----------
producer = create_producer()
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
producer_polling_running = threading.Event()
producer_polling_running.set()

# Stats
partition_stats = {}
total_messages = 0
stats_lock = threading.Lock()

# ---------- Producer delivery callback ----------
def delivery_callback(err, msg):
    if err:
        print(f"[delivery] Failed: {err}")

# ---------- Background thread to poll producer ----------
def producer_poller_loop(interval_sec: float = 0.1):
    try:
        while producer_polling_running.is_set():
            producer.poll(0.1)
            time.sleep(interval_sec)
    except Exception as e:
        print("Producer poller exception:", e)

poller_thread = threading.Thread(target=producer_poller_loop, name="producer-poller", daemon=True)
poller_thread.start()

# ---------- Publish function ----------
def publish_row_to_kafka(row_dict):
    global total_messages, partition_stats

    try:
        symbol = row_dict.get("symbol", "") or ""

        # Build payload
        message_value = {
            "timestamp": row_dict.get("timestamp"),
            "symbol": symbol,
            "price": row_dict.get("price"),
            "volume": row_dict.get("volume"),
            "bid": row_dict.get("bid"),
            "ask": row_dict.get("ask"),
            "high": row_dict.get("high"),
            "low": row_dict.get("low"),
            "open": row_dict.get("open"),
            "time": row_dict.get("time"),
            "diff": row_dict.get("diff"),
        }

        partition = get_partition_key(symbol, NUM_PARTITIONS)
        
        # Add partition to the message value
        message_value["partition"] = partition

        # Produce to Kafka
        producer.produce(
            topic=KAFKA_TOPIC,
            key=symbol.encode("utf-8"),
            value=json.dumps(message_value).encode("utf-8"),
            partition=int(partition),
            callback=delivery_callback
        )

        # Update stats
        with stats_lock:
            partition_stats[partition] = partition_stats.get(partition, 0) + 1
            total_messages += 1

    except Exception as e:
        print("publish_row_to_kafka error:", e)

# ---------- Pathway subscribe callback ----------
def process_row(key, row, time_ts, is_addition):
    if not is_addition:
        return

    # Convert Pathway row to dict
    row_dict = {
        "timestamp": row["timestamp"],
        "symbol": row["symbol"],
        "price": row["price"],
        "volume": row["volume"],
        "bid": row["bid"],
        "ask": row["ask"],
        "high": row["high"],
        "low": row["low"],
        "open": row["open"],
        "time": row["time"],
        "diff": row["diff"]
    }

    # Submit to threadpool
    try:
        executor.submit(publish_row_to_kafka, row_dict)
    except RuntimeError as e:
        print("Executor submit failed, falling back to inline publish:", e)
        publish_row_to_kafka(row_dict)

# ---------- Main ----------
def main():
    print("=" * 80)
    print("PATHWAY MARKET DATA CSV REPLAY → KAFKA")
    print("=" * 80)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"CSV path: {CSV_PATH}")
    print(f"Speedup: {SPEEDUP}x")
    print(f"Max workers: {MAX_WORKERS}")
    print(f"Partitions: {NUM_PARTITIONS}")
    print("=" * 80)
    print()

    # Create Pathway replay stream
    market_stream = pw.demo.replay_csv_with_time(
        path=CSV_PATH,
        schema=MarketDataSchema,
        time_column="time",
        unit="ms",
        autocommit_ms=100,
        speedup=SPEEDUP
    )

    # Subscribe
    pw.io.subscribe(market_stream, on_change=process_row)

    # Signal handlers
    def shutdown(signum=None, frame=None):
        print("\nShutdown requested. Stopping...")
        try:
            pw.shutdown()
        except Exception:
            pass
        producer_polling_running.clear()
        executor.shutdown(wait=True)
        try:
            producer.flush(10)
        except Exception:
            pass
        print("Shutdown complete.")
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Run Pathway
    try:
        pw.run()
    except KeyboardInterrupt:
        pass
    finally:
        producer_polling_running.clear()
        executor.shutdown(wait=True)
        try:
            producer.flush(10)
        except Exception:
            pass

        # Final stats
        print("\nFinal Statistics:")
        with stats_lock:
            print(f"  Total messages sent: {total_messages}")
            print(f"  Partition distribution: {partition_stats}")

if __name__ == "__main__":
    main()