#!/usr/bin/env python3
import os
import json
import time
import asyncio
import yfinance as yf
from datetime import datetime
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor
import hashlib

KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = {
    "stocks": os.getenv("TOPIC_STOCKS", "market-data-stocks"),
    "crypto": os.getenv("TOPIC_CRYPTO", "market-data-crypto"),
    "commodities": os.getenv("TOPIC_COMMODITIES", "market-data-commodities"),
    "forex": os.getenv("TOPIC_FOREX", "market-data-forex")
}

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 3))

STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "AMD", "INTC", "BABA"]
CRYPTO_SYMBOLS = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD", "DOGE-USD", "XRP-USD", "MATIC-USD"]
COMMODITY_SYMBOLS = ["GC=F", "CL=F", "NG=F", "SI=F", "HG=F"]
FOREX_PAIRS = ["EURUSD=X", "GBPUSD=X", "JPYUSD=X", "AUDUSD=X", "CADUSD=X"]

def create_producer():
    """Create Kafka producer with partitioning support"""
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'async-partitioned-producer',
        'compression.type': 'gzip',
        'linger.ms': 10,
        'batch.size': 16384,
        'acks': 'all',
        'enable.idempotence': True
    })

def get_partition_key(symbol, num_partitions):
    """
    Consistent hash-based partitioning
    Same symbol always goes to same partition
    """
    hash_value = int(hashlib.md5(symbol.encode()).hexdigest(), 16)
    partition = hash_value % num_partitions
    return partition

def fetch_stock_data(symbol: str):
    """Fetch real stock data for a symbol using yfinance"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        
        if hist.empty:
            return None
        
        latest = hist.iloc[-1]
        info = ticker.info
        close_price = float(latest['Close'])
        spread = close_price * 0.001
        
        partition = get_partition_key(symbol, NUM_PARTITIONS)
        
        data = {
            "timestamp": int(time.time() * 1000),
            "symbol": symbol,
            "price": round(close_price, 2),
            "volume": int(latest['Volume']),
            "bid": round(info.get('bid', close_price - spread/2), 2),
            "ask": round(info.get('ask', close_price + spread/2), 2),
            "high": round(float(latest['High']), 2),
            "low": round(float(latest['Low']), 2),
            "open": round(float(latest['Open']), 2),
            "partition": partition
        }
        
        return ("stocks", symbol, data, partition)
    except Exception as e:
        print(f"Error fetching stock {symbol}: {str(e)}")
        return None

def fetch_crypto_data(symbol: str):
    """Fetch cryptocurrency data"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        
        if hist.empty:
            return None
        
        latest = hist.iloc[-1]
        info = ticker.info
        clean_symbol = symbol.replace("-USD", "")
        
        partition = get_partition_key(clean_symbol, NUM_PARTITIONS)
        
        data = {
            "timestamp": int(time.time() * 1000),
            "symbol": clean_symbol,
            "price": round(float(latest['Close']), 2),
            "volume": float(latest['Volume']),
            "market_cap": info.get('marketCap', 0),
            "high": round(float(latest['High']), 2),
            "low": round(float(latest['Low']), 2),
            "partition": partition
        }
        
        return ("crypto", clean_symbol, data, partition)
    except Exception as e:
        print(f"Error fetching crypto {symbol}: {str(e)}")
        return None

def fetch_commodity_data(symbol: str):
    """Fetch commodity data"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        
        if hist.empty:
            return None
        
        latest = hist.iloc[-1]
        contract_month = datetime.now().strftime("%Y-%m")
        
        partition = get_partition_key(symbol, NUM_PARTITIONS)
        
        data = {
            "timestamp": int(time.time() * 1000),
            "symbol": symbol,
            "price": round(float(latest['Close']), 2),
            "volume": int(latest['Volume']),
            "contract_month": contract_month,
            "high": round(float(latest['High']), 2),
            "low": round(float(latest['Low']), 2),
            "partition": partition
        }
        
        return ("commodities", symbol, data, partition)
    except Exception as e:
        print(f"Error fetching commodity {symbol}: {str(e)}")
        return None

def fetch_forex_data(pair: str):
    """Fetch forex data"""
    try:
        ticker = yf.Ticker(pair)
        hist = ticker.history(period="1d", interval="1m")
        
        if hist.empty:
            return None
        
        latest = hist.iloc[-1]
        clean_pair = pair.replace("=X", "")
        
        partition = get_partition_key(clean_pair, NUM_PARTITIONS)
        
        data = {
            "timestamp": int(time.time() * 1000),
            "pair": clean_pair,
            "rate": round(float(latest['Close']), 5),
            "volume": float(latest['Volume']),
            "high": round(float(latest['High']), 5),
            "low": round(float(latest['Low']), 5),
            "partition": partition
        }
        
        return ("forex", clean_pair, data, partition)
    except Exception as e:
        print(f"Error fetching forex {pair}: {str(e)}")
        return None

def delivery_report(err, msg):
    """Callback for message delivery with partition info"""
    if err is not None:
        print(f'Delivery failed to {msg.topic()}: {err}')
    else:
        print(f'Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}')

async def fetch_all_data_parallel():
    """
    Fetch all market data in parallel using ThreadPoolExecutor
    This is the ASYNC part of the hybrid approach
    """
    loop = asyncio.get_event_loop()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = []
        
        for symbol in STOCK_SYMBOLS:
            tasks.append(loop.run_in_executor(executor, fetch_stock_data, symbol))
        
        for symbol in CRYPTO_SYMBOLS:
            tasks.append(loop.run_in_executor(executor, fetch_crypto_data, symbol))
        
        for symbol in COMMODITY_SYMBOLS:
            tasks.append(loop.run_in_executor(executor, fetch_commodity_data, symbol))
        
        for pair in FOREX_PAIRS:
            tasks.append(loop.run_in_executor(executor, fetch_forex_data, pair))
        
        results = await asyncio.gather(*tasks)
        
        return [r for r in results if r is not None]

def publish_results(producer, results):
    """
    Publish all fetched data to Kafka with partition routing
    This is the PARTITIONS part of the hybrid approach
    """
    partition_stats = {}
    published_count = 0
    
    for result in results:
        category, key, data, target_partition = result
        topic = KAFKA_TOPICS[category]
        
        producer.produce(
            topic,
            key=key.encode('utf-8'),
            value=json.dumps(data).encode('utf-8'),
            partition=target_partition,
            callback=delivery_report
        )
        
        published_count += 1
        
        if target_partition not in partition_stats:
            partition_stats[target_partition] = 0
        partition_stats[target_partition] += 1
        
        producer.poll(0)
    
    producer.flush()
    
    return published_count, partition_stats

async def main_async():
    """Main async event loop"""
    
    print("=" * 80)
    print("ASYNC + PARTITIONED KAFKA PRODUCER (HYBRID APPROACH)")
    print("=" * 80)
    print()
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topics:")
    for name, topic in KAFKA_TOPICS.items():
        print(f"  - {name}: {topic}")
    print()
    print(f"Configuration:")
    print(f"  - Async workers: {MAX_WORKERS}")
    print(f"  - Partitions per topic: {NUM_PARTITIONS}")
    print(f"  - Poll interval: {POLL_INTERVAL}s")
    print()
    print("Strategy:")
    print("  - ASYNC: Parallel data fetching with ThreadPoolExecutor")
    print("  - PARTITIONS: Consistent hash-based partition assignment")
    print("  - Result: Fast fetching + distributed processing")
    print()
    print("Starting producer...")
    print("Press Ctrl+C to stop")
    print()
    
    producer = create_producer()
    total_messages = 0
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            print(f"\n{'='*80}")
            print(f"CYCLE {cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}")
            
            start_time = time.time()
            
            print("\nPhase 1: ASYNC FETCHING")
            print("-" * 40)
            results = await fetch_all_data_parallel()
            fetch_time = time.time() - start_time
            
            print(f"Fetched {len(results)} items in {fetch_time:.2f}s")
            print(f"Avg time per item: {(fetch_time/len(results)*1000):.0f}ms")
            
            print("\nPhase 2: PARTITIONED PUBLISHING")
            print("-" * 40)
            
            publish_start = time.time()
            count, partition_stats = publish_results(producer, results)
            publish_time = time.time() - publish_start
            
            total_messages += count
            
            print(f"\nPublished {count} messages in {publish_time:.2f}s")
            print(f"\nPartition Distribution:")
            for partition_id in sorted(partition_stats.keys()):
                msg_count = partition_stats[partition_id]
                percentage = (msg_count / count) * 100
                bar = "█" * int(percentage / 2)
                print(f"  Partition {partition_id}: {msg_count:3d} messages {bar} ({percentage:.1f}%)")
            
            print(f"\nSession Statistics:")
            print(f"  - Total cycles: {cycle_count}")
            print(f"  - Total messages: {total_messages}")
            print(f"  - Avg messages/cycle: {total_messages/cycle_count:.1f}")
            
            total_time = time.time() - start_time
            print(f"\nTotal cycle time: {total_time:.2f}s")
            print(f"  - Fetching: {fetch_time:.2f}s ({(fetch_time/total_time)*100:.1f}%)")
            print(f"  - Publishing: {publish_time:.2f}s ({(publish_time/total_time)*100:.1f}%)")
            
            print(f"\nWaiting {POLL_INTERVAL}s before next cycle...")
            await asyncio.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
        producer.flush()
        print(f"\nFinal Statistics:")
        print(f"  - Total cycles: {cycle_count}")
        print(f"  - Total messages: {total_messages}")
        print("\nProducer stopped cleanly")

def main():
    """Entry point"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()