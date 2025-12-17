"""
Market Data Streaming Script
Reads OHLCV data from CSV using Pathway, calculates SMA and VWAP,
and persists both raw data and features to MongoDB.
"""

import pathway as pw
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time
import warnings

warnings.filterwarnings("ignore")

# ==========================================
# CONFIGURATION
# ==========================================

CSV_PATH = os.getenv("CSV_PATH", "/app/data/market_data.csv")
SPEEDUP = float(os.getenv("SPEEDUP", 1.0))

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "market_db")
MONGODB_RAW_COLLECTION = os.getenv("MONGODB_RAW_COLLECTION", "ohlcv_raw")
MONGODB_FEATURES_COLLECTION = os.getenv("MONGODB_FEATURES_COLLECTION", "market_features")

# Window size in milliseconds (5 minutes = 300,000 ms)
WINDOW_SIZE_MS = int(os.getenv("WINDOW_SIZE_MS", 300000))

print("=" * 70)
print("MARKET DATA STREAMING WITH MONGODB")
print("=" * 70)
print(f"CSV Path: {CSV_PATH}")
print(f"Speedup: {SPEEDUP}x")
print(f"MongoDB URI: {MONGODB_URI}")
print(f"Database: {MONGODB_DATABASE}")
print(f"Raw Collection: {MONGODB_RAW_COLLECTION}")
print(f"Features Collection: {MONGODB_FEATURES_COLLECTION}")
print(f"Window Size: {WINDOW_SIZE_MS}ms ({WINDOW_SIZE_MS // 60000} minutes)")
print("=" * 70)
print()


# ==========================================
# MONGODB CONNECTION TEST
# ==========================================

def wait_for_mongodb(max_retries=10, retry_delay=3):
    """Wait for MongoDB to be ready before proceeding."""
    print("⏳ Waiting for MongoDB to be ready...")
    
    for attempt in range(max_retries):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            print("✅ MongoDB connection successful!")
            
            # Check if database and collections exist
            db = client[MONGODB_DATABASE]
            raw_count = db[MONGODB_RAW_COLLECTION].count_documents({})
            features_count = db[MONGODB_FEATURES_COLLECTION].count_documents({})
            print(f"📊 Current documents in {MONGODB_RAW_COLLECTION}: {raw_count}")
            print(f"📊 Current documents in {MONGODB_FEATURES_COLLECTION}: {features_count}")
            client.close()
            return True
            
        except ConnectionFailure as e:
            if attempt == max_retries - 1:
                print(f"❌ MongoDB connection failed after {max_retries} attempts: {e}")
                return False
            print(f"⏳ Connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        except Exception as e:
            print(f"❌ MongoDB error: {e}")
            if attempt == max_retries - 1:
                return False
            time.sleep(retry_delay)
    
    return False


# Wait for MongoDB before setting up pipeline
if not wait_for_mongodb():
    print("⚠️  Proceeding without confirmed MongoDB connection. Writes may fail!")

print()


# ==========================================
# PATHWAY SCHEMA
# ==========================================

class MarketDataSchema(pw.Schema):
    """Schema for market data CSV file."""
    timestamp: int      # Unix timestamp in milliseconds
    symbol: str         # Stock symbol (e.g., AAPL)
    price: float        # Price (used as close price)
    volume: int         # Trading volume
    bid: float          # Bid price
    ask: float          # Ask price
    high: float         # High price
    low: float          # Low price
    open: float         # Open price
    time: int           # Time column for replay
    diff: int           # Diff column


# ==========================================
# CUSTOM ACCUMULATORS FOR WINDOWED CALCULATIONS
# ==========================================

class SMAAccumulator(pw.BaseCustomAccumulator):
    """
    Simple Moving Average accumulator for sliding window.
    Maintains a list of prices and computes average.
    """
    def __init__(self, prices: tuple = (), timestamps: tuple = ()):
        self.prices = prices
        self.timestamps = timestamps
        self.window_size_ms = WINDOW_SIZE_MS

    @classmethod
    def from_row(cls, row):
        price = row[0]
        timestamp = row[1]
        if price is None or timestamp is None:
            return cls((), ())
        return cls((float(price),), (int(timestamp),))

    def update(self, other):
        # Combine values
        combined_prices = self.prices + other.prices
        combined_timestamps = self.timestamps + other.timestamps
        
        if not combined_timestamps:
            self.prices = ()
            self.timestamps = ()
            return
        
        # Filter to window (keep only data within window of latest timestamp)
        latest_ts = max(combined_timestamps)
        window_start = latest_ts - self.window_size_ms
        
        filtered = [(p, t) for p, t in zip(combined_prices, combined_timestamps) 
                    if t >= window_start]
        
        if filtered:
            self.prices, self.timestamps = zip(*filtered)
        else:
            self.prices = ()
            self.timestamps = ()

    def compute_result(self) -> float:
        if not self.prices:
            return 0.0
        return sum(self.prices) / len(self.prices)


class VWAPAccumulator(pw.BaseCustomAccumulator):
    """
    Volume Weighted Average Price accumulator.
    VWAP = sum(price * volume) / sum(volume)
    """
    def __init__(self, price_volume_products: tuple = (), volumes: tuple = (), timestamps: tuple = ()):
        self.price_volume_products = price_volume_products
        self.volumes = volumes
        self.timestamps = timestamps
        self.window_size_ms = WINDOW_SIZE_MS

    @classmethod
    def from_row(cls, row):
        price = row[0]
        volume = row[1]
        timestamp = row[2]
        if price is None or volume is None or timestamp is None:
            return cls((), (), ())
        pv_product = float(price) * float(volume)
        return cls((pv_product,), (float(volume),), (int(timestamp),))

    def update(self, other):
        # Combine values
        combined_pv = self.price_volume_products + other.price_volume_products
        combined_vol = self.volumes + other.volumes
        combined_ts = self.timestamps + other.timestamps
        
        if not combined_ts:
            self.price_volume_products = ()
            self.volumes = ()
            self.timestamps = ()
            return
        
        # Filter to window
        latest_ts = max(combined_ts)
        window_start = latest_ts - self.window_size_ms
        
        filtered = [(pv, v, t) for pv, v, t in zip(combined_pv, combined_vol, combined_ts)
                    if t >= window_start]
        
        if filtered:
            self.price_volume_products, self.volumes, self.timestamps = zip(*filtered)
        else:
            self.price_volume_products = ()
            self.volumes = ()
            self.timestamps = ()

    def compute_result(self) -> float:
        if not self.volumes or sum(self.volumes) == 0:
            return 0.0
        return sum(self.price_volume_products) / sum(self.volumes)


# ==========================================
# DATA PIPELINE
# ==========================================

print("📖 Reading market data from CSV...")
print(f"   Path: {CSV_PATH}")

# Read CSV with time-based replay
raw_market_data = pw.demo.replay_csv_with_time(
    path=CSV_PATH,
    schema=MarketDataSchema,
    time_column="time",
    unit="ms",
    autocommit_ms=1000,
    speedup=SPEEDUP
)

print("✅ CSV reader configured")


# ==========================================
# STATE TABLE 1: RAW OHLCV DATA
# ==========================================

# Transform raw data to OHLCV format (using price as close)
ohlcv_table = raw_market_data.select(
    timestamp=pw.this.timestamp,
    symbol=pw.this.symbol,
    open=pw.this.open,
    high=pw.this.high,
    low=pw.this.low,
    close=pw.this.price,  # Map price to close
    volume=pw.this.volume,
)

print("✅ Raw OHLCV table configured")


# ==========================================
# STATE TABLE 2: FEATURES (SMA & VWAP)
# ==========================================

# Group by symbol for per-symbol calculations
grouped_by_symbol = raw_market_data.groupby(pw.this.symbol)

# Calculate SMA and VWAP using custom accumulators
features_table = grouped_by_symbol.reduce(
    symbol=pw.this.symbol,
    timestamp=pw.reducers.latest(pw.this.timestamp),
    latest_price=pw.reducers.latest(pw.this.price),
    latest_volume=pw.reducers.latest(pw.this.volume),
    sma_5=pw.reducers.udf_reducer(SMAAccumulator)(pw.this.price, pw.this.timestamp),
    vwap_5=pw.reducers.udf_reducer(VWAPAccumulator)(pw.this.price, pw.this.volume, pw.this.timestamp),
)

# Add window metadata
features_with_metadata = features_table.with_columns(
    window_size_ms=WINDOW_SIZE_MS,
)

print("✅ Features table configured (SMA-5, VWAP-5)")


# ==========================================
# MONGODB OUTPUTS
# ==========================================

print()
print("📤 Configuring MongoDB outputs...")

# Write raw OHLCV data to MongoDB
try:
    pw.io.mongodb.write(
        ohlcv_table,
        connection_string=MONGODB_URI,
        database=MONGODB_DATABASE,
        collection=MONGODB_RAW_COLLECTION,
    )
    print(f"   ✅ Raw OHLCV → {MONGODB_DATABASE}.{MONGODB_RAW_COLLECTION}")
except Exception as e:
    print(f"   ❌ Failed to configure OHLCV MongoDB writer: {e}")

# Write features to MongoDB
try:
    pw.io.mongodb.write(
        features_with_metadata,
        connection_string=MONGODB_URI,
        database=MONGODB_DATABASE,
        collection=MONGODB_FEATURES_COLLECTION,
    )
    print(f"   ✅ Features → {MONGODB_DATABASE}.{MONGODB_FEATURES_COLLECTION}")
except Exception as e:
    print(f"   ❌ Failed to configure Features MongoDB writer: {e}")


# ==========================================
# RUN PIPELINE
# ==========================================

print()
print("=" * 70)
print("🚀 Starting market data streaming pipeline...")
print("   Press Ctrl+C to stop")
print("=" * 70)

try:
    pw.run()
except KeyboardInterrupt:
    print("\n\n⏹️  Streaming stopped by user")
except Exception as e:
    print(f"\n❌ Error during execution: {e}")
    raise
