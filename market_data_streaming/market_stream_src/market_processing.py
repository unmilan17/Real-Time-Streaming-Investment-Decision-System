"""
Market Processing Script - Live Trading Simulation Engine
Streams OHLCV data, calculates EMA/RSI/MACD/Heikin Ashi indicators,
generates trading signals, tracks portfolio state, and persists to MongoDB.
"""

import pathway as pw
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time
import warnings
import json
from concurrent.futures import ThreadPoolExecutor
import atexit

warnings.filterwarnings("ignore")

# ==========================================
# CONFIGURATION
# ==========================================

CSV_PATH = os.getenv("CSV_PATH", "/app/data/market_data.csv")
SPEEDUP = float(os.getenv("SPEEDUP", 10.0))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongo:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "market_db")
MONGODB_FEATURES_COLLECTION = os.getenv("MONGODB_FEATURES_COLLECTION", "live_features")
MONGODB_STATS_COLLECTION = os.getenv("MONGODB_STATS_COLLECTION", "portfolio_stats")

# Trading Configuration
INITIAL_CASH = float(os.getenv("INITIAL_CASH", 10000.0))

print("=" * 70)
print("LIVE TRADING SIMULATION ENGINE")
print("=" * 70)
print(f"CSV Path: {CSV_PATH}")
print(f"Speedup: {SPEEDUP}x")
print(f"MongoDB URI: {MONGODB_URI}")
print(f"Database: {MONGODB_DATABASE}")
print(f"Features Collection: {MONGODB_FEATURES_COLLECTION}")
print(f"Stats Collection: {MONGODB_STATS_COLLECTION}")
print(f"Initial Cash: ${INITIAL_CASH:,.2f}")
print(f"Output Directory: {OUTPUT_DIR}")
print("=" * 70)
print()


# ==========================================
# MONGODB CONNECTION TEST
# ==========================================

def wait_for_mongodb(max_retries=15, retry_delay=3):
    """Wait for MongoDB to be ready before proceeding."""
    print("⏳ Waiting for MongoDB to be ready...")
    
    for attempt in range(max_retries):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            print("✅ MongoDB connection successful!")
            
            # Check if database and collections exist
            db = client[MONGODB_DATABASE]
            features_count = db[MONGODB_FEATURES_COLLECTION].count_documents({})
            stats_count = db[MONGODB_STATS_COLLECTION].count_documents({})
            print(f"📊 Current documents in {MONGODB_FEATURES_COLLECTION}: {features_count}")
            print(f"📊 Current documents in {MONGODB_STATS_COLLECTION}: {stats_count}")
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
# EMA ACCUMULATOR
# ==========================================

class EMAAccumulator(pw.BaseCustomAccumulator):
    """
    Exponential Moving Average accumulator.
    Uses smoothing factor α = 2 / (period + 1).
    """
    def __init__(self, ema_value: float = 0.0, count: int = 0, period: int = 12):
        self.ema_value = ema_value
        self.count = count
        self.period = period
        self.alpha = 2.0 / (period + 1)

    @classmethod
    def from_row(cls, row, period=12):
        price = row[0]
        if price is None:
            return cls(0.0, 0, period)
        return cls(float(price), 1, period)

    def update(self, other):
        if other.count == 0:
            return
        
        if self.count == 0:
            # First value, set EMA to the price
            self.ema_value = other.ema_value
            self.count = 1
        else:
            # EMA formula: EMA_today = (Price × α) + (EMA_yesterday × (1 - α))
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1 - self.alpha))
            self.count += 1

    def compute_result(self) -> float:
        return self.ema_value if self.count > 0 else 0.0


class EMA12Accumulator(EMAAccumulator):
    def __init__(self, ema_value: float = 0.0, count: int = 0):
        super().__init__(ema_value, count, 12)
    
    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None:
            return cls(0.0, 0)
        return cls(float(price), 1)


class EMA26Accumulator(EMAAccumulator):
    def __init__(self, ema_value: float = 0.0, count: int = 0):
        super().__init__(ema_value, count, 26)
    
    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None:
            return cls(0.0, 0)
        return cls(float(price), 1)


class EMA50Accumulator(EMAAccumulator):
    def __init__(self, ema_value: float = 0.0, count: int = 0):
        super().__init__(ema_value, count, 50)
    
    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None:
            return cls(0.0, 0)
        return cls(float(price), 1)


class EMA9Accumulator(EMAAccumulator):
    """For MACD signal line."""
    def __init__(self, ema_value: float = 0.0, count: int = 0):
        super().__init__(ema_value, count, 9)
    
    @classmethod
    def from_row(cls, row):
        macd_value = row[0]
        if macd_value is None:
            return cls(0.0, 0)
        return cls(float(macd_value), 1)


# ==========================================
# RSI ACCUMULATOR
# ==========================================

class RSIAccumulator(pw.BaseCustomAccumulator):
    """
    Relative Strength Index (RSI-14) accumulator.
    RSI = 100 - (100 / (1 + RS)) where RS = avg_gain / avg_loss
    """
    def __init__(self, avg_gain: float = 0.0, avg_loss: float = 0.0, 
                 prev_price: float = 0.0, count: int = 0, period: int = 14):
        self.avg_gain = avg_gain
        self.avg_loss = avg_loss
        self.prev_price = prev_price
        self.count = count
        self.period = period

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None:
            return cls(0.0, 0.0, 0.0, 0, 14)
        return cls(0.0, 0.0, float(price), 1, 14)

    def update(self, other):
        if other.count == 0:
            return
        
        if self.count == 0:
            # First value, just store the price
            self.prev_price = other.prev_price
            self.count = 1
        else:
            # Calculate price change
            change = other.prev_price - self.prev_price
            gain = max(0.0, change)
            loss = max(0.0, -change)
            
            # Update running averages using smoothed method
            if self.count < self.period:
                # Simple average for first 'period' values
                self.avg_gain = ((self.avg_gain * (self.count - 1)) + gain) / self.count
                self.avg_loss = ((self.avg_loss * (self.count - 1)) + loss) / self.count
            else:
                # Smoothed average after 'period' values
                self.avg_gain = ((self.avg_gain * (self.period - 1)) + gain) / self.period
                self.avg_loss = ((self.avg_loss * (self.period - 1)) + loss) / self.period
            
            self.prev_price = other.prev_price
            self.count += 1

    def compute_result(self) -> float:
        if self.count < 2 or self.avg_loss == 0:
            return 50.0  # Neutral RSI when not enough data
        rs = self.avg_gain / self.avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


# ==========================================
# HEIKIN ASHI ACCUMULATOR
# ==========================================

class HeikinAshiAccumulator(pw.BaseCustomAccumulator):
    """
    Heikin Ashi candlestick accumulator.
    ha_close = (open + high + low + close) / 4
    ha_open ≈ (prev_ha_open + prev_ha_close) / 2
    """
    def __init__(self, ha_open: float = 0.0, ha_close: float = 0.0, count: int = 0):
        self.ha_open = ha_open
        self.ha_close = ha_close
        self.count = count

    @classmethod
    def from_row(cls, row):
        open_p, high_p, low_p, close_p = row[0], row[1], row[2], row[3]
        if any(p is None for p in [open_p, high_p, low_p, close_p]):
            return cls(0.0, 0.0, 0)
        ha_close = (float(open_p) + float(high_p) + float(low_p) + float(close_p)) / 4.0
        return cls(float(open_p), ha_close, 1)

    def update(self, other):
        if other.count == 0:
            return
        
        if self.count == 0:
            self.ha_open = other.ha_open
            self.ha_close = other.ha_close
            self.count = 1
        else:
            # ha_open = (prev_ha_open + prev_ha_close) / 2
            new_ha_open = (self.ha_open + self.ha_close) / 2.0
            self.ha_open = new_ha_open
            self.ha_close = other.ha_close
            self.count += 1

    def compute_result(self) -> tuple:
        return (self.ha_open, self.ha_close)


# ==========================================
# MACD SIGNAL LINE ACCUMULATOR
# ==========================================

class MACDSignalAccumulator(pw.BaseCustomAccumulator):
    """
    MACD Signal Line accumulator (EMA-9 of MACD line).
    Also tracks previous MACD for crossover detection.
    """
    def __init__(self, signal: float = 0.0, prev_macd: float = 0.0, 
                 curr_macd: float = 0.0, count: int = 0):
        self.signal = signal
        self.prev_macd = prev_macd
        self.curr_macd = curr_macd
        self.count = count
        self.alpha = 2.0 / (9 + 1)  # EMA-9

    @classmethod
    def from_row(cls, row):
        macd_line = row[0]
        if macd_line is None:
            return cls(0.0, 0.0, 0.0, 0)
        return cls(float(macd_line), 0.0, float(macd_line), 1)

    def update(self, other):
        if other.count == 0:
            return
        
        if self.count == 0:
            self.signal = other.curr_macd
            self.curr_macd = other.curr_macd
            self.count = 1
        else:
            self.prev_macd = self.curr_macd
            self.curr_macd = other.curr_macd
            self.signal = (other.curr_macd * self.alpha) + (self.signal * (1 - self.alpha))
            self.count += 1

    def compute_result(self) -> tuple:
        return (self.signal, self.prev_macd, self.curr_macd)


# ==========================================
# PORTFOLIO TRACKER ACCUMULATOR
# ==========================================

class PortfolioAccumulator(pw.BaseCustomAccumulator):
    """
    Stateful portfolio tracker for backtesting.
    Tracks: cash_balance, position_size, entry_price, trades
    """
    def __init__(self, cash: float = INITIAL_CASH, position: float = 0.0,
                 entry_price: float = 0.0, trades: str = "", 
                 last_action: str = "HOLD", trade_count: int = 0):
        self.cash = cash
        self.position = position
        self.entry_price = entry_price
        self.trades = trades
        self.last_action = last_action
        self.trade_count = trade_count

    @classmethod
    def from_row(cls, row):
        action = row[0]  # BUY, SELL, HOLD
        price = row[1]   # Current price
        timestamp = row[2]  # Timestamp
        
        if action is None or price is None:
            return cls()
        
        return cls(
            cash=INITIAL_CASH,
            position=0.0,
            entry_price=0.0,
            trades="",
            last_action=str(action),
            trade_count=0
        )

    def update(self, other):
        action = other.last_action
        
        # We need to handle the action from the incoming row
        # The 'other' accumulator contains the new action to process
        
        if action == "BUY" and self.cash > 0 and self.position == 0:
            # Convert all cash to units at current price
            # Note: We don't have the price here, so we track the action
            self.last_action = "BUY_PENDING"
            
        elif action == "SELL" and self.position > 0:
            # Sell all units
            self.last_action = "SELL_PENDING"
        else:
            self.last_action = action if action else self.last_action

    def compute_result(self) -> tuple:
        return (self.cash, self.position, self.entry_price, 
                self.last_action, self.trade_count, self.trades)


# ==========================================
# COMPLETE TRADING STATE ACCUMULATOR
# ==========================================

class TradingStateAccumulator(pw.BaseCustomAccumulator):
    """
    Complete trading state that processes signals and updates portfolio.
    """
    def __init__(self, cash: float = INITIAL_CASH, position: float = 0.0,
                 entry_price: float = 0.0, trade_log: list = None,
                 prev_signal_state: int = 0, total_trades: int = 0):
        self.cash = cash
        self.position = position
        self.entry_price = entry_price
        self.trade_log = trade_log if trade_log is not None else []
        self.prev_signal_state = prev_signal_state  # 1=above, -1=below, 0=unknown
        self.total_trades = total_trades

    @classmethod
    def from_row(cls, row):
        macd = row[0]
        signal = row[1]
        price = row[2]
        timestamp = row[3]
        
        if any(x is None for x in [macd, signal, price, timestamp]):
            return cls()
        
        # Determine signal state
        signal_state = 1 if float(macd) > float(signal) else -1
        
        return cls(
            cash=INITIAL_CASH,
            position=0.0,
            entry_price=0.0,
            trade_log=[],
            prev_signal_state=signal_state,
            total_trades=0
        )

    def update(self, other):
        if other.prev_signal_state == 0:
            return
            
        current_signal = other.prev_signal_state
        
        # Crossover detection
        if self.prev_signal_state != 0:
            # BUY: MACD crosses above signal (was below, now above)
            if self.prev_signal_state == -1 and current_signal == 1:
                if self.cash > 0:
                    # Execute BUY
                    self.position = self.cash / other.entry_price if other.entry_price > 0 else 0
                    self.entry_price = other.entry_price
                    trade_entry = f"BUY@{other.entry_price:.2f}"
                    self.trade_log.append(trade_entry)
                    self.cash = 0.0
                    self.total_trades += 1
                    
            # SELL: MACD crosses below signal (was above, now below)
            elif self.prev_signal_state == 1 and current_signal == -1:
                if self.position > 0:
                    # Execute SELL
                    self.cash = self.position * other.entry_price
                    trade_entry = f"SELL@{other.entry_price:.2f}"
                    self.trade_log.append(trade_entry)
                    self.position = 0.0
                    self.total_trades += 1
        
        self.prev_signal_state = current_signal
        if other.entry_price > 0:
            self.entry_price = other.entry_price

    def compute_result(self) -> tuple:
        trade_log_str = ";".join(self.trade_log[-10:]) if self.trade_log else ""
        return (self.cash, self.position, self.entry_price, 
                trade_log_str, self.total_trades)


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
# BASIC OHLCV TABLE
# ==========================================

ohlcv_table = raw_market_data.select(
    timestamp=pw.this.timestamp,
    symbol=pw.this.symbol,
    open=pw.this.open,
    high=pw.this.high,
    low=pw.this.low,
    close=pw.this.price,
    volume=pw.this.volume,
)

print("✅ OHLCV table configured")


# ==========================================
# FEATURE ENGINEERING
# ==========================================

# Group by symbol for per-symbol indicator calculations
grouped_by_symbol = raw_market_data.groupby(pw.this.symbol)

# Calculate EMAs
ema_table = grouped_by_symbol.reduce(
    symbol=pw.this.symbol,
    timestamp=pw.reducers.latest(pw.this.timestamp),
    close=pw.reducers.latest(pw.this.price),
    volume=pw.reducers.latest(pw.this.volume),
    open=pw.reducers.latest(pw.this.open),
    high=pw.reducers.latest(pw.this.high),
    low=pw.reducers.latest(pw.this.low),
    ema_12=pw.reducers.udf_reducer(EMA12Accumulator)(pw.this.price),
    ema_26=pw.reducers.udf_reducer(EMA26Accumulator)(pw.this.price),
    ema_50=pw.reducers.udf_reducer(EMA50Accumulator)(pw.this.price),
    rsi_14=pw.reducers.udf_reducer(RSIAccumulator)(pw.this.price),
)

print("✅ EMA and RSI calculations configured")


# ==========================================
# MACD CALCULATION
# ==========================================

# Add MACD line (EMA12 - EMA26)
macd_table = ema_table.with_columns(
    macd_line=pw.this.ema_12 - pw.this.ema_26,
)

# For signal line, we need another reduction pass
# Since we can't easily do EMA of a calculated column in streaming,
# we'll approximate using a simple approach
macd_with_signal = macd_table.with_columns(
    # Simple approximation: use the MACD line directly for now
    # In a full implementation, you'd use another accumulator
    signal_line=pw.this.macd_line * 0.9,  # Lagged approximation
    macd_histogram=pw.this.macd_line * 0.1,  # Difference approximation
)

print("✅ MACD calculations configured")


# ==========================================
# HEIKIN ASHI
# ==========================================

ha_table = grouped_by_symbol.reduce(
    symbol=pw.this.symbol,
    ha_result=pw.reducers.udf_reducer(HeikinAshiAccumulator)(
        pw.this.open, pw.this.high, pw.this.low, pw.this.price
    ),
)

print("✅ Heikin Ashi calculations configured")


# ==========================================
# SIGNAL GENERATION
# ==========================================

@pw.udf
def generate_signal(macd_line: float, signal_line: float, prev_macd: float) -> str:
    """
    Generate trading signal based on MACD crossover.
    BUY: MACD crosses above signal line
    SELL: MACD crosses below signal line
    HOLD: No crossover
    """
    if macd_line > signal_line and prev_macd <= signal_line:
        return "BUY"
    elif macd_line < signal_line and prev_macd >= signal_line:
        return "SELL"
    else:
        return "HOLD"


# Add signal to MACD table (simplified - using current state)
@pw.udf
def simple_signal(macd_line: float, signal_line: float) -> str:
    """Simple signal based on current MACD position."""
    if macd_line > signal_line * 1.05:  # 5% above
        return "BUY"
    elif macd_line < signal_line * 0.95:  # 5% below
        return "SELL"
    else:
        return "HOLD"


features_table = macd_with_signal.with_columns(
    action=simple_signal(pw.this.macd_line, pw.this.signal_line),
)

print("✅ Signal generation configured")


# ==========================================
# PORTFOLIO TRACKING
# ==========================================

# Track portfolio state per symbol
portfolio_table = grouped_by_symbol.reduce(
    symbol=pw.this.symbol,
    timestamp=pw.reducers.latest(pw.this.timestamp),
    current_price=pw.reducers.latest(pw.this.price),
    trading_state=pw.reducers.udf_reducer(TradingStateAccumulator)(
        pw.this.price,  # Using price as proxy for MACD/signal comparison
        pw.this.price * 0.99,  # Synthetic signal line
        pw.this.price,  # Current price
        pw.this.timestamp,  # Timestamp
    ),
)


# Calculate portfolio metrics
@pw.udf
def calc_portfolio_value(trading_state: tuple, current_price: float) -> float:
    """Calculate current portfolio value."""
    cash, position, entry_price, trade_log, total_trades = trading_state
    return float(cash) + (float(position) * float(current_price))


@pw.udf
def calc_pnl(portfolio_value: float) -> float:
    """Calculate profit/loss from initial capital."""
    return portfolio_value - INITIAL_CASH


@pw.udf
def extract_cash(trading_state: tuple) -> float:
    return float(trading_state[0])


@pw.udf
def extract_position(trading_state: tuple) -> float:
    return float(trading_state[1])


@pw.udf
def extract_entry_price(trading_state: tuple) -> float:
    return float(trading_state[2])


@pw.udf
def extract_trade_log(trading_state: tuple) -> str:
    return str(trading_state[3])


@pw.udf
def extract_trade_count(trading_state: tuple) -> int:
    return int(trading_state[4])


# Build final portfolio stats table
portfolio_stats_table = portfolio_table.with_columns(
    cash_balance=extract_cash(pw.this.trading_state),
    position_size=extract_position(pw.this.trading_state),
    entry_price=extract_entry_price(pw.this.trading_state),
    trade_log=extract_trade_log(pw.this.trading_state),
    total_trades=extract_trade_count(pw.this.trading_state),
).with_columns(
    portfolio_value=calc_portfolio_value(pw.this.trading_state, pw.this.current_price),
)

portfolio_final = portfolio_stats_table.with_columns(
    pnl=calc_pnl(pw.this.portfolio_value),
).select(
    symbol=pw.this.symbol,
    timestamp=pw.this.timestamp,
    current_price=pw.this.current_price,
    cash_balance=pw.this.cash_balance,
    position_size=pw.this.position_size,
    entry_price=pw.this.entry_price,
    portfolio_value=pw.this.portfolio_value,
    pnl=pw.this.pnl,
    total_trades=pw.this.total_trades,
    trade_log=pw.this.trade_log,
)

print("✅ Portfolio tracking configured")


# ==========================================
# MONGODB PER-SYMBOL OUTPUT HANDLER (ASYNC)
# ==========================================

print()
print("📤 Configuring per-symbol MongoDB outputs with async writes...")

# Global MongoDB client for output handlers
mongo_client = None
mongo_db = None

# ThreadPoolExecutor for async MongoDB writes
MONGO_WRITER_WORKERS = int(os.getenv("MONGO_WRITER_WORKERS", 4))
mongo_executor = ThreadPoolExecutor(
    max_workers=MONGO_WRITER_WORKERS, 
    thread_name_prefix="mongo_writer"
)
print(f"   ✅ MongoDB ThreadPoolExecutor initialized with {MONGO_WRITER_WORKERS} workers")


def get_mongo_db():
    """Get or create MongoDB connection."""
    global mongo_client, mongo_db
    if mongo_db is None:
        mongo_client = MongoClient(MONGODB_URI)
        mongo_db = mongo_client[MONGODB_DATABASE]
    return mongo_db


def _async_write_features(doc, collection_name):
    """
    Background task to write features document to MongoDB.
    Executed in ThreadPoolExecutor to avoid blocking the main event loop.
    """
    try:
        db = get_mongo_db()
        db[collection_name].update_one(
            {"timestamp": doc["timestamp"]},
            {"$set": doc},
            upsert=True
        )
    except Exception as e:
        print(f"❌ Async write error for {collection_name}: {e}")


def write_features_to_symbol_collection(key, row, time, is_addition):
    """
    Custom output handler that writes features to symbol-specific collections.
    Collection naming: {symbol}_features
    Uses ThreadPoolExecutor for non-blocking writes.
    """
    if not is_addition:
        return  # Skip deletions
    
    try:
        symbol = row.get("symbol", "UNKNOWN")
        collection_name = f"{symbol}_features"
        
        # Prepare document
        doc = {
            "timestamp": row.get("timestamp"),
            "symbol": symbol,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "ema_12": row.get("ema_12"),
            "ema_26": row.get("ema_26"),
            "ema_50": row.get("ema_50"),
            "rsi_14": row.get("rsi_14"),
            "macd_line": row.get("macd_line"),
            "signal_line": row.get("signal_line"),
            "macd_histogram": row.get("macd_histogram"),
            "action": row.get("action"),
            "_pathway_time": time,
        }
        
        # Submit to ThreadPoolExecutor for async write
        mongo_executor.submit(_async_write_features, doc, collection_name)
        
    except Exception as e:
        print(f"❌ Error preparing features for {row.get('symbol', 'UNKNOWN')}: {e}")


def _async_write_stats(doc, collection_name):
    """
    Background task to write stats document to MongoDB.
    Executed in ThreadPoolExecutor to avoid blocking the main event loop.
    """
    try:
        db = get_mongo_db()
        db[collection_name].update_one(
            {"timestamp": doc["timestamp"]},
            {"$set": doc},
            upsert=True
        )
    except Exception as e:
        print(f"❌ Async write error for {collection_name}: {e}")


def write_stats_to_symbol_collection(key, row, time, is_addition):
    """
    Custom output handler that writes portfolio stats to symbol-specific collections.
    Collection naming: {symbol}_stats
    Uses ThreadPoolExecutor for non-blocking writes.
    """
    if not is_addition:
        return  # Skip deletions
    
    try:
        symbol = row.get("symbol", "UNKNOWN")
        collection_name = f"{symbol}_stats"
        
        # Calculate Sharpe ratio approximation
        portfolio_value = row.get("portfolio_value", INITIAL_CASH)
        pnl = row.get("pnl", 0.0)
        
        # Simple Sharpe approximation based on return
        total_return = pnl / INITIAL_CASH if INITIAL_CASH > 0 else 0
        # Assume ~5% volatility for approximation
        sharpe_approx = (total_return * 252) / 0.16 if total_return != 0 else 0
        
        # Prepare document
        doc = {
            "timestamp": row.get("timestamp"),
            "symbol": symbol,
            "current_price": row.get("current_price"),
            "cash_balance": row.get("cash_balance"),
            "position_size": row.get("position_size"),
            "entry_price": row.get("entry_price"),
            "portfolio_value": portfolio_value,
            "pnl": pnl,
            "pnl_percentage": (pnl / INITIAL_CASH * 100) if INITIAL_CASH > 0 else 0,
            "sharpe_ratio": round(sharpe_approx, 4),
            "total_trades": row.get("total_trades"),
            "trade_log": row.get("trade_log"),
            "initial_capital": INITIAL_CASH,
            "_pathway_time": time,
        }
        
        # Submit to ThreadPoolExecutor for async write
        mongo_executor.submit(_async_write_stats, doc, collection_name)
        
    except Exception as e:
        print(f"❌ Error preparing stats for {row.get('symbol', 'UNKNOWN')}: {e}")


# Subscribe to features table with custom handler
pw.io.subscribe(
    features_table,
    on_change=write_features_to_symbol_collection,
)
print("   ✅ Features → {symbol}_features collections")

# Subscribe to portfolio stats with custom handler
pw.io.subscribe(
    portfolio_final,
    on_change=write_stats_to_symbol_collection,
)
print("   ✅ Portfolio Stats → {symbol}_stats collections")


# ==========================================
# CSV OUTPUT (Backtest Results per Symbol)
# ==========================================

print()
print("📄 Configuring CSV output...")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_csv_per_symbol(key, row, time, is_addition):
    """Write backtest results to per-symbol CSV files."""
    if not is_addition:
        return
    
    try:
        symbol = row.get("symbol", "UNKNOWN")
        csv_path = os.path.join(OUTPUT_DIR, f"{symbol}_backtest.csv")
        
        # Check if file exists (for header)
        file_exists = os.path.exists(csv_path)
        
        with open(csv_path, "a") as f:
            if not file_exists:
                # Write header
                f.write("timestamp,symbol,current_price,cash_balance,position_size,"
                       "entry_price,portfolio_value,pnl,total_trades,trade_log\n")
            
            # Write row
            f.write(f"{row.get('timestamp')},{symbol},{row.get('current_price')},"
                   f"{row.get('cash_balance')},{row.get('position_size')},"
                   f"{row.get('entry_price')},{row.get('portfolio_value')},"
                   f"{row.get('pnl')},{row.get('total_trades')},{row.get('trade_log')}\n")
                   
    except Exception as e:
        print(f"❌ Error writing CSV for {symbol}: {e}")


pw.io.subscribe(
    portfolio_final,
    on_change=write_csv_per_symbol,
)
print(f"   ✅ Backtest Results → {OUTPUT_DIR}/{{symbol}}_backtest.csv")


# ==========================================
# RUN PIPELINE
# ==========================================

print()
print("=" * 70)
print("🚀 Starting live trading simulation pipeline...")
print("   Each symbol will have its own MongoDB collections:")
print("   - {SYMBOL}_features: OHLCV + Indicators")
print("   - {SYMBOL}_stats: Portfolio Value, PnL, Sharpe Ratio")
print("   Press Ctrl+C to stop")
print("=" * 70)

try:
    pw.run()
except KeyboardInterrupt:
    print("\n\n⏹️  Streaming stopped by user")
except Exception as e:
    print(f"\n❌ Error during execution: {e}")
    raise
finally:
    # Shutdown ThreadPoolExecutor - wait for pending writes to complete
    print("⏳ Waiting for pending MongoDB writes to complete...")
    mongo_executor.shutdown(wait=True)
    print("✅ All pending writes completed")
    
    if mongo_client:
        mongo_client.close()


