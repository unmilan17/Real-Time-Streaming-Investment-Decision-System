import pathway as pw
import os
import json
import boto3
from datetime import datetime
import numpy as np
import warnings

warnings.filterwarnings("ignore")

# Environment variables
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC_NAME", "market-data-stocks")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/consumer_output")
CONSUMER_ID = os.getenv("CONSUMER_ID", "consumer-default")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "stocks-group")

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "market-data-bucket")
S3_PREFIX = os.getenv("S3_PREFIX", "raw-data")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/features", exist_ok=True)

# Initialize S3 client
s3_client = None
if AWS_ACCESS_KEY and AWS_SECRET_KEY:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    print(f"✓ S3 client initialized for bucket: {S3_BUCKET}")
else:
    print("⚠️  S3 credentials not provided. S3 storage disabled.")

# Schema
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
    partition: int
    time: int
    diff: int

print("=" * 70)
print(f"STATEFUL REAL-TIME FEATURE CALCULATOR + S3 STORAGE: {CONSUMER_ID}")
print("=" * 70)

# ==========================================
# STATEFUL CUSTOM ACCUMULATORS - Module Level
# ==========================================

class EMA6Accumulator(pw.BaseCustomAccumulator):
    """EMA-6 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 6
        self.alpha = 2.0 / 7.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA10Accumulator(pw.BaseCustomAccumulator):
    """EMA-10 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 10
        self.alpha = 2.0 / 11.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA12Accumulator(pw.BaseCustomAccumulator):
    """EMA-12 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 12
        self.alpha = 2.0 / 13.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA13Accumulator(pw.BaseCustomAccumulator):
    """EMA-13 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 13
        self.alpha = 2.0 / 14.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA20Accumulator(pw.BaseCustomAccumulator):
    """EMA-20 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 20
        self.alpha = 2.0 / 21.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA26Accumulator(pw.BaseCustomAccumulator):
    """EMA-26 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 26
        self.alpha = 2.0 / 27.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA50Accumulator(pw.BaseCustomAccumulator):
    """EMA-50 accumulator"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 50
        self.alpha = 2.0 / 51.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None or np.isnan(price):
            return cls(0.0, False)
        return cls(float(price), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class EMA9Accumulator(pw.BaseCustomAccumulator):
    """EMA-9 accumulator for MACD signal"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 9
        self.alpha = 2.0 / 10.0
        self.ema_value = ema_value
        self.is_init = is_init

    @classmethod
    def from_row(cls, row):
        value = row[0]
        if value is None or np.isnan(value):
            return cls(0.0, False)
        return cls(float(value), True)

    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))

    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0

class RSIAccumulator(pw.BaseCustomAccumulator):
    """RSI-14 accumulator"""
    def __init__(self, last_price: float = None, avg_gain: float = 0.0, 
                 avg_loss: float = 0.0, counter: int = 0, is_seeded: bool = False):
        self.period = 14
        self.alpha = 1.0 / 14.0
        self.last_price = last_price
        self.avg_gain = avg_gain
        self.avg_loss = avg_loss
        self.counter = counter
        self.is_seeded = is_seeded

    @classmethod
    def from_row(cls, row):
        price = row[0]
        if price is None:
            return cls()
        return cls(last_price=float(price))

    def update(self, other):
        if other.last_price is None:
            return
        
        if self.last_price is not None:
            delta = other.last_price - self.last_price
            gain = max(delta, 0.0)
            loss = max(-delta, 0.0)

            if not self.is_seeded:
                self.avg_gain += gain
                self.avg_loss += loss
                self.counter += 1
                if self.counter >= self.period:
                    self.avg_gain /= self.period
                    self.avg_loss /= self.period
                    self.is_seeded = True
            else:
                self.avg_gain = (gain * self.alpha) + (self.avg_gain * (1.0 - self.alpha))
                self.avg_loss = (loss * self.alpha) + (self.avg_loss * (1.0 - self.alpha))
        
        self.last_price = other.last_price

    def compute_result(self) -> float:
        if not self.is_seeded:
            return 50.0
        if self.avg_loss == 0:
            return 100.0
        rs = self.avg_gain / self.avg_loss
        return float(100.0 - (100.0 / (1.0 + rs)))

class SMA5Accumulator(pw.BaseCustomAccumulator):
    """SMA-5 accumulator for RSI smoothing"""
    def __init__(self, values: tuple = ()):
        self.period = 5
        self.values = values

    @classmethod
    def from_row(cls, row):
        value = row[0]
        if value is None or np.isnan(value):
            return cls(())
        return cls((float(value),))

    def update(self, other):
        combined = self.values + other.values
        if len(combined) > self.period:
            self.values = combined[-self.period:]
        else:
            self.values = combined

    def compute_result(self) -> float:
        if len(self.values) < self.period:
            return 0.0
        return float(sum(self.values) / len(self.values))


class MACDSignalAccumulator(pw.BaseCustomAccumulator):
    """Stateful EMA-9 for MACD signal - processes the full row"""
    def __init__(self, ema_value: float = 0.0, is_init: bool = False):
        self.span = 9
        self.alpha = 2.0 / 10.0
        self.ema_value = ema_value
        self.is_init = is_init
    
    @classmethod
    def from_row(cls, row):
        # Extract MACD value from the row
        macd = row[0]
        if macd is None or np.isnan(macd):
            return cls(0.0, False)
        return cls(float(macd), True)
    
    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))
    
    def compute_result(self) -> float:
        return float(self.ema_value) if self.is_init else 0.0


class RSISmoothedAccumulator(pw.BaseCustomAccumulator):
    """Stateful SMA-5 for RSI smoothing - processes the full row"""
    def __init__(self, values: tuple = ()):
        self.period = 5
        self.values = values
    
    @classmethod
    def from_row(cls, row):
        rsi = row[0]
        if rsi is None or np.isnan(rsi):
            return cls(())
        return cls((float(rsi),))
    
    def update(self, other):
        combined = self.values + other.values
        if len(combined) > self.period:
            self.values = combined[-self.period:]
        else:
            self.values = combined
    
    def compute_result(self) -> float:
        if len(self.values) < self.period:
            return 0.0
        return float(sum(self.values) / len(self.values))


class HeikenAshiAccumulator(pw.BaseCustomAccumulator):
    """Heiken Ashi accumulator - tracks previous HA values for proper calculation"""
    def __init__(self, prev_ha_open: float = 0.0, prev_ha_close: float = 0.0,
                 current_open: float = 0.0, current_high: float = 0.0,
                 current_low: float = 0.0, current_close: float = 0.0,
                 is_init: bool = False):
        self.prev_ha_open = prev_ha_open
        self.prev_ha_close = prev_ha_close
        self.current_open = current_open
        self.current_high = current_high
        self.current_low = current_low
        self.current_close = current_close
        self.is_init = is_init
    
    @classmethod
    def from_row(cls, row):
        open_p = row[0]
        high_p = row[1]
        low_p = row[2]
        close_p = row[3]
        if any(x is None for x in [open_p, high_p, low_p, close_p]):
            return cls()
        return cls(0.0, 0.0, float(open_p), float(high_p), float(low_p), float(close_p), True)
    
    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            # First candle: HA_Open = (Open + Close) / 2
            self.prev_ha_open = (other.current_open + other.current_close) / 2.0
            self.prev_ha_close = (other.current_open + other.current_high + 
                                  other.current_low + other.current_close) / 4.0
            self.is_init = True
        else:
            # Update previous HA values for next calculation
            self.prev_ha_open = (self.prev_ha_open + self.prev_ha_close) / 2.0
            self.prev_ha_close = (other.current_open + other.current_high + 
                                  other.current_low + other.current_close) / 4.0
        self.current_open = other.current_open
        self.current_high = other.current_high
        self.current_low = other.current_low
        self.current_close = other.current_close
    
    def compute_result(self) -> tuple:
        if not self.is_init:
            return (0.0, 0.0, 0.0, 0.0)
        ha_close = (self.current_open + self.current_high + 
                    self.current_low + self.current_close) / 4.0
        ha_open = (self.prev_ha_open + self.prev_ha_close) / 2.0
        ha_high = max(self.current_high, ha_open, ha_close)
        ha_low = min(self.current_low, ha_open, ha_close)
        return (ha_open, ha_close, ha_high, ha_low)


class DMIAccumulator(pw.BaseCustomAccumulator):
    """Directional Movement Index accumulator - calculates +DI and -DI"""
    def __init__(self, period: int = 14, prev_high: float = None, prev_low: float = None,
                 prev_close: float = None, smooth_plus_dm: float = 0.0,
                 smooth_minus_dm: float = 0.0, smooth_tr: float = 0.0,
                 counter: int = 0, is_seeded: bool = False):
        self.period = period
        self.prev_high = prev_high
        self.prev_low = prev_low
        self.prev_close = prev_close
        self.smooth_plus_dm = smooth_plus_dm
        self.smooth_minus_dm = smooth_minus_dm
        self.smooth_tr = smooth_tr
        self.counter = counter
        self.is_seeded = is_seeded
    
    @classmethod
    def from_row(cls, row):
        high = row[0]
        low = row[1]
        close = row[2]
        period = row[3] if len(row) > 3 else 14
        if any(x is None for x in [high, low, close]):
            return cls(period=period)
        return cls(period=period, prev_high=float(high), prev_low=float(low), prev_close=float(close))
    
    def update(self, other):
        if other.prev_high is None:
            return
        
        if self.prev_high is not None:
            # Calculate directional movement
            up_move = other.prev_high - self.prev_high
            down_move = self.prev_low - other.prev_low
            
            plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
            minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
            
            # True Range
            tr = max(
                other.prev_high - other.prev_low,
                abs(other.prev_high - self.prev_close) if self.prev_close else 0,
                abs(other.prev_low - self.prev_close) if self.prev_close else 0
            )
            
            if not self.is_seeded:
                self.smooth_plus_dm += plus_dm
                self.smooth_minus_dm += minus_dm
                self.smooth_tr += tr
                self.counter += 1
                if self.counter >= self.period:
                    self.is_seeded = True
            else:
                # Wilder smoothing
                alpha = 1.0 / self.period
                self.smooth_plus_dm = self.smooth_plus_dm * (1 - alpha) + plus_dm
                self.smooth_minus_dm = self.smooth_minus_dm * (1 - alpha) + minus_dm
                self.smooth_tr = self.smooth_tr * (1 - alpha) + tr
        
        self.prev_high = other.prev_high
        self.prev_low = other.prev_low
        self.prev_close = other.prev_close
    
    def compute_result(self) -> tuple:
        if not self.is_seeded or self.smooth_tr == 0:
            return (0.0, 0.0)
        plus_di = 100.0 * self.smooth_plus_dm / self.smooth_tr
        minus_di = 100.0 * self.smooth_minus_dm / self.smooth_tr
        return (plus_di, minus_di)


class DMI63Accumulator(DMIAccumulator):
    """DMI with 63-period lookback for longer-term trend"""
    def __init__(self, **kwargs):
        super().__init__(period=63, **kwargs)
    
    @classmethod
    def from_row(cls, row):
        high = row[0]
        low = row[1]
        close = row[2]
        if any(x is None for x in [high, low, close]):
            return cls()
        return cls(prev_high=float(high), prev_low=float(low), prev_close=float(close))


class AroonAccumulator(pw.BaseCustomAccumulator):
    """Aroon Oscillator accumulator - 25 period default"""
    def __init__(self, period: int = 25, highs: tuple = (), lows: tuple = ()):
        self.period = period
        self.highs = highs
        self.lows = lows
    
    @classmethod
    def from_row(cls, row):
        high = row[0]
        low = row[1]
        if high is None or low is None:
            return cls()
        return cls(highs=(float(high),), lows=(float(low),))
    
    def update(self, other):
        combined_highs = self.highs + other.highs
        combined_lows = self.lows + other.lows
        if len(combined_highs) > self.period:
            self.highs = combined_highs[-self.period:]
            self.lows = combined_lows[-self.period:]
        else:
            self.highs = combined_highs
            self.lows = combined_lows
    
    def compute_result(self) -> float:
        if len(self.highs) < 2:
            return 0.0
        # Days since highest high
        max_idx = self.highs.index(max(self.highs))
        days_since_high = len(self.highs) - 1 - max_idx
        aroon_up = 100.0 * (self.period - days_since_high) / self.period
        
        # Days since lowest low
        min_idx = self.lows.index(min(self.lows))
        days_since_low = len(self.lows) - 1 - min_idx
        aroon_down = 100.0 * (self.period - days_since_low) / self.period
        
        return aroon_up - aroon_down


class ChaikinVolAccumulator(pw.BaseCustomAccumulator):
    """Chaikin Volatility accumulator - EMA of high-low range with pct change"""
    def __init__(self, period: int = 10, ema_value: float = 0.0, 
                 prev_ema: float = 0.0, is_init: bool = False):
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.ema_value = ema_value
        self.prev_ema = prev_ema
        self.is_init = is_init
    
    @classmethod
    def from_row(cls, row):
        high = row[0]
        low = row[1]
        if high is None or low is None:
            return cls()
        hl_range = float(high) - float(low)
        return cls(ema_value=hl_range, is_init=True)
    
    def update(self, other):
        if not other.is_init:
            return
        if not self.is_init:
            self.ema_value = other.ema_value
            self.is_init = True
        else:
            self.prev_ema = self.ema_value
            self.ema_value = (other.ema_value * self.alpha) + (self.ema_value * (1.0 - self.alpha))
    
    def compute_result(self) -> float:
        if not self.is_init or self.prev_ema == 0:
            return 0.0
        # Percent change of EMA
        return 100.0 * (self.ema_value - self.prev_ema) / self.prev_ema


class WindowMetadataAccumulator(pw.BaseCustomAccumulator):
    """Track window metadata - count, first/last timestamp, total volume"""
    def __init__(self, count: int = 0, first_ts: int = None, last_ts: int = None,
                 total_volume: float = 0.0):
        self.count = count
        self.first_ts = first_ts
        self.last_ts = last_ts
        self.total_volume = total_volume
    
    @classmethod
    def from_row(cls, row):
        timestamp = row[0]
        volume = row[1]
        if timestamp is None:
            return cls()
        return cls(count=1, first_ts=int(timestamp), last_ts=int(timestamp),
                   total_volume=float(volume) if volume else 0.0)
    
    def update(self, other):
        if other.count == 0:
            return
        self.count += other.count
        self.total_volume += other.total_volume
        if self.first_ts is None or (other.first_ts and other.first_ts < self.first_ts):
            self.first_ts = other.first_ts
        if self.last_ts is None or (other.last_ts and other.last_ts > self.last_ts):
            self.last_ts = other.last_ts
    
    def compute_result(self) -> tuple:
        avg_vol = self.total_volume / self.count if self.count > 0 else 0.0
        return (self.count, self.first_ts or 0, self.last_ts or 0, self.total_volume, avg_vol)

# ==========================================
# DATA INGESTION AND TRANSFORMATION
# ==========================================

# Read from Kafka
market_data_table = pw.io.kafka.read(
    rdkafka_settings={
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    },
    topic=TOPIC,
    format="json",
    schema=MarketDataSchema,
    autocommit_duration_ms=1000,
)

# Convert timestamp to datetime
market_data_table = market_data_table.with_columns(
    event_time=market_data_table.timestamp.dt.from_timestamp(unit="ms")
)

# Group by symbol for stateful processing
grouped_by_symbol = market_data_table.groupby(market_data_table.symbol)

# Calculate ALL technical indicators in a single reduce operation
final_features = grouped_by_symbol.reduce(
    symbol=pw.this.symbol,
    time=pw.reducers.latest(pw.this.event_time),
    price=pw.reducers.latest(pw.this.price),
    high=pw.reducers.latest(pw.this.high),
    low=pw.reducers.latest(pw.this.low),
    open=pw.reducers.latest(pw.this.open),
    volume=pw.reducers.latest(pw.this.volume),
    bid=pw.reducers.latest(pw.this.bid),
    ask=pw.reducers.latest(pw.this.ask),
    
    # EMAs
    ema_6=pw.reducers.udf_reducer(EMA6Accumulator)(pw.this.price),
    ema_10=pw.reducers.udf_reducer(EMA10Accumulator)(pw.this.price),
    ema_12=pw.reducers.udf_reducer(EMA12Accumulator)(pw.this.price),
    ema_13=pw.reducers.udf_reducer(EMA13Accumulator)(pw.this.price),
    ema_20=pw.reducers.udf_reducer(EMA20Accumulator)(pw.this.price),
    ema_26=pw.reducers.udf_reducer(EMA26Accumulator)(pw.this.price),
    ema_50=pw.reducers.udf_reducer(EMA50Accumulator)(pw.this.price),
    
    # RSI
    rsi=pw.reducers.udf_reducer(RSIAccumulator)(pw.this.price),
)

# Calculate derived features
derived_features = final_features.with_columns(
    # MACD
    macd=final_features.ema_12 - final_features.ema_26,
    
    # Elder Ray
    bull_power=final_features.high - final_features.ema_13,
    bear_power=final_features.low - final_features.ema_13,
    
    # Simple spread and mid price
    spread=final_features.ask - final_features.bid,
    mid_price=(final_features.ask + final_features.bid) / 2.0,
    price_range=final_features.high - final_features.low,
)

# Group again for signal calculations
grouped_for_signals = derived_features.groupby(derived_features.symbol)

final_with_signals = grouped_for_signals.reduce(
    symbol=pw.reducers.sorted_tuple(pw.this.symbol)[-1],
    time=pw.reducers.sorted_tuple(pw.this.time)[-1],
    price=pw.reducers.sorted_tuple(pw.this.price)[-1],
    high=pw.reducers.sorted_tuple(pw.this.high)[-1],
    low=pw.reducers.sorted_tuple(pw.this.low)[-1],
    open=pw.reducers.sorted_tuple(pw.this.open)[-1],
    volume=pw.reducers.sorted_tuple(pw.this.volume)[-1],
    ema_6=pw.reducers.sorted_tuple(pw.this.ema_6)[-1],
    ema_10=pw.reducers.sorted_tuple(pw.this.ema_10)[-1],
    ema_12=pw.reducers.sorted_tuple(pw.this.ema_12)[-1],
    ema_13=pw.reducers.sorted_tuple(pw.this.ema_13)[-1],
    ema_20=pw.reducers.sorted_tuple(pw.this.ema_20)[-1],
    ema_26=pw.reducers.sorted_tuple(pw.this.ema_26)[-1],
    ema_50=pw.reducers.sorted_tuple(pw.this.ema_50)[-1],
    rsi=pw.reducers.sorted_tuple(pw.this.rsi)[-1],
    macd=pw.reducers.sorted_tuple(pw.this.macd)[-1],
    bull_power=pw.reducers.sorted_tuple(pw.this.bull_power)[-1],
    bear_power=pw.reducers.sorted_tuple(pw.this.bear_power)[-1],
    spread=pw.reducers.sorted_tuple(pw.this.spread)[-1],
    mid_price=pw.reducers.sorted_tuple(pw.this.mid_price)[-1],
    price_range=pw.reducers.sorted_tuple(pw.this.price_range)[-1],
    
    # Compute signal indicators
    macd_signal=pw.reducers.udf_reducer(EMA9Accumulator)(pw.this.macd),
    rsi_smoothed=pw.reducers.udf_reducer(SMA5Accumulator)(pw.this.rsi),
)

# ==========================================
# TRADING SIGNALS LOGIC
# ==========================================

# Long Entry Signal
long_entry_signal = (
    (final_with_signals.ema_20 > final_with_signals.ema_50) &
    (final_with_signals.price > final_with_signals.ema_20) &
    (final_with_signals.macd < final_with_signals.macd_signal) &
    (final_with_signals.rsi_smoothed > 50.0) &
    (final_with_signals.bull_power > 0) &
    (final_with_signals.bear_power < 0)
)

# Long Exit Signal
long_exit_signal = (
    (final_with_signals.ema_20 < final_with_signals.ema_50) &
    (final_with_signals.price < final_with_signals.ema_20)
)

# Short Entry Signal
short_entry_signal = (
    (final_with_signals.ema_6 < final_with_signals.ema_10) &
    (final_with_signals.price < final_with_signals.ema_6) &
    (final_with_signals.macd > final_with_signals.macd_signal) &
    (final_with_signals.rsi_smoothed < 40.0)
)

# Short Exit Signal
short_exit_signal = (
    (final_with_signals.ema_6 > final_with_signals.ema_10) &
    (final_with_signals.price > final_with_signals.ema_6)
)

# Add signals to table
final_output = final_with_signals.with_columns(
    signal=pw.if_else(
        long_entry_signal, 1,
        pw.if_else(short_entry_signal, -1,
            pw.if_else(long_exit_signal, 0,
                pw.if_else(short_exit_signal, 0, 99)
            )
        )
    ),
    trade_type=pw.if_else(
        long_entry_signal, "long_entry",
        pw.if_else(short_entry_signal, "short_entry",
            pw.if_else(long_exit_signal, "long_exit",
                pw.if_else(short_exit_signal, "short_exit", "hold")
            )
        )
    )
)

# ==========================================
# S3 STORAGE FUNCTION
# ==========================================

def upload_to_s3_callback(key, row, time_ts, is_addition):
    """Upload each event to S3"""
    if not is_addition or not s3_client:
        return
    
    try:
        # Create a unique key for this event
        timestamp = datetime.now().strftime("%Y%m%d/%H")
        symbol = row.get("symbol", "unknown")
        event_id = f"{time_ts}_{symbol}"
        s3_key = f"{S3_PREFIX}/{timestamp}/{symbol}/{event_id}.json"
        
        # Convert row to JSON
        event_data = {
            "symbol": symbol,
            "time": str(row.get("time", "")),
            "price": float(row.get("price", 0)),
            "volume": int(row.get("volume", 0)),
            "high": float(row.get("high", 0)),
            "low": float(row.get("low", 0)),
            "open": float(row.get("open", 0)),
            "ema_6": float(row.get("ema_6", 0)),
            "ema_10": float(row.get("ema_10", 0)),
            "ema_20": float(row.get("ema_20", 0)),
            "ema_50": float(row.get("ema_50", 0)),
            "rsi": float(row.get("rsi", 0)),
            "macd": float(row.get("macd", 0)),
            "signal": int(row.get("signal", 99)),
            "trade_type": str(row.get("trade_type", "hold")),
            "uploaded_at": datetime.now().isoformat()
        }
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(event_data),
            ContentType='application/json'
        )
        
        print(f"✓ Uploaded to S3: {s3_key}")
        
    except Exception as e:
        print(f"✗ S3 upload error: {e}")

# Subscribe to final output for S3 upload
if s3_client:
    pw.io.subscribe(final_output, on_change=upload_to_s3_callback)

# ==========================================
# OUTPUT
# ==========================================

# Write final features and signals
pw.io.csv.write(final_output, f"{OUTPUT_DIR}/features/stateful_indicators.csv")
pw.io.jsonlines.write(final_output, f"{OUTPUT_DIR}/features/stateful_indicators.jsonl")

# Write trading signals (filter for actual trades)
trading_signals = final_output.filter(final_output.trade_type != "hold")
pw.io.csv.write(trading_signals, f"{OUTPUT_DIR}/features/trading_signals.csv")

# Statistics
stats_table = market_data_table.groupby(market_data_table.symbol).reduce(
    symbol=pw.this.symbol,
    count=pw.reducers.count(),
    avg_price=pw.reducers.avg(pw.this.price),
    max_price=pw.reducers.max(pw.this.price),
    min_price=pw.reducers.min(pw.this.price),
    total_volume=pw.reducers.sum(pw.this.volume),
)

pw.io.csv.write(stats_table, f"{OUTPUT_DIR}/symbol_stats.csv")

# Original enriched data
enriched_table = market_data_table.select(
    pw.this.timestamp,
    pw.this.symbol,
    pw.this.price,
    pw.this.volume,
    pw.this.bid,
    pw.this.ask,
    pw.this.high,
    pw.this.low,
    pw.this.open,
    pw.this.partition,
    pw.this.event_time,
    spread=pw.this.ask - pw.this.bid,
    mid_price=(pw.this.ask + pw.this.bid) / 2.0,
    price_range=pw.this.high - pw.this.low
)

pw.io.csv.write(enriched_table, f"{OUTPUT_DIR}/enriched_data.csv")
pw.io.jsonlines.write(enriched_table, f"{OUTPUT_DIR}/enriched_data.jsonl")

print("Starting STATEFUL real-time feature calculation with S3 storage...")
print(f"Outputs will be written to: {OUTPUT_DIR}")
if s3_client:
    print(f"Events will be stored in S3: s3://{S3_BUCKET}/{S3_PREFIX}/")
pw.run()