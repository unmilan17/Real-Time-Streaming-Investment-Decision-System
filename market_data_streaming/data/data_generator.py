#!/usr/bin/env python3
"""
build_streaming_csv.py

Fetch historical bars for multiple symbols (yfinance), convert to the
MarketDataSchema and produce one interleaved CSV suitable for streaming replay.

Output columns (exact order):
timestamp,symbol,price,volume,bid,ask,high,low,open,time,diff

Notes:
- timestamp: epoch milliseconds (int)
- time: epoch seconds (int)
- diff: price - open, rounded and cast to int (change if you want float)
- bid/ask synthesized from high/low as mid +/- half-spread because yfinance lacks BBO
- rows are globally ordered by timestamp; rows sharing same timestamp are
  deterministically shuffled (so replay ordering looks realistic and is reproducible)
"""
import time
from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd
import numpy as np
import yfinance as yf
import sys
import os

# ----------------- CONFIG -----------------
OUTPUT_FILE = "market_data_stream_ready.csv"

# Edit symbols as needed
SYMBOLS: List[str] = [
    "AAPL", "MSFT", "AMZN", "GOOG", "TSLA",
    "NVDA", "META", "JPM", "BAC", "XOM"
]

INTERVAL = "1d"                 # '1d' for daily; for intraday use '1m' or '5m' (limited history)
TARGET_ROWS_PER_SYMBOL = 500    # desired rows per symbol (will tail if more returned)
YEARS_EXTRA = 3                 # fetch a few extra years to ensure enough rows
SLEEP_BETWEEN_SYMBOLS = 0.5     # polite pause between requests
# ------------------------------------------

def fetch_history(symbol: str, interval: str = INTERVAL, target_rows: int = TARGET_ROWS_PER_SYMBOL) -> Optional[pd.DataFrame]:
    """
    Fetch historical bars using yfinance. Returns last `target_rows` rows DataFrame
    or None on failure / empty result.
    """
    try:
        years_needed = int(target_rows / 252) + YEARS_EXTRA
        start = (datetime.utcnow() - timedelta(days=365 * years_needed)).strftime("%Y-%m-%d")
        end = datetime.utcnow().strftime("%Y-%m-%d")

        # preferred: explicit start/end
        df = yf.Ticker(symbol).history(start=start, end=end, interval=interval, auto_adjust=False, actions=False)

        # fallback to period='max' if empty
        if df is None or df.empty:
            df = yf.Ticker(symbol).history(period="max", interval=interval)

        if df is None or df.empty:
            print(f"[{symbol}] No data returned from yfinance.", file=sys.stderr)
            return None

        # normalize column names and ensure DatetimeIndex
        df.columns = [str(c).lower() for c in df.columns]
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce")

        # keep last target_rows
        df = df.tail(target_rows).copy()
        return df

    except Exception as e:
        print(f"[{symbol}] fetch error: {e}", file=sys.stderr)
        return None

def build_schema_rows(symbol: str, df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Convert a yfinance dataframe for one symbol into a DataFrame with columns:
    timestamp,symbol,price,volume,bid,ask,high,low,open,time,diff
    """
    if df is None or df.empty:
        return None

    # Ensure standard columns exist in lowercase
    for c in ["open","high","low","close","volume"]:
        if c not in df.columns:
            df[c] = np.nan

    # Convert index to datetime and compute epoch values
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")

    # keep values
    price = pd.to_numeric(df["close"], errors="coerce")
    open_p = pd.to_numeric(df["open"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    vol = pd.to_numeric(df.get("volume", pd.Series([0]*len(df))), errors="coerce").fillna(0).astype(int)

    # timestamps
    ts_ns = df.index.view("int64")            # nanoseconds
    ts_ms = (ts_ns // 1_000_000).astype(int)  # milliseconds
    ts_s = (ts_ns // 1_000_000_000).astype(int)  # seconds

    # synthetic bid/ask using mid +/- half spread
    mid = (high + low) / 2.0
    half_spread = (high - low) / 2.0
    half_spread = half_spread.fillna(0.0)

    bid = (mid - half_spread).astype(float)
    ask = (mid + half_spread).astype(float)

    # diff = price - open (rounded to nearest int for your schema)
    diff_float = (price - open_p)
    diff_int = diff_float.round().fillna(0).astype(int)

    out = pd.DataFrame({
        "timestamp": ts_ms,
        "symbol": symbol,
        "price": price.astype(float),
        "volume": vol,
        "bid": bid,
        "ask": ask,
        "high": high.astype(float),
        "low": low.astype(float),
        "open": open_p.astype(float),
        "time": ts_s,
        "diff": diff_int
    })

    # Drop rows with missing timestamp
    out = out.dropna(subset=["timestamp"]).reset_index(drop=True)

    # Ensure types are plain Python / numpy compatible (no pandas nullable dtypes)
    out["timestamp"] = out["timestamp"].astype(int)
    out["time"] = out["time"].astype(int)
    out["volume"] = out["volume"].astype(int)
    out["diff"] = out["diff"].astype(int)

    # Keep exact column order required
    cols = ["timestamp","symbol","price","volume","bid","ask","high","low","open","time","diff"]
    return out[cols]

def deterministic_shuffle_within_timestamp(df: pd.DataFrame, ts_col: str = "timestamp") -> pd.DataFrame:
    """
    For rows that share the same timestamp value, deterministically shuffle
    their order using the timestamp as RNG seed. Keeps chronological ordering
    across different timestamps.
    """
    parts = []
    # groupby with sort=True to ensure increasing timestamps
    for ts, group in df.groupby(ts_col, sort=True):
        g = group.copy()
        seed = int(ts) & 0xFFFFFFFF
        rng = np.random.RandomState(seed)
        perm = rng.permutation(len(g))
        parts.append(g.iloc[perm])
    if not parts:
        return pd.DataFrame(columns=df.columns)
    return pd.concat(parts, ignore_index=True)

def build_combined_csv(symbols: List[str], output_file: str = OUTPUT_FILE):
    frames = []
    for sym in symbols:
        print(f"[{sym}] fetching...", flush=True)
        raw = fetch_history(sym)
        if raw is None or raw.empty:
            print(f"[{sym}] skipped (no data).", file=sys.stderr, flush=True)
            time.sleep(SLEEP_BETWEEN_SYMBOLS)
            continue
        processed = build_schema_rows(sym, raw)
        if processed is None or processed.empty:
            print(f"[{sym}] processed empty; skipping.", file=sys.stderr, flush=True)
            time.sleep(SLEEP_BETWEEN_SYMBOLS)
            continue
        print(f"[{sym}] fetched {len(processed)} rows", flush=True)
        frames.append(processed)
        time.sleep(SLEEP_BETWEEN_SYMBOLS)

    if not frames:
        print("No data fetched for any symbol; exiting.", file=sys.stderr)
        return

    # Concatenate all frames (order = symbol fetch order)
    combined = pd.concat(frames, ignore_index=True)

    # sort by timestamp across symbols (primary)
    combined = combined.sort_values(["timestamp", "symbol"], ascending=[True, True]).reset_index(drop=True)

    # deterministic shuffle within rows that share the same timestamp value
    combined = deterministic_shuffle_within_timestamp(combined, ts_col="timestamp")

    # final sort/consistency: ensure numeric columns are right types
    final_cols = ["timestamp","symbol","price","volume","bid","ask","high","low","open","time","diff"]
    combined = combined[final_cols]

    # save CSV
    combined.to_csv(output_file, index=False)
    print(f"\nSaved combined CSV with {len(combined)} rows -> {output_file}", flush=True)

if __name__ == "__main__":
    # Ensure yfinance uses a sane user-agent in some environments (optional)
    # os.environ["YF_DATA_SOURCE"] = "yfinance"

    # build CSV
    build_combined_csv(SYMBOLS, OUTPUT_FILE)
