"""
Trading Simulation Backend API with Async MongoDB (Motor)
FastAPI backend serving live market data, trading statistics, and trade history.
Uses Motor for non-blocking database operations.
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Optional
from datetime import datetime
from bson import ObjectId
import os
import math

# ==========================================
# CONFIGURATION
# ==========================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongo:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "market_db")
INITIAL_CASH = float(os.getenv("INITIAL_CASH", 10000.0))

# ==========================================
# ASYNC MONGODB CONNECTION (Motor)
# ==========================================

motor_client: Optional[AsyncIOMotorClient] = None
db = None


async def get_database():
    """Get or create async MongoDB connection."""
    global motor_client, db
    if motor_client is None:
        motor_client = AsyncIOMotorClient(MONGODB_URI)
        db = motor_client[MONGODB_DATABASE]
        print(f"✅ Motor async client connected to {MONGODB_DATABASE}")
    return db


def get_collection(database, symbol: str, suffix: str):
    """Get the collection for a specific symbol."""
    collection_name = f"{symbol.upper()}_{suffix}"
    return database[collection_name]


async def list_symbol_collections(database, suffix: str) -> List[str]:
    """List all symbols that have collections with given suffix."""
    symbols = []
    collection_names = await database.list_collection_names()
    for coll_name in collection_names:
        if coll_name.endswith(f"_{suffix}"):
            symbol = coll_name.replace(f"_{suffix}", "")
            symbols.append(symbol)
    return sorted(symbols)


# ==========================================
# FASTAPI APP
# ==========================================

app = FastAPI(
    title="Trading Simulation API (Async)",
    description="API for live market data, trading statistics, and trade history with async MongoDB",
    version="3.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_db_client():
    """Initialize database connection on startup."""
    await get_database()


@app.on_event("shutdown")
async def shutdown_db_client():
    """Close database connection on shutdown."""
    global motor_client
    if motor_client:
        motor_client.close()


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def serialize_doc(doc):
    """Serialize MongoDB document for JSON response."""
    if doc is None:
        return None
    
    serialized = {}
    for key, value in doc.items():
        if key == "_id":
            serialized[key] = str(value)
        elif key == "_pathway_time":
            continue  # Skip internal field
        elif isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                serialized[key] = 0.0
            else:
                serialized[key] = round(value, 6)
        else:
            serialized[key] = value
    
    return serialized


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    """Calculate approximate Sharpe Ratio."""
    if not returns or len(returns) < 2:
        return 0.0
    
    mean_return = sum(returns) / len(returns)
    variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
    std_dev = math.sqrt(variance) if variance > 0 else 0.001
    
    # Annualized (assuming daily data, ~252 trading days)
    annualized_return = mean_return * 252
    annualized_std = std_dev * math.sqrt(252)
    
    if annualized_std == 0:
        return 0.0
    
    sharpe = (annualized_return - risk_free_rate) / annualized_std
    return round(sharpe, 4)


# ==========================================
# API ENDPOINTS
# ==========================================

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Trading Simulation API (Async)",
        "version": "3.0.0",
        "driver": "motor (async)",
        "note": "Uses per-symbol collections: {SYMBOL}_features, {SYMBOL}_stats",
        "endpoints": {
            "market_live": "/market/live?symbol=AAPL",
            "trading_stats": "/trading/stats?symbol=AAPL",
            "trading_history": "/trading/history?symbol=AAPL",
            "symbols": "/market/symbols",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    mongo_status = "disconnected"
    collections = []
    
    try:
        database = await get_database()
        if database is not None:
            await motor_client.admin.command('ping')
            mongo_status = "connected"
            collections = await database.list_collection_names()
    except Exception as e:
        print(f"Health check MongoDB error: {e}")
    
    return {
        "status": "running",
        "mongodb": mongo_status,
        "driver": "motor (async)",
        "database": MONGODB_DATABASE,
        "collection_pattern": "{symbol}_features, {symbol}_stats",
        "available_collections": collections
    }


@app.get("/market/symbols")
async def get_symbols():
    """Get list of available symbols from collections."""
    try:
        database = await get_database()
        
        features_symbols = await list_symbol_collections(database, "features")
        stats_symbols = await list_symbol_collections(database, "stats")
        
        all_symbols = sorted(set(features_symbols + stats_symbols))
        
        return {
            "success": True,
            "count": len(all_symbols),
            "symbols": all_symbols,
            "collections": {
                "features": [f"{s}_features" for s in features_symbols],
                "stats": [f"{s}_stats" for s in stats_symbols]
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/market/live")
async def get_live_market_data(
    symbol: str = Query(..., description="Stock symbol (required)"),
    limit: int = Query(10, description="Number of records", le=100)
):
    """
    GET /market/live - Latest price & indicators for a symbol.
    """
    try:
        database = await get_database()
        collection = get_collection(database, symbol, "features")
        
        cursor = collection.find({}).sort("timestamp", -1).limit(limit)
        data = [serialize_doc(doc) async for doc in cursor]
        
        if not data:
            return {
                "success": True,
                "symbol": symbol.upper(),
                "collection": f"{symbol.upper()}_features",
                "count": 0,
                "data": [],
                "message": f"No data found. Collection {symbol.upper()}_features may not exist or is empty."
            }
        
        formatted_data = []
        for doc in data:
            formatted_data.append({
                "symbol": doc.get("symbol"),
                "timestamp": doc.get("timestamp"),
                "ohlcv": {
                    "open": doc.get("open", 0),
                    "high": doc.get("high", 0),
                    "low": doc.get("low", 0),
                    "close": doc.get("close", 0),
                    "volume": doc.get("volume", 0)
                },
                "indicators": {
                    "ema_12": doc.get("ema_12", 0),
                    "ema_26": doc.get("ema_26", 0),
                    "ema_50": doc.get("ema_50", 0),
                    "rsi_14": doc.get("rsi_14", 0),
                    "macd_line": doc.get("macd_line", 0),
                    "signal_line": doc.get("signal_line", 0),
                    "macd_histogram": doc.get("macd_histogram", 0)
                },
                "signal": doc.get("action", "HOLD")
            })
        
        return {
            "success": True,
            "symbol": symbol.upper(),
            "collection": f"{symbol.upper()}_features",
            "count": len(formatted_data),
            "data": formatted_data
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/trading/stats")
async def get_trading_stats(
    symbol: str = Query(..., description="Stock symbol (required)")
):
    """
    GET /trading/stats - Current PnL, Portfolio Value, Sharpe Ratio for a symbol.
    """
    try:
        database = await get_database()
        collection = get_collection(database, symbol, "stats")
        
        latest_stats = await collection.find_one({}, sort=[("timestamp", -1)])
        
        if not latest_stats:
            return {
                "success": True,
                "symbol": symbol.upper(),
                "collection": f"{symbol.upper()}_stats",
                "data": {
                    "portfolio_value": INITIAL_CASH,
                    "pnl": 0.0,
                    "pnl_percentage": 0.0,
                    "sharpe_ratio": 0.0,
                    "cash_balance": INITIAL_CASH,
                    "position_size": 0.0,
                    "current_price": 0.0,
                    "total_trades": 0
                },
                "message": f"No data found. Collection {symbol.upper()}_stats may not exist or is empty."
            }
        
        sharpe = latest_stats.get("sharpe_ratio", 0.0)
        
        if sharpe == 0:
            cursor = collection.find({}).sort("timestamp", 1).limit(100)
            historical = [doc async for doc in cursor]
            
            returns = []
            prev_value = INITIAL_CASH
            for doc in historical:
                pv = doc.get("portfolio_value", INITIAL_CASH)
                if prev_value > 0:
                    daily_return = (pv - prev_value) / prev_value
                    returns.append(daily_return)
                prev_value = pv
            
            sharpe = calculate_sharpe_ratio(returns)
        
        portfolio_value = latest_stats.get("portfolio_value", INITIAL_CASH)
        pnl = latest_stats.get("pnl", 0.0)
        pnl_pct = latest_stats.get("pnl_percentage", (pnl / INITIAL_CASH) * 100 if INITIAL_CASH > 0 else 0)
        
        return {
            "success": True,
            "symbol": symbol.upper(),
            "collection": f"{symbol.upper()}_stats",
            "data": {
                "symbol": latest_stats.get("symbol"),
                "timestamp": latest_stats.get("timestamp"),
                "portfolio_value": round(portfolio_value, 2),
                "pnl": round(pnl, 2),
                "pnl_percentage": round(pnl_pct, 2),
                "sharpe_ratio": round(sharpe, 4),
                "cash_balance": round(latest_stats.get("cash_balance", 0), 2),
                "position_size": round(latest_stats.get("position_size", 0), 4),
                "entry_price": round(latest_stats.get("entry_price", 0), 2),
                "current_price": round(latest_stats.get("current_price", 0), 2),
                "total_trades": latest_stats.get("total_trades", 0),
                "initial_capital": INITIAL_CASH
            }
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/trading/history")
async def get_trading_history(
    symbol: str = Query(..., description="Stock symbol (required)"),
    limit: int = Query(50, description="Number of records", le=500)
):
    """
    GET /trading/history - List of executed trades for a symbol.
    """
    try:
        database = await get_database()
        collection = get_collection(database, symbol, "stats")
        
        cursor = collection.find(
            {},
            {"symbol": 1, "timestamp": 1, "trade_log": 1, "total_trades": 1, 
             "portfolio_value": 1, "pnl": 1, "pnl_percentage": 1, 
             "sharpe_ratio": 1, "current_price": 1}
        ).sort("timestamp", -1).limit(limit)
        
        trades = []
        seen_trades = set()
        
        async for doc in cursor:
            trade_log = doc.get("trade_log", "")
            if trade_log:
                for trade in trade_log.split(";"):
                    if trade and trade not in seen_trades:
                        seen_trades.add(trade)
                        
                        if "@" in trade:
                            action, price = trade.split("@", 1)
                            trades.append({
                                "symbol": doc.get("symbol"),
                                "timestamp": doc.get("timestamp"),
                                "action": action,
                                "price": float(price),
                                "portfolio_value_at_trade": doc.get("portfolio_value")
                            })
        
        if not trades:
            cursor = collection.find({}).sort("timestamp", -1).limit(limit)
            
            async for doc in cursor:
                trades.append({
                    "symbol": doc.get("symbol"),
                    "timestamp": doc.get("timestamp"),
                    "portfolio_value": round(doc.get("portfolio_value", INITIAL_CASH), 2),
                    "pnl": round(doc.get("pnl", 0), 2),
                    "pnl_percentage": round(doc.get("pnl_percentage", 0), 2),
                    "sharpe_ratio": round(doc.get("sharpe_ratio", 0), 4),
                    "current_price": round(doc.get("current_price", 0), 2),
                    "total_trades": doc.get("total_trades", 0)
                })
        
        return {
            "success": True,
            "symbol": symbol.upper(),
            "collection": f"{symbol.upper()}_stats",
            "count": len(trades),
            "trades": trades
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/trading/all")
async def get_all_symbols_stats():
    """
    GET /trading/all - Get latest stats for all available symbols.
    """
    try:
        database = await get_database()
        symbols = await list_symbol_collections(database, "stats")
        
        all_stats = []
        for symbol in symbols:
            collection = get_collection(database, symbol, "stats")
            latest = await collection.find_one({}, sort=[("timestamp", -1)])
            
            if latest:
                all_stats.append({
                    "symbol": symbol,
                    "timestamp": latest.get("timestamp"),
                    "portfolio_value": round(latest.get("portfolio_value", INITIAL_CASH), 2),
                    "pnl": round(latest.get("pnl", 0), 2),
                    "pnl_percentage": round(latest.get("pnl_percentage", 0), 2),
                    "sharpe_ratio": round(latest.get("sharpe_ratio", 0), 4),
                    "total_trades": latest.get("total_trades", 0)
                })
        
        return {
            "success": True,
            "count": len(all_stats),
            "data": all_stats
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}
