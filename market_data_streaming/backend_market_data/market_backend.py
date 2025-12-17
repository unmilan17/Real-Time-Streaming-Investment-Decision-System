"""
Market Data Backend API
FastAPI backend serving OHLCV and feature data from MongoDB.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from typing import List, Optional
import json
import asyncio
from datetime import datetime
from bson import ObjectId
import os
import time

# ==========================================
# MONGODB CONNECTION
# ==========================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "market_db")
MONGODB_RAW_COLLECTION = os.getenv("MONGODB_RAW_COLLECTION", "ohlcv_raw")
MONGODB_FEATURES_COLLECTION = os.getenv("MONGODB_FEATURES_COLLECTION", "market_features")


def get_mongodb_connection(max_retries=5, retry_delay=2):
    """
    Get MongoDB connection with retry logic.
    Prevents startup crashes when MongoDB is not yet ready.
    """
    for attempt in range(max_retries):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            # Test the connection
            client.admin.command('ping')
            db = client[MONGODB_DATABASE]
            print(f"✅ Successfully connected to MongoDB: {MONGODB_DATABASE}")
            return db
        except ConnectionFailure as e:
            if attempt == max_retries - 1:
                print(f"❌ Failed to connect to MongoDB after {max_retries} attempts: {e}")
                raise e
            print(f"⏳ Connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"❌ MongoDB error: {e}")
                raise e
            time.sleep(retry_delay)


# Initialize MongoDB connection
try:
    db = get_mongodb_connection()
    raw_collection = db[MONGODB_RAW_COLLECTION]
    features_collection = db[MONGODB_FEATURES_COLLECTION]
except Exception as e:
    print(f"⚠️  Failed to initialize MongoDB: {e}")
    db = None
    raw_collection = None
    features_collection = None


# ==========================================
# FASTAPI APP
# ==========================================

app = FastAPI(
    title="Market Data Streaming API",
    description="API for accessing OHLCV data and computed market features",
    version="1.0.0"
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# WEBSOCKET CONNECTION MANAGER
# ==========================================

class ConnectionManager:
    """Manages WebSocket connections for live streaming."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def serialize_doc(doc):
    """Serialize MongoDB document for JSON response."""
    if doc is None:
        return None
    
    # Convert ObjectId to string
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    
    # Ensure timestamp is an integer
    if "timestamp" in doc:
        doc["timestamp"] = int(doc["timestamp"]) if doc["timestamp"] else None
    
    return doc


# ==========================================
# REST API ENDPOINTS
# ==========================================

@app.get("/market/ohlcv")
async def get_ohlcv_data(
    symbol: Optional[str] = Query(None, description="Filter by stock symbol"),
    start_time: Optional[int] = Query(None, description="Start timestamp (ms)"),
    end_time: Optional[int] = Query(None, description="End timestamp (ms)"),
    limit: int = Query(100, description="Maximum number of records to return", le=1000)
):
    """
    Get raw OHLCV data from the database.
    
    - **symbol**: Optional filter by stock symbol (e.g., AAPL)
    - **start_time**: Optional start timestamp in milliseconds
    - **end_time**: Optional end timestamp in milliseconds
    - **limit**: Maximum records to return (default: 100, max: 1000)
    """
    if raw_collection is None:
        return {"success": False, "error": "Database not connected"}
    
    try:
        # Build query filter
        query = {}
        
        if symbol:
            query["symbol"] = symbol.upper()
        
        if start_time or end_time:
            query["timestamp"] = {}
            if start_time:
                query["timestamp"]["$gte"] = start_time
            if end_time:
                query["timestamp"]["$lte"] = end_time
            
            # Remove empty timestamp filter
            if not query["timestamp"]:
                del query["timestamp"]
        
        # Execute query
        cursor = raw_collection.find(query).sort("timestamp", -1).limit(limit)
        data = [serialize_doc(doc) for doc in cursor]
        
        return {
            "success": True,
            "count": len(data),
            "filters": {
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time
            },
            "data": data
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/market/features")
async def get_features_data(
    symbol: Optional[str] = Query(None, description="Filter by stock symbol"),
    limit: int = Query(100, description="Maximum number of records to return", le=1000)
):
    """
    Get computed market features (SMA, VWAP) from the database.
    
    - **symbol**: Optional filter by stock symbol (e.g., AAPL)
    - **limit**: Maximum records to return (default: 100, max: 1000)
    """
    if features_collection is None:
        return {"success": False, "error": "Database not connected"}
    
    try:
        # Build query filter
        query = {}
        
        if symbol:
            query["symbol"] = symbol.upper()
        
        # Execute query
        cursor = features_collection.find(query).sort("timestamp", -1).limit(limit)
        data = [serialize_doc(doc) for doc in cursor]
        
        return {
            "success": True,
            "count": len(data),
            "filters": {"symbol": symbol},
            "data": data
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/market/symbols")
async def get_available_symbols():
    """
    Get list of all available stock symbols in the database.
    """
    if raw_collection is None:
        return {"success": False, "error": "Database not connected"}
    
    try:
        # Get distinct symbols
        symbols = raw_collection.distinct("symbol")
        
        return {
            "success": True,
            "count": len(symbols),
            "symbols": sorted(symbols)
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/market/stats")
async def get_market_stats():
    """
    Get overall market statistics.
    """
    if raw_collection is None or features_collection is None:
        return {"success": False, "error": "Database not connected"}
    
    try:
        # Get counts
        raw_count = raw_collection.count_documents({})
        features_count = features_collection.count_documents({})
        
        # Get symbol count
        symbols = raw_collection.distinct("symbol")
        
        # Get latest data timestamp
        latest_raw = raw_collection.find_one(
            {}, 
            sort=[("timestamp", -1)]
        )
        latest_timestamp = latest_raw.get("timestamp") if latest_raw else None
        
        return {
            "success": True,
            "stats": {
                "total_ohlcv_records": raw_count,
                "total_feature_records": features_count,
                "unique_symbols": len(symbols),
                "symbols": sorted(symbols),
                "latest_timestamp": latest_timestamp
            }
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/market/latest")
async def get_latest_data(symbol: Optional[str] = None):
    """
    Get the most recent OHLCV and feature data for each symbol.
    """
    if raw_collection is None:
        return {"success": False, "error": "Database not connected"}
    
    try:
        if symbol:
            # Get latest for specific symbol
            raw_data = raw_collection.find_one(
                {"symbol": symbol.upper()},
                sort=[("timestamp", -1)]
            )
            feature_data = features_collection.find_one(
                {"symbol": symbol.upper()},
                sort=[("timestamp", -1)]
            ) if features_collection else None
            
            return {
                "success": True,
                "symbol": symbol.upper(),
                "ohlcv": serialize_doc(raw_data),
                "features": serialize_doc(feature_data)
            }
        else:
            # Get latest for all symbols
            pipeline = [
                {"$sort": {"timestamp": -1}},
                {"$group": {
                    "_id": "$symbol",
                    "latest": {"$first": "$$ROOT"}
                }},
                {"$replaceRoot": {"newRoot": "$latest"}}
            ]
            
            latest_data = list(raw_collection.aggregate(pipeline))
            
            return {
                "success": True,
                "count": len(latest_data),
                "data": [serialize_doc(doc) for doc in latest_data]
            }
    
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
# WEBSOCKET ENDPOINT
# ==========================================

@app.websocket("/ws/market/live")
async def websocket_market_stream(websocket: WebSocket):
    """
    WebSocket endpoint for live market data updates.
    Streams new OHLCV data as it arrives.
    """
    await manager.connect(websocket)
    
    try:
        # Send initial data
        if raw_collection is not None:
            initial_data = list(
                raw_collection.find()
                .sort("timestamp", -1)
                .limit(50)
            )
            await websocket.send_json({
                "type": "initial",
                "data": [serialize_doc(doc) for doc in initial_data]
            })
        
        # Track last seen timestamp
        last_timestamp = int(datetime.now().timestamp() * 1000)
        
        while True:
            await asyncio.sleep(2)  # Poll every 2 seconds
            
            if raw_collection is None:
                continue
            
            # Check for new data
            new_data = list(
                raw_collection.find({"timestamp": {"$gt": last_timestamp}})
                .sort("timestamp", 1)
            )
            
            if new_data:
                for doc in new_data:
                    await websocket.send_json({
                        "type": "new",
                        "data": serialize_doc(doc)
                    })
                    last_timestamp = doc.get("timestamp", last_timestamp)
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# ==========================================
# HEALTH CHECK
# ==========================================

@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    Returns status of the API and database connection.
    """
    mongo_status = "disconnected"
    
    try:
        if db is not None:
            # Test connection
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            mongo_status = "connected"
            client.close()
    except Exception as e:
        print(f"Health check MongoDB error: {e}")
        mongo_status = "disconnected"
    
    return {
        "status": "running",
        "mongodb": mongo_status,
        "database": MONGODB_DATABASE,
        "collections": {
            "raw": MONGODB_RAW_COLLECTION,
            "features": MONGODB_FEATURES_COLLECTION
        },
        "active_websockets": len(manager.active_connections)
    }


# ==========================================
# ROOT ENDPOINT
# ==========================================

@app.get("/")
async def root():
    """
    Root endpoint with API information.
    """
    return {
        "name": "Market Data Streaming API",
        "version": "1.0.0",
        "endpoints": {
            "ohlcv": "/market/ohlcv",
            "features": "/market/features",
            "symbols": "/market/symbols",
            "stats": "/market/stats",
            "latest": "/market/latest",
            "health": "/health",
            "websocket": "/ws/market/live"
        }
    }
