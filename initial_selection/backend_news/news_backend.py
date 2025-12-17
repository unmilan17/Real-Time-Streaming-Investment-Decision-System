"""
News Streaming API with Async MongoDB (Motor)
FastAPI backend serving news data with non-blocking database operations.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Optional
import asyncio
from datetime import datetime
from bson import ObjectId
import os

# ==========================================
# CONFIGURATION
# ==========================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "news_database")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "news_collection")

# ==========================================
# ASYNC MONGODB CONNECTION (Motor)
# ==========================================

# Motor async client - non-blocking MongoDB operations
motor_client: Optional[AsyncIOMotorClient] = None
db = None
collection = None


async def get_database():
    """Get or create async MongoDB connection."""
    global motor_client, db, collection
    if motor_client is None:
        motor_client = AsyncIOMotorClient(MONGODB_URI)
        db = motor_client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        print(f"✅ Motor async client connected to {MONGODB_DATABASE}")
    return collection


# ==========================================
# FASTAPI APP
# ==========================================

app = FastAPI(
    title="News Streaming API (Async)",
    description="API for live news streaming with async MongoDB operations",
    version="2.0.0"
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
# WEBSOCKET MANAGER
# ==========================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass


manager = ConnectionManager()


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def serialize_doc(doc):
    """Serialize MongoDB document for JSON response."""
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if doc and "time" in doc:
        doc["time"] = int(doc["time"]) if doc["time"] else 0
    return doc


# ==========================================
# REST ENDPOINTS
# ==========================================

@app.get("/api/news/latest")
async def get_latest_news(limit: int = 50):
    """Get latest news items."""
    try:
        coll = await get_database()
        cursor = coll.find().sort("published_at", -1).limit(limit)
        news = [serialize_doc(doc) async for doc in cursor]
        return {
            "success": True,
            "count": len(news),
            "data": news
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/news/symbol/{symbol}")
async def get_news_by_symbol(symbol: str, limit: int = 20):
    """Get news for specific symbol."""
    try:
        coll = await get_database()
        cursor = coll.find({"symbol": symbol.upper()}).sort("published_at", -1).limit(limit)
        news = [serialize_doc(doc) async for doc in cursor]
        return {
            "success": True,
            "symbol": symbol.upper(),
            "count": len(news),
            "data": news
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/news/stats")
async def get_news_stats():
    """Get news statistics by symbol."""
    try:
        coll = await get_database()
        pipeline = [
            {"$group": {
                "_id": "$symbol",
                "count": {"$sum": 1},
                "latest": {"$max": "$published_at"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        stats = [doc async for doc in coll.aggregate(pipeline)]
        total_count = await coll.count_documents({})
        
        return {
            "success": True,
            "total_news": total_count,
            "by_symbol": [
                {
                    "symbol": s["_id"],
                    "count": s["count"],
                    "latest": s["latest"]
                }
                for s in stats
            ]
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/news/search")
async def search_news(query: str, limit: int = 20):
    """Search news by title or description."""
    try:
        coll = await get_database()
        cursor = coll.find({
            "$or": [
                {"title": {"$regex": query, "$options": "i"}},
                {"description": {"$regex": query, "$options": "i"}}
            ]
        }).sort("published_at", -1).limit(limit)
        news = [serialize_doc(doc) async for doc in cursor]
        
        return {
            "success": True,
            "query": query,
            "count": len(news),
            "data": news
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
# WEBSOCKET ENDPOINT
# ==========================================

@app.websocket("/ws/news/live")
async def websocket_news_stream(websocket: WebSocket):
    """WebSocket endpoint for live news updates."""
    await manager.connect(websocket)
    
    try:
        coll = await get_database()
        
        # Send initial news batch
        cursor = coll.find().sort("published_at", -1).limit(10)
        initial_news = [serialize_doc(doc) async for doc in cursor]
        await websocket.send_json({
            "type": "initial",
            "data": initial_news
        })
        
        # Keep track of last seen timestamp
        last_timestamp = datetime.now().timestamp() * 1000
        
        while True:
            await asyncio.sleep(2)  # Poll every 2 seconds
            
            # Check for new news items
            cursor = coll.find({"time": {"$gt": last_timestamp}}).sort("time", 1)
            new_news = [serialize_doc(doc) async for doc in cursor]
            
            for news_item in new_news:
                await websocket.send_json({
                    "type": "new",
                    "data": news_item
                })
                last_timestamp = news_item.get("time", last_timestamp)
    
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
    """Health check endpoint."""
    mongo_status = "disconnected"
    
    try:
        if motor_client:
            await motor_client.admin.command('ping')
            mongo_status = "connected"
    except Exception as e:
        print(f"MongoDB connection error: {e}")
    
    return {
        "status": "running",
        "mongodb": mongo_status,
        "driver": "motor (async)",
        "active_websockets": len(manager.active_connections)
    }