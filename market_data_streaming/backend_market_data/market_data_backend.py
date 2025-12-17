from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Optional
import json
import asyncio
import os
from datetime import datetime
from pathlib import Path

app = FastAPI(title="Market Data Streaming API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
OUTPUT_BASE_DIR = os.getenv("OUTPUT_BASE_DIR", "./consumer_output")

# In-memory cache for latest market data
market_data_cache: Dict[str, Dict] = {}
cache_lock = asyncio.Lock()

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                disconnected.append(connection)
        
        # Remove disconnected clients
        for conn in disconnected:
            try:
                self.active_connections.remove(conn)
            except:
                pass

manager = ConnectionManager()

# Helper functions
def read_latest_market_data(category: str = "stocks") -> List[Dict]:
    """Read latest market data from consumer output files"""
    try:
        # Try different consumer output directories
        consumer_dirs = [
            f"{OUTPUT_BASE_DIR}/{category}-1",
            f"{OUTPUT_BASE_DIR}/{category}-2",
            f"{OUTPUT_BASE_DIR}/{category}-3",
            f"{OUTPUT_BASE_DIR}/{category}"
        ]
        
        all_data = {}
        
        for consumer_dir in consumer_dirs:
            jsonl_path = Path(consumer_dir) / "enriched_data.jsonl"
            
            if not jsonl_path.exists():
                continue
            
            # Read last N lines for latest data
            with open(jsonl_path, 'r') as f:
                lines = f.readlines()
                recent_lines = lines[-100:] if len(lines) > 100 else lines
                
                for line in recent_lines:
                    try:
                        data = json.loads(line.strip())
                        symbol = data.get('symbol')
                        
                        # Keep only the latest entry per symbol
                        if symbol:
                            if symbol not in all_data or data.get('timestamp', 0) > all_data[symbol].get('timestamp', 0):
                                all_data[symbol] = data
                    except:
                        continue
        
        return list(all_data.values())
    
    except Exception as e:
        print(f"Error reading market data: {e}")
        return []

def calculate_price_change(current: float, previous: float) -> Dict:
    """Calculate price change and percentage"""
    if previous == 0:
        return {"change": 0, "change_percent": 0}
    
    change = current - previous
    change_percent = (change / previous) * 100
    
    return {
        "change": round(change, 2),
        "change_percent": round(change_percent, 2)
    }

async def update_market_cache():
    """Background task to update market data cache"""
    while True:
        try:
            async with cache_lock:
                # Update stocks
                stocks_data = read_latest_market_data("stocks")
                for stock in stocks_data:
                    symbol = stock.get('symbol')
                    if symbol:
                        # Calculate price change if we have previous data
                        if symbol in market_data_cache:
                            prev_price = market_data_cache[symbol].get('price', stock.get('price'))
                            change_data = calculate_price_change(stock.get('price', 0), prev_price)
                            stock.update(change_data)
                        
                        market_data_cache[symbol] = stock
                
                # Update crypto
                crypto_data = read_latest_market_data("crypto")
                for crypto in crypto_data:
                    symbol = crypto.get('symbol')
                    if symbol:
                        if symbol in market_data_cache:
                            prev_price = market_data_cache[symbol].get('price', crypto.get('price'))
                            change_data = calculate_price_change(crypto.get('price', 0), prev_price)
                            crypto.update(change_data)
                        
                        market_data_cache[symbol] = crypto
            
            # Broadcast updates via WebSocket
            if market_data_cache:
                await manager.broadcast({
                    "type": "market_update",
                    "data": list(market_data_cache.values()),
                    "timestamp": int(datetime.now().timestamp() * 1000)
                })
        
        except Exception as e:
            print(f"Cache update error: {e}")
        
        await asyncio.sleep(2)  # Update every 2 seconds

# REST Endpoints

@app.get("/api/market/leaderboard")
async def get_market_leaderboard(
    category: Optional[str] = "all",
    sort_by: Optional[str] = "volume",
    limit: int = 50
):
    """
    Get market leaderboard
    category: all, stocks, crypto
    sort_by: volume, price, change_percent
    """
    try:
        async with cache_lock:
            data = list(market_data_cache.values())
        
        # Filter by category if specified
        if category != "all":
            # Crypto symbols typically contain certain patterns
            if category == "crypto":
                data = [d for d in data if any(x in d.get('symbol', '') for x in ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOGE', 'XRP', 'MATIC'])]
            elif category == "stocks":
                data = [d for d in data if not any(x in d.get('symbol', '') for x in ['BTC', 'ETH', 'BNB', 'SOL', 'ADA', 'DOGE', 'XRP', 'MATIC'])]
        
        # Sort data
        if sort_by == "volume":
            data.sort(key=lambda x: x.get('volume', 0), reverse=True)
        elif sort_by == "price":
            data.sort(key=lambda x: x.get('price', 0), reverse=True)
        elif sort_by == "change_percent":
            data.sort(key=lambda x: x.get('change_percent', 0), reverse=True)
        
        # Add rank
        for idx, item in enumerate(data[:limit], 1):
            item['rank'] = idx
        
        return {
            "success": True,
            "category": category,
            "sort_by": sort_by,
            "count": len(data[:limit]),
            "data": data[:limit]
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/market/symbol/{symbol}")
async def get_symbol_data(symbol: str):
    """Get latest data for specific symbol"""
    try:
        async with cache_lock:
            data = market_data_cache.get(symbol.upper())
        
        if data:
            return {
                "success": True,
                "symbol": symbol.upper(),
                "data": data
            }
        else:
            return {
                "success": False,
                "error": "Symbol not found"
            }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/market/top-movers")
async def get_top_movers(limit: int = 10):
    """Get top gainers and losers"""
    try:
        async with cache_lock:
            data = list(market_data_cache.values())
        
        # Filter data with change_percent
        data_with_change = [d for d in data if 'change_percent' in d]
        
        # Get top gainers
        gainers = sorted(data_with_change, key=lambda x: x.get('change_percent', 0), reverse=True)[:limit]
        
        # Get top losers
        losers = sorted(data_with_change, key=lambda x: x.get('change_percent', 0))[:limit]
        
        return {
            "success": True,
            "gainers": gainers,
            "losers": losers
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/market/stats")
async def get_market_stats():
    """Get overall market statistics"""
    try:
        async with cache_lock:
            data = list(market_data_cache.values())
        
        if not data:
            return {"success": True, "stats": {}}
        
        total_volume = sum(d.get('volume', 0) for d in data)
        avg_price = sum(d.get('price', 0) for d in data) / len(data)
        
        # Calculate gainers/losers count
        gainers = sum(1 for d in data if d.get('change_percent', 0) > 0)
        losers = sum(1 for d in data if d.get('change_percent', 0) < 0)
        unchanged = len(data) - gainers - losers
        
        return {
            "success": True,
            "stats": {
                "total_symbols": len(data),
                "total_volume": round(total_volume, 2),
                "avg_price": round(avg_price, 2),
                "gainers": gainers,
                "losers": losers,
                "unchanged": unchanged
            }
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/market/search")
async def search_symbols(query: str, limit: int = 20):
    """Search symbols by name"""
    try:
        async with cache_lock:
            data = list(market_data_cache.values())
        
        # Filter by query
        results = [
            d for d in data 
            if query.upper() in d.get('symbol', '').upper()
        ][:limit]
        
        return {
            "success": True,
            "query": query,
            "count": len(results),
            "data": results
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}

# WebSocket endpoint
@app.websocket("/ws/market/live")
async def websocket_market_stream(websocket: WebSocket):
    """WebSocket endpoint for live market data updates"""
    await manager.connect(websocket)
    
    try:
        # Send initial data
        async with cache_lock:
            initial_data = list(market_data_cache.values())
        
        await websocket.send_json({
            "type": "initial",
            "data": initial_data,
            "timestamp": int(datetime.now().timestamp() * 1000)
        })
        
        # Keep connection alive and listen for messages
        while True:
            try:
                # Wait for any message from client (ping/pong)
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                await websocket.send_json({"type": "ping"})
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    """Start background tasks on startup"""
    asyncio.create_task(update_market_cache())

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    async with cache_lock:
        cache_size = len(market_data_cache)
    
    return {
        "status": "running",
        "cached_symbols": cache_size,
        "active_websockets": len(manager.active_connections)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)