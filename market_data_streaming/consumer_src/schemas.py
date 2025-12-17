import pathway as pw

class MarketDataSchema(pw.Schema):
    timestamp: int
    symbol: str
    price: float
    volume: int
    bid: float
    ask: float

class EnrichedDataSchema(pw.Schema):
    timestamp: int
    symbol: str
    price: float
    volume: int
    bid: float
    ask: float
    spread: float
    mid_price: float

class SymbolStatsSchema(pw.Schema):
    symbol: str
    count: int
    avg_price: float
    max_price: float
    min_price: float
    total_volume: int
    price_range: float
    avg_volume: float
