import os
import pathway as pw
from .schemas import MarketDataSchema

KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("TOPIC_NAME", "market-data")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "./output")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def run_pipeline():
    market_data = pw.io.kafka.read(
        rdkafka_settings={
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "pathway-consumer",
            "auto.offset.reset": "earliest",
        },
        topic=KAFKA_TOPIC,
        format="json",
        schema=MarketDataSchema,
        autocommit_duration_ms=1000
    )

    enriched_data = market_data.select(
        *pw.this,
        spread=pw.this.ask - pw.this.bid,
        mid_price=(pw.this.ask + pw.this.bid)/2
    )

    symbol_stats = enriched_data.groupby(pw.this.symbol).reduce(
        symbol=pw.this.symbol,
        count=pw.reducers.count(),
        avg_price=pw.reducers.avg(pw.this.price),
        max_price=pw.reducers.max(pw.this.price),
        min_price=pw.reducers.min(pw.this.price),
        total_volume=pw.reducers.sum(pw.this.volume),
    )

    symbol_stats_enriched = symbol_stats.select(
        *pw.this,
        price_range=pw.this.max_price - pw.this.min_price,
        avg_volume=pw.this.total_volume / pw.this.count
    )

    pw.io.jsonlines.write(enriched_data, f"{OUTPUT_FOLDER}/enriched_market_data.jsonl")
    pw.io.jsonlines.write(symbol_stats_enriched, f"{OUTPUT_FOLDER}/symbol_statistics.jsonl")

    return enriched_data
