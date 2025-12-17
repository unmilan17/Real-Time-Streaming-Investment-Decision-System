#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         📥 NEWS CONSUMER + MONGODB LOGS (LIVE)                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "This shows the consumer processing Kafka messages"
echo "and writing to CSV, JSONL, and MongoDB"
echo "Press Ctrl+C to exit"
echo ""

docker-compose logs -f --tail=100 consumer-stocks