#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              📊 SYSTEM STATUS CHECK                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Function to check service status
check_service() {
    local service=$1
    local status=$(docker-compose ps $service 2>/dev/null | grep -o "Up\|Exit\|healthy\|unhealthy" | head -1)
    
    if [[ $status == "Up" ]] || [[ $status == "healthy" ]]; then
        echo "✅ $service: Running"
    elif [[ $status == "Exit" ]] || [[ $status == "unhealthy" ]]; then
        echo "❌ $service: Stopped/Unhealthy"
    else
        echo "⚠️  $service: Not found"
    fi
}

echo "🔍 Container Status:"
check_service "mongodb"
check_service "mongo-express"
check_service "zookeeper"
check_service "kafka"
check_service "kafka-ui"
check_service "replay-producer"
check_service "consumer-stocks"
echo ""

echo "🌐 Web Interfaces:"
echo "   ├─ Kafka UI:       http://localhost:8080"
echo "   └─ Mongo Express:  http://localhost:8081 (admin/admin)"
echo ""

echo "📊 Quick Stats:"
echo "   ├─ Running Containers: $(docker-compose ps | grep -c "Up")"
echo "   └─ Docker Disk Usage:  $(docker system df --format "{{.Size}}" | head -1)"
echo ""

echo "📂 Output Files:"
if [ -f "./output-replay/streamed_news_data.csv" ]; then
    csv_lines=$(wc -l < ./output-replay/streamed_news_data.csv)
    echo "   ├─ CSV Records:    $((csv_lines - 1))"
else
    echo "   ├─ CSV Records:    0 (file not found)"
fi

if [ -f "./output-replay/news_data.jsonl" ]; then
    json_lines=$(wc -l < ./output-replay/news_data.jsonl)
    echo "   └─ JSONL Records:  $json_lines"
else
    echo "   └─ JSONL Records:  0 (file not found)"
fi
echo ""

echo "🍃 MongoDB Status:"
mongo_count=$(docker exec mongodb mongosh --quiet --eval "db.getSiblingDB('news_database').news_collection.countDocuments({})" 2>/dev/null || echo "N/A")
echo "   └─ Documents in DB: $mongo_count"
echo ""

echo "💡 Use these commands:"
echo "   ├─ View all logs:      ./logs-all.sh"
echo "   ├─ View producer:      ./logs-producer.sh"
echo "   ├─ View consumer:      ./logs-consumer.sh"
echo "   └─ Stop everything:    ./stop-all.sh"
echo ""