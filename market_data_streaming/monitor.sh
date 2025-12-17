#!/bin/bash

echo "=============================================="
echo "  KAFKA HYBRID SYSTEM MONITOR"
echo "=============================================="
echo ""

# Check consumer group lag
echo "CONSUMER GROUP LAG:"
echo "-------------------"
echo ""

echo "Stocks Group:"
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe \
  --group stocks-group

echo ""
echo "Crypto Group:"
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe \
  --group crypto-group

echo ""
echo "=============================================="
echo "PARTITION DISTRIBUTION:"
echo "-------------------"
echo ""

# Check partition stats for stocks
echo "Stocks Topic Messages per Partition:"
docker exec kafka-broker-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:19092 \
  --topic market-data-stocks

echo ""
echo "Crypto Topic Messages per Partition:"
docker exec kafka-broker-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:19092 \
  --topic market-data-crypto

echo ""
echo "=============================================="
echo "OUTPUT FILE SIZES:"
echo "-------------------"
echo ""

du -sh consumer_output/stocks-*/* 2>/dev/null || echo "No stock consumer output yet"
du -sh consumer_output/crypto-*/* 2>/dev/null || echo "No crypto consumer output yet"
du -sh consumer_output/commodities/* 2>/dev/null || echo "No commodity consumer output yet"
du -sh consumer_output/forex/* 2>/dev/null || echo "No forex consumer output yet"

echo ""
echo "=============================================="
echo "CONTAINER STATUS:"
echo "-------------------"
echo ""
docker-compose ps

echo ""
echo "=============================================="
echo ""
echo "To watch live logs:"
echo "  docker-compose logs -f producer"
echo "  docker-compose logs -f consumer-stocks-1"
echo ""
echo "To view Kafka UI:"
echo "  open http://localhost:8080"
echo ""