#!/bin/bash

echo "=============================================="
echo "  HYBRID ASYNC + PARTITIONS SETUP SCRIPT"
echo "=============================================="
echo ""

# Create output directories
echo "Creating output directories..."
mkdir -p consumer_output/stocks-1
mkdir -p consumer_output/stocks-2
mkdir -p consumer_output/stocks-3
mkdir -p consumer_output/crypto-1
mkdir -p consumer_output/crypto-2
mkdir -p consumer_output/crypto-3
mkdir -p consumer_output/commodities
mkdir -p consumer_output/forex

echo "Output directories created."
echo ""

# Stop any existing containers
echo "Stopping existing containers..."
docker-compose down -v
echo ""

# Build and start services
echo "Building and starting services..."
echo "This may take a few minutes..."
docker-compose up --build -d

echo ""
echo "Waiting for Kafka cluster to be ready (30 seconds)..."
sleep 30

# Create topics with 3 partitions explicitly
echo ""
echo "Creating Kafka topics with 3 partitions..."

docker exec kafka-broker-1 kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server localhost:19092 \
  --replication-factor 3 \
  --partitions 3 \
  --topic market-data-stocks

docker exec kafka-broker-1 kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server localhost:19092 \
  --replication-factor 3 \
  --partitions 3 \
  --topic market-data-crypto

docker exec kafka-broker-1 kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server localhost:19092 \
  --replication-factor 3 \
  --partitions 3 \
  --topic market-data-commodities

docker exec kafka-broker-1 kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server localhost:19092 \
  --replication-factor 3 \
  --partitions 3 \
  --topic market-data-forex

echo ""
echo "Verifying topics..."
docker exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server localhost:19092

echo ""
echo "Topic details:"
docker exec kafka-broker-1 kafka-topics \
  --describe \
  --bootstrap-server localhost:19092 \
  --topic market-data-stocks

echo ""
echo "=============================================="
echo "  DEPLOYMENT COMPLETE!"
echo "=============================================="
echo ""
echo "Services running:"
echo "  - 3 Kafka brokers"
echo "  - 1 Async producer (10 parallel workers)"
echo "  - 3 Stock consumers (1 per partition)"
echo "  - 3 Crypto consumers (1 per partition)"
echo "  - 1 Commodities consumer"
echo "  - 1 Forex consumer"
echo ""
echo "Kafka UI: http://localhost:8080"
echo ""
echo "Output directories:"
echo "  ./consumer_output/stocks-1/"
echo "  ./consumer_output/stocks-2/"
echo "  ./consumer_output/stocks-3/"
echo "  ./consumer_output/crypto-1/"
echo "  ./consumer_output/crypto-2/"
echo "  ./consumer_output/crypto-3/"
echo "  ./consumer_output/commodities/"
echo "  ./consumer_output/forex/"
echo ""
echo "Useful commands:"
echo "  docker-compose logs -f producer          # Watch producer logs"
echo "  docker-compose logs -f consumer-stocks-1 # Watch consumer logs"
echo "  docker-compose ps                        # Check status"
echo "  docker-compose down                      # Stop everything"
echo ""