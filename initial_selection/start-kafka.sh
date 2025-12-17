#!/bin/bash

set -e  # Exit on any error

echo "=========================================="
echo "KAFKA CLUSTER FRESH START"
echo "=========================================="
echo ""

# Pull latest images first
echo "  Pulling latest images..."
docker-compose -f docker-compose.yml pull

echo ""
echo "  Starting Zookeeper..."
docker-compose -f docker-compose.yml up -d zookeeper

echo "  Waiting for Zookeeper (30 seconds)..."
sleep 30

# Verify Zookeeper
if ! docker-compose -f docker-compose.yml ps zookeeper | grep -q "healthy"; then
    echo "  Zookeeper failed!"
    docker-compose -f docker-compose.yml logs zookeeper
    exit 1
fi
echo "  Zookeeper is healthy"
echo ""

echo "  Starting Kafka..."
docker-compose -f docker-compose.yml up -d kafka

echo "  Waiting for Kafka (40 seconds)..."
sleep 40

# Verify Kafka
if ! docker-compose -f docker-compose.yml ps kafka | grep -q "healthy"; then
    echo "  Kafka failed!"
    docker-compose -f docker-compose.yml logs kafka
    exit 1
fi
echo "  Kafka is healthy"
echo ""

echo "  Starting all other services..."
docker-compose -f docker-compose.yml up -d

echo ""
echo "=========================================="
echo "  CLUSTER STARTED SUCCESSFULLY"
echo "=========================================="
echo ""
echo "Services:"
echo "  - Kafka: localhost:9092"
echo "  - Kafka UI: http://localhost:8080"
echo ""
echo "Check logs:"
echo "  docker-compose -f docker-compose.yml logs -f"
echo ""
echo "Check producer:"
echo "  docker-compose -f docker-compose.yml logs -f replay-producer"
echo ""
echo "Check consumer:"
echo "  docker-compose -f docker-compose.yml logs -f consumer-stocks"
echo ""