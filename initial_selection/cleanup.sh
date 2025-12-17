#!/bin/bash

echo "=========================================="
echo "KAFKA CLUSTER COMPLETE NUCLEAR CLEANUP"
echo "=========================================="
echo ""

# Stop all services
echo "1️⃣ Stopping all services..."
docker-compose down --volumes --remove-orphans 2>/dev/null || true
docker-compose -f docker-compose-simple.yml down --volumes --remove-orphans 2>/dev/null || true
sleep 3

# Kill any running containers
echo "2️⃣ Killing all containers..."
docker ps -q | xargs -r docker kill 2>/dev/null || true
sleep 2

# Remove all containers
echo "3️⃣ Removing all containers..."
docker ps -aq | xargs -r docker rm -f 2>/dev/null || true
sleep 2

# Remove ALL volumes (not just project-specific)
echo "4️⃣ Removing ALL Docker volumes..."
docker volume ls -q | xargs -r docker volume rm -f 2>/dev/null || true
sleep 2

# Prune everything
echo "5️⃣ Pruning Docker system..."
docker system prune -af --volumes
sleep 3

# Remove networks
echo "6️⃣ Removing networks..."
docker network prune -f
sleep 2

# Remove any cached images
echo "7️⃣ Removing Kafka/Zookeeper images..."
docker images | grep -E "kafka|zookeeper" | awk '{print $3}' | xargs -r docker rmi -f 2>/dev/null || true
sleep 2

# Final verification
echo ""
echo "=========================================="
echo "VERIFICATION"
echo "=========================================="
echo "Docker containers: $(docker ps -a | wc -l) (should be 1 - header only)"
echo "Docker volumes: $(docker volume ls | wc -l) (should be 1 - header only)"
echo "Docker images with kafka/zookeeper: $(docker images | grep -E 'kafka|zookeeper' | wc -l)"
echo ""

# Show current disk usage
docker system df

echo ""
echo "=========================================="
echo "✅ COMPLETE CLEANUP DONE!"
echo "=========================================="
echo "Now run: ./start-kafka.sh"
echo ""