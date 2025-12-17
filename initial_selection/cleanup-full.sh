#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          ⚠️  COMPLETE CLEANUP (INCLUDING DATA)                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "⚠️  WARNING: This will delete:"
echo "   ├─ All Docker containers"
echo "   ├─ All Docker volumes (MongoDB data will be lost!)"
echo "   ├─ All Docker networks"
echo "   └─ Output files in ./output-replay/"
echo ""

read -p "Are you sure? (yes/no): " confirm

if [[ $confirm != "yes" ]]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

echo ""
echo "🧹 Starting cleanup..."
echo ""

# Stop all services
echo "1️⃣  Stopping all services..."
docker-compose down --volumes --remove-orphans 2>/dev/null || true
sleep 3

# Kill any running containers
echo "2️⃣  Killing all containers..."
docker ps -q | xargs -r docker kill 2>/dev/null || true
sleep 2

# Remove all containers
echo "3️⃣  Removing all containers..."
docker ps -aq | xargs -r docker rm -f 2>/dev/null || true
sleep 2

# Remove ALL volumes
echo "4️⃣  Removing ALL Docker volumes..."
docker volume ls -q | xargs -r docker volume rm -f 2>/dev/null || true
sleep 2

# Prune everything
echo "5️⃣  Pruning Docker system..."
docker system prune -af --volumes
sleep 3

# Remove networks
echo "6️⃣  Removing networks..."
docker network prune -f
sleep 2

# Clean output directory
echo "7️⃣  Cleaning output directory..."
rm -rf ./output-replay/*
mkdir -p ./output-replay

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              ✅ COMPLETE CLEANUP DONE!                         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Final Status:"
echo "   ├─ Docker containers: $(docker ps -a | wc -l) (should be 1 - header only)"
echo "   ├─ Docker volumes:    $(docker volume ls | wc -l) (should be 1 - header only)"
echo "   └─ Output directory:  Empty"
echo ""
echo "🚀 To start fresh: ./start-all.sh"
echo ""