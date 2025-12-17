#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              🛑 STOPPING ALL SERVICES                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "⏸️  Stopping all containers..."
docker-compose down

echo ""
echo "✅ All services stopped!"
echo ""
echo "💡 Data preserved in:"
echo "   ├─ MongoDB volume (persistent)"
echo "   └─ ./output-replay/ directory"
echo ""
echo "To start again: ./start-all.sh"
echo ""