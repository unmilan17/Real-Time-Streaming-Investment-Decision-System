#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                  🍃 MONGODB LOGS (LIVE)                        ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "This shows MongoDB database operations"
echo "Press Ctrl+C to exit"
echo ""

docker-compose logs -f --tail=100 mongodb