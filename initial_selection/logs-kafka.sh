#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                  📨 KAFKA BROKER LOGS (LIVE)                   ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "This shows the Kafka broker operations"
echo "Press Ctrl+C to exit"
echo ""

docker-compose logs -f --tail=100 kafka