#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              📋 ALL SERVICES LOGS (LIVE)                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Press Ctrl+C to exit"
echo ""

docker-compose logs -f --tail=50