#!/bin/bash
set -e

PROJECT_DIR="/home/beck/matter-watchdog"
cd "$PROJECT_DIR"

echo "Checking and pulling latest images..."
# This downloads new layers but doesn't touch the running containers yet
docker compose pull

echo "Restarting updated services cleanly..."
# 'down' removes the host network mappings and frees up /dev/ttyUSB0 / DBus sockets.
# 'up -d' spins up the fresh images cleanly without resource conflicts.
docker compose down && docker compose up -d

echo "Pruning old, unused images..."
docker image prune -f

