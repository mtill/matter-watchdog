#!/bin/bash
set -e

PROJECT_DIR="/home/beck/matter-watchdog"
cd "$PROJECT_DIR"

echo "Stopping existing containers cleanly..."
# 'down' removes network bindings, frees up /dev/ttyUSB0 and DBus sockets, 
# and prevents resource conflicts before updating.
docker compose down

echo "Pulling latest container images..."
docker compose pull

echo "Starting updated services..."
docker compose up -d

echo "Pruning old, unused images..."
docker image prune -f

