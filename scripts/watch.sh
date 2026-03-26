#!/bin/bash

SERVICE_DIR="$(cd "$(dirname "$0")" && pwd)/.."
HEARTBEAT_FILE="$SERVICE_DIR/notification_heartbeat"
TIMEOUT=120

if [ ! -f "$HEARTBEAT_FILE" ]; then
    echo "[$(date)] Heartbeat file not found, service may not be running"
    exit 1
fi

LAST_HEARTBEAT=$(cat "$HEARTBEAT_FILE")
NOW=$(date +%s)
ELAPSED=$((NOW - LAST_HEARTBEAT))

if [ $ELAPSED -gt $TIMEOUT ]; then
    echo "[$(date)] Heartbeat timeout (${ELAPSED}s), restarting service..."

    cd "$SERVICE_DIR"

    if [ -f notification_service.pid ]; then
        PID=$(cat notification_service.pid)
        if kill -0 $PID 2>/dev/null; then
            kill $PID
            sleep 2
        fi
        rm -f notification_service.pid
    fi

    nohup python3 notification_service.py >> notification_service.log 2>&1 &
    NEW_PID=$!
    echo $NEW_PID > notification_service.pid
    echo "[$(date)] Service restarted, new PID: $NEW_PID"
else
    echo "[$(date)] Heartbeat OK (${ELAPSED}s)"
fi