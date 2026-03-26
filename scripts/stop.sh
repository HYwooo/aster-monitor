#!/bin/bash
cd "$(dirname "$0")/.."
if [ -f notification_service.pid ]; then
    PID=$(cat notification_service.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID
        echo "Service stopped, PID: $PID"
    else
        echo "Process not running"
    fi
    rm -f notification_service.pid
else
    echo "PID file not found, trying to kill by process name..."
    pkill -f "python3 notification_service.py" && echo "Service stopped" || echo "Service not found"
fi