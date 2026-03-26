#!/bin/bash
cd "$(dirname "$0")/.."
nohup python3 notification_service.py > notification_service.log 2>&1 &
echo "Service started, PID: $!"
echo $! > notification_service.pid