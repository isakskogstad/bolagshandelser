#!/bin/bash
# Bolagshändelser Launcher - starts server + opens browser
cd "$(dirname "$0")"

# Kill old server if running
pkill -f "bolagshandelser-server.py" 2>/dev/null

# Start server in background
python3 "$(dirname "$0")/bolagshandelser-server.py" &
SERVER_PID=$!

# Wait for server
for i in $(seq 1 15); do
    if curl -s http://localhost:9999/api/fetch > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Open the HTML
open "$(dirname "$0")/bolagshandelser-live.html"

# Keep running until user closes terminal
wait $SERVER_PID
