#!/bin/bash
# Restart AGStream Manager Backend
# Kills all instances and starts fresh to avoid Python module caching issues

set -e

echo "=========================================="
echo "Restarting AGStream Manager Backend"
echo "=========================================="
echo ""

# Step 1: Kill all running instances
echo "Step 1: Stopping all AGStream Manager instances..."
pkill -9 -f agstream_manager_service 2>/dev/null || echo "  No running instances found"
sleep 2

# Verify all killed
if pgrep -f agstream_manager_service > /dev/null; then
    echo "  ⚠️  Warning: Some processes still running, trying again..."
    pkill -9 -f agstream_manager_service 2>/dev/null
    sleep 2
fi

echo "  ✅ All instances stopped"
echo ""

# Step 2: Start fresh instance
echo "Step 2: Starting AGStream Manager..."
cd "$(dirname "$0")/../../.."  # Go to project root

# Start in background
nohup python tools/agstream_manager/backend/agstream_manager_service.py > /tmp/agstream_manager.log 2>&1 &
BACKEND_PID=$!

echo "  ✅ Started with PID: $BACKEND_PID"
echo "  📝 Logs: /tmp/agstream_manager.log"
echo ""

# Step 3: Wait for startup
echo "Step 3: Waiting for backend to start..."
sleep 3

# Check if it's running
if ps -p $BACKEND_PID > /dev/null; then
    echo "  ✅ Backend is running"
else
    echo "  ❌ Backend failed to start. Check logs:"
    tail -20 /tmp/agstream_manager.log
    exit 1
fi

echo ""
echo "=========================================="
echo "AGStream Manager Restarted Successfully!"
echo "=========================================="
echo ""
echo "🌐 Open: http://localhost:5003"
echo "📝 Logs: tail -f /tmp/agstream_manager.log"
echo "🛑 Stop: pkill -f agstream_manager_service"
echo ""

# Made with Bob
