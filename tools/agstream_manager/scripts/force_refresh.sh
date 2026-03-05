#!/bin/bash
# Force refresh script for AGstream Manager

echo "🔄 Forcing browser cache clear..."
echo ""
echo "Step 1: Kill any running agstream_manager_service.py processes"
pkill -f "agstream_manager_service.py"
sleep 1

echo "Step 2: Start the service"
cd "$(dirname "$0")"
python agstream_manager_service.py &
SERVICE_PID=$!
echo "   Service started with PID: $SERVICE_PID"
sleep 2

echo ""
echo "Step 3: Open browser with cache-busting URL"
echo "   Opening: http://localhost:5003/?v=$(date +%s)"
open "http://localhost:5003/?v=$(date +%s)"

echo ""
echo "✅ Done! The page should open with a fresh version."
echo ""
echo "If you still see the old version:"
echo "1. Press Cmd+Shift+R (Mac) or Ctrl+Shift+R (Windows/Linux) to hard refresh"
echo "2. Or open DevTools (F12) → Network tab → Check 'Disable cache'"
echo "3. Then refresh the page"
echo ""
echo "To stop the service later: kill $SERVICE_PID"

# Made with Bob
