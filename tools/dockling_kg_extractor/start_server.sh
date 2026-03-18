#!/bin/bash

# Knowledge Graph Extractor - Server Startup Script
# This script kills any existing server on port 5000 and starts a new one

PORT=5000
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$APP_DIR/.venv"

echo "🧠 Docling-Agentics Knowledge Graph Extractor"
echo "=============================================="

# Check if virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo "❌ Error: Virtual environment not found at $VENV_DIR"
    exit 1
fi

# Kill any existing process on port 5000
echo "🔍 Checking for existing server on port $PORT..."
PID=$(lsof -ti:$PORT)

if [ ! -z "$PID" ]; then
    echo "⚠️  Found existing server (PID: $PID). Killing it..."
    kill -9 $PID 2>/dev/null
    sleep 1
    echo "✅ Previous server stopped"
else
    echo "✅ No existing server found"
fi

# Start server using uv run
echo "🚀 Starting FastAPI server with Uvicorn..."
echo ""

cd "$APP_DIR"

# Start the server using uv run to ensure correct environment
uv run python app_simple.py

# If the script exits (Ctrl+C), clean up
echo ""
echo "👋 Server stopped"

# Made with Bob
