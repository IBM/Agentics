#!/bin/bash

# Rebuild Flink Docker Image with All Dependencies
# This ensures Agentics and all UDFs work without manual installation
#
# Usage:
#   ./rebuild_flink_image.sh          # Incremental build (uses cache)
#   ./rebuild_flink_image.sh --force  # Full rebuild (no cache)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Parse arguments
FORCE_REBUILD=false
if [[ "$1" == "--force" ]]; then
    FORCE_REBUILD=true
fi

echo "=================================================="
echo "Rebuild Flink Docker Image"
echo "=================================================="
echo ""

if [ "$FORCE_REBUILD" = true ]; then
    echo "🔄 FULL REBUILD MODE (--force flag detected)"
    echo "   This will rebuild from scratch without using cache"
else
    echo "⚡ INCREMENTAL BUILD MODE (default)"
    echo "   Using Docker cache for faster builds"
    echo "   Use --force flag for full rebuild"
fi
echo ""

cd "$SCRIPT_DIR/.."

echo "📦 Building custom Flink image with Agentics dependencies..."
echo "   This includes: hnswlib, scikit-learn, and all required packages"
echo ""

# Build the image with or without cache
if [ "$FORCE_REBUILD" = true ]; then
    docker build --no-cache -f Dockerfile.flink-python -t flink-python:1.18.1 "$PROJECT_ROOT"
else
    docker build -f Dockerfile.flink-python -t flink-python:1.18.1 "$PROJECT_ROOT"
fi

echo ""
echo "✅ Docker image rebuilt successfully!"
echo ""
echo "⚠️  IMPORTANT: Services are NOT automatically restarted!"
echo ""
echo "Next steps:"
echo "1. Restart services to use the new image:"
echo "   cd tools/agstream_manager"
echo "   ./manage_services_full.sh restart"
echo ""
echo "2. Or use the 'build' command which rebuilds AND restarts:"
echo "   ./manage_services_full.sh build"
echo ""
echo "3. The new image includes:"
echo "   - All Agentics dependencies (hnswlib, scikit-learn, etc.)"
echo "   - UDFs pre-installed"
echo "   - No need for manual installation after restarts"
echo ""

# Made with Bob
