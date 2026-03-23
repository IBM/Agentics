#!/bin/bash

# Quick UDF Update Script
# Updates UDF files in the running Flink container without rebuilding
# This is much faster than a full rebuild for UDF-only changes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UDF_DIR="$SCRIPT_DIR/../udfs"

# Default to ag_operators.py if no argument provided
UDF_FILE="${1:-ag_operators.py}"

echo "=================================================="
echo "Quick UDF Update (No Container Rebuild)"
echo "=================================================="
echo ""
echo "Updating: $UDF_FILE"
echo ""

# Check if Flink container is running
if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: Flink jobmanager container is not running"
    echo "   Start services first: ./manage_services_full.sh start"
    exit 1
fi

# Check if UDF file exists
if [ ! -f "$UDF_DIR/$UDF_FILE" ]; then
    echo "❌ Error: UDF file not found: $UDF_DIR/$UDF_FILE"
    echo ""
    echo "Available UDF files:"
    ls -1 "$UDF_DIR"/*.py 2>/dev/null || echo "  (none found)"
    exit 1
fi

echo "📦 Copying updated UDF to Flink container..."
docker cp "$UDF_DIR/$UDF_FILE" flink-jobmanager:/opt/flink/udfs/

echo "✅ UDF file updated in container!"
echo ""
echo "📝 Note: The UDF is already registered in Flink SQL"
echo "   Changes will take effect on next query execution"
echo ""
echo "🔄 If you need to re-register the UDF, run the appropriate SQL:"
echo "   - For ag.map: See sql/init_ag_operators.sql"
echo "   - For agmap: See sql/init_agmap.sql"
echo ""
echo "✨ Quick update complete! No container rebuild needed."

# Made with Bob
