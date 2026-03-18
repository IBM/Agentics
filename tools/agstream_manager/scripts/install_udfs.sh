#!/bin/bash
# Install Python UDFs to Flink container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UDF_DIR="$SCRIPT_DIR/../udfs"
UDF_FILE="$UDF_DIR/udfs_example.py"

echo "📦 Installing Python UDFs to Flink container..."
echo ""

# Check if UDF file exists
if [ ! -f "$UDF_FILE" ]; then
    echo "❌ Error: UDF file not found at $UDF_FILE"
    exit 1
fi

# Check if Flink container is running
if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: Flink container 'flink-jobmanager' is not running"
    echo "   Start it with: docker-compose up -d"
    exit 1
fi

# Copy UDF file to Flink containers (both JobManager and TaskManager)
echo "📋 Copying udfs_example.py to Flink JobManager..."
docker cp "$UDF_FILE" flink-jobmanager:/opt/flink/udfs_example.py

echo "📋 Copying udfs_example.py to Flink TaskManager..."
docker cp "$UDF_FILE" flink-taskmanager:/opt/flink/udfs_example.py

if [ $? -eq 0 ]; then
    echo "✅ UDFs installed successfully to both JobManager and TaskManager!"
    echo ""
    echo "📝 Now you can register them in Flink SQL:"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION add_prefix"
    echo "   AS 'udfs_example.add_prefix'"
    echo "   LANGUAGE PYTHON;"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION uppercase_text"
    echo "   AS 'udfs_example.uppercase_text'"
    echo "   LANGUAGE PYTHON;"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION format_with_confidence"
    echo "   AS 'udfs_example.format_with_confidence'"
    echo "   LANGUAGE PYTHON;"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION word_count"
    echo "   AS 'udfs_example.word_count'"
    echo "   LANGUAGE PYTHON;"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION normalize_score"
    echo "   AS 'udfs_example.normalize_score'"
    echo "   LANGUAGE PYTHON;"
    echo ""
    echo "🎯 Then use them in queries:"
    echo "   SELECT add_prefix(text) FROM Q3;"
else
    echo "❌ Failed to copy UDFs to container"
    exit 1
fi

# Made with Bob
