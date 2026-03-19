#!/bin/bash
# Install Python UDFs to Flink container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGSTREAM_DIR="$SCRIPT_DIR/.."

# Load environment variables from .env file
if [ -f "$AGSTREAM_DIR/.env" ]; then
    export $(grep -v '^#' "$AGSTREAM_DIR/.env" | xargs)
fi

# Set defaults if not in .env
UDF_FOLDER="${UDF_FOLDER:-udfs}"
FLINK_JOBMANAGER_CONTAINER="${FLINK_JOBMANAGER_CONTAINER:-flink-jobmanager}"
FLINK_TASKMANAGER_CONTAINER="${FLINK_TASKMANAGER_CONTAINER:-flink-taskmanager}"

# Resolve UDF directory (support relative and absolute paths)
if [[ "$UDF_FOLDER" = /* ]]; then
    # Absolute path
    UDF_DIR="$UDF_FOLDER"
else
    # Relative path from agstream_manager directory
    UDF_DIR="$AGSTREAM_DIR/$UDF_FOLDER"
fi

echo "📦 Installing Python UDFs to Flink containers..."
echo "   UDF Directory: $UDF_DIR"
echo ""

# Check if UDF directory exists
if [ ! -d "$UDF_DIR" ]; then
    echo "❌ Error: UDF directory not found at $UDF_DIR"
    echo "   Check UDF_FOLDER setting in .env file"
    exit 1
fi

# Find all Python files in UDF directory
UDF_FILES=$(find "$UDF_DIR" -maxdepth 1 -name "*.py" -type f)

if [ -z "$UDF_FILES" ]; then
    echo "❌ Error: No Python files found in $UDF_DIR"
    exit 1
fi

echo "Found UDF files:"
echo "$UDF_FILES" | while read -r file; do
    echo "  - $(basename "$file")"
done
echo ""

# Check if Flink containers are running
if ! docker ps | grep -q "$FLINK_JOBMANAGER_CONTAINER"; then
    echo "❌ Error: Flink container '$FLINK_JOBMANAGER_CONTAINER' is not running"
    echo "   Start it with: ./manage_services_full.sh start"
    exit 1
fi

if ! docker ps | grep -q "$FLINK_TASKMANAGER_CONTAINER"; then
    echo "❌ Error: Flink container '$FLINK_TASKMANAGER_CONTAINER' is not running"
    echo "   Start it with: ./manage_services_full.sh start"
    exit 1
fi

# Copy all UDF files to Flink containers
COPY_SUCCESS=true

echo "$UDF_FILES" | while read -r udf_file; do
    filename=$(basename "$udf_file")

    echo "📋 Copying $filename to Flink JobManager..."
    if ! docker cp "$udf_file" "$FLINK_JOBMANAGER_CONTAINER:/opt/flink/$filename"; then
        echo "❌ Failed to copy $filename to JobManager"
        COPY_SUCCESS=false
        continue
    fi

    echo "📋 Copying $filename to Flink TaskManager..."
    if ! docker cp "$udf_file" "$FLINK_TASKMANAGER_CONTAINER:/opt/flink/$filename"; then
        echo "❌ Failed to copy $filename to TaskManager"
        COPY_SUCCESS=false
        continue
    fi

    echo "✅ $filename copied successfully"
    echo ""
done

if [ "$COPY_SUCCESS" = true ]; then
    echo "✅ All UDFs installed successfully to both JobManager and TaskManager!"
    echo ""
    echo "📝 Example: Register functions in Flink SQL:"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment"
    echo "   AS 'udfs_example.generate_sentiment'"
    echo "   LANGUAGE PYTHON;"
    echo ""
    echo "🎯 Then use them in queries:"
    echo "   SELECT text, generate_sentiment(text) as sentiment FROM Q LIMIT 10;"
else
    echo "❌ Some UDFs failed to install"
    exit 1
fi

# Made with Bob
