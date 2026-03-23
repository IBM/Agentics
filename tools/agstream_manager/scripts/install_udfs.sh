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

# Step 1: Copy .env file with API keys
echo "📋 Step 1: Copying .env file with API keys..."
PROJECT_ROOT="$AGSTREAM_DIR/../.."
ENV_FILE="$PROJECT_ROOT/.env"

if [ -f "$ENV_FILE" ]; then
    echo "   Found .env at: $ENV_FILE"
    docker cp "$ENV_FILE" "$FLINK_JOBMANAGER_CONTAINER:/opt/flink/.env" 2>/dev/null && echo "   ✓ Copied to JobManager" || echo "   ⚠ Failed to copy to JobManager"
    docker cp "$ENV_FILE" "$FLINK_TASKMANAGER_CONTAINER:/opt/flink/.env" 2>/dev/null && echo "   ✓ Copied to TaskManager" || echo "   ⚠ Failed to copy to TaskManager"
elif [ -f "$AGSTREAM_DIR/.env" ]; then
    echo "   Found .env at: $AGSTREAM_DIR/.env"
    docker cp "$AGSTREAM_DIR/.env" "$FLINK_JOBMANAGER_CONTAINER:/opt/flink/.env" 2>/dev/null && echo "   ✓ Copied to JobManager" || echo "   ⚠ Failed to copy to JobManager"
    docker cp "$AGSTREAM_DIR/.env" "$FLINK_TASKMANAGER_CONTAINER:/opt/flink/.env" 2>/dev/null && echo "   ✓ Copied to TaskManager" || echo "   ⚠ Failed to copy to TaskManager"
else
    echo "   ⚠ Warning: No .env file found"
    echo "   Checked: $ENV_FILE"
    echo "   Checked: $AGSTREAM_DIR/.env"
    echo "   UDFs may not work without API keys (OPENAI_API_KEY or ANTHROPIC_API_KEY)"
fi
echo ""

# Step 2: Copy all UDF files to Flink containers
echo "📋 Step 2: Installing UDF files..."
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

# Step 3: Verify installation
echo "📋 Step 3: Verifying installation..."
echo ""

# Check if agmap.py is accessible
echo "Checking agmap UDF..."
if docker exec "$FLINK_TASKMANAGER_CONTAINER" test -f /opt/flink/agmap.py; then
    echo "   ✓ agmap.py found in TaskManager"
else
    echo "   ✗ agmap.py not found in TaskManager"
fi

# Check if .env is accessible
echo "Checking .env file..."
if docker exec "$FLINK_TASKMANAGER_CONTAINER" test -f /opt/flink/.env; then
    echo "   ✓ .env found in TaskManager"
    # Check if API keys are present
    if docker exec "$FLINK_TASKMANAGER_CONTAINER" grep -q "API_KEY" /opt/flink/.env 2>/dev/null; then
        echo "   ✓ API keys found in .env"
    else
        echo "   ⚠ No API keys found in .env"
    fi
else
    echo "   ✗ .env not found in TaskManager"
fi

# Check if agentics is importable
echo "Checking Agentics installation..."
if docker exec "$FLINK_TASKMANAGER_CONTAINER" python3 -c "from agentics import AG; import hnswlib" 2>/dev/null; then
    echo "   ✓ Agentics and dependencies are installed"
else
    echo "   ✗ Agentics or dependencies missing"
    echo "   Run: ./scripts/rebuild_flink_image.sh && ./manage_services_full.sh restart"
fi

echo ""

if [ "$COPY_SUCCESS" = true ]; then
    echo "✅ Installation complete!"
    echo ""
    echo "📝 Quick Start - Register agmap UDF in Flink SQL:"
    echo ""
    echo "   CREATE TEMPORARY SYSTEM FUNCTION agmap"
    echo "   AS 'agmap.agmap' LANGUAGE PYTHON;"
    echo ""
    echo "🎯 Then generate jokes:"
    echo "   SELECT agmap('Joke', text) FROM Q LIMIT 3;"
    echo ""
    echo "📖 For more examples, see: GENERATE_JOKES_GUIDE.md"
else
    echo "❌ Some UDFs failed to install"
    exit 1
fi

# Made with Bob
