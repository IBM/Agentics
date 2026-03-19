#!/bin/bash
# Copy .env file to Flink containers for API key access

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENTICS_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENV_FILE="$AGENTICS_DIR/.env"

echo "📋 Copying .env file to Flink containers..."
echo ""

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env file not found at $ENV_FILE"
    echo ""
    echo "Please create a .env file with your API keys:"
    echo "  OPENAI_API_KEY=your_key_here"
    echo "  # or"
    echo "  ANTHROPIC_API_KEY=your_key_here"
    exit 1
fi

echo "Found .env file: $ENV_FILE"
echo ""

# Check if containers are running
if ! docker ps | grep -q flink-taskmanager; then
    echo "❌ Error: flink-taskmanager container is not running"
    echo "Start it with: ./manage_services_full.sh start"
    exit 1
fi

if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: flink-jobmanager container is not running"
    echo "Start it with: ./manage_services_full.sh start"
    exit 1
fi

# Copy to TaskManager
echo "📋 Copying to TaskManager..."
docker cp "$ENV_FILE" flink-taskmanager:/opt/flink/.env

# Copy to JobManager
echo "📋 Copying to JobManager..."
docker cp "$ENV_FILE" flink-jobmanager:/opt/flink/.env

echo ""
echo "✅ .env file copied successfully to both containers!"
echo ""
echo "The UDFs can now access API keys from /opt/flink/.env"
echo ""
echo "Note: You may need to restart any running Flink jobs for them to pick up the new environment variables."

# Made with Bob
