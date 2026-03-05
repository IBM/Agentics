#!/bin/bash

# PyFlink SQL Shell Launcher for AGStream Manager
# Starts an interactive SQL shell to query Kafka topics

set -e

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}PyFlink SQL Shell for AGStream Manager${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Load environment variables from .env file
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    echo -e "${GREEN}✓${NC} Loaded environment from .env"
else
    echo -e "${YELLOW}⚠${NC}  No .env file found, using defaults"
fi

# Set defaults if not in environment
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export SCHEMA_REGISTRY_URL="${AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL:-http://localhost:8081}"

echo -e "${GREEN}✓${NC} Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
echo -e "${GREEN}✓${NC} Schema Registry: $SCHEMA_REGISTRY_URL"
echo ""

# Check if uv is available
if command -v uv &> /dev/null; then
    echo -e "${GREEN}✓${NC} Using uv to run Python"
    echo ""
    cd "$PROJECT_ROOT"
    uv run "$SCRIPT_DIR/flink_sql_shell.py"
else
    # Fallback to regular python3
    echo -e "${YELLOW}⚠${NC}  uv not found, using python3"

    # Check if PyFlink is installed
    if ! python3 -c "import pyflink" 2>/dev/null; then
        echo -e "${YELLOW}⚠${NC}  PyFlink not installed!"
        echo ""
        echo "Install with:"
        echo "  pip install apache-flink"
        echo ""
        exit 1
    fi

    echo -e "${GREEN}✓${NC} PyFlink is installed"
    echo ""

    cd "$PROJECT_ROOT"
    python3 "$SCRIPT_DIR/flink_sql_shell.py"
fi

# Made with Bob
