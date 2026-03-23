#!/bin/bash
# Script to run Flink tests inside the Flink container
# This script copies tests to the container, installs pytest if needed, and runs the tests

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Flink Test Runner ===${NC}"

# Check if container is running
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}Error: flink-jobmanager container is not running${NC}"
    echo "Start services with: ./manage_services.sh start"
    exit 1
fi

# Install pytest if not already installed
echo -e "${YELLOW}Checking pytest installation...${NC}"
if ! docker exec flink-jobmanager python3 -m pytest --version &>/dev/null; then
    echo -e "${YELLOW}Installing pytest in Flink container...${NC}"
    docker exec flink-jobmanager python3 -m pip install pytest pytest-asyncio
fi

# Setup semantic_operators symlink if needed
echo -e "${YELLOW}Setting up test environment...${NC}"
docker exec flink-jobmanager bash -c "cd /opt/flink/udfs && ln -sf /opt/flink/agentics/core/semantic_operators.py semantic_operators.py"

# Clean up any existing tests directory first
docker exec -u root flink-jobmanager rm -rf /opt/flink/tests/ 2>/dev/null || true

# Copy only test files (not subdirectories like non_flink_tests)
echo -e "${YELLOW}Copying tests to Flink container...${NC}"
docker exec flink-jobmanager mkdir -p /opt/flink/tests
for file in tests/test_*.py tests/*.md tests/README.md; do
    [ -f "$file" ] && docker cp "$file" flink-jobmanager:/opt/flink/tests/ 2>/dev/null
done

# Run tests with proper PYTHONPATH and show output
echo -e "${GREEN}Running tests in Flink container...${NC}"
if [ $# -eq 0 ]; then
    # Run all tests with live output
    docker exec -e PYTHONPATH=/opt/flink:/opt/flink/agentics:/opt/flink/udfs flink-jobmanager python3 -m pytest /opt/flink/tests/ -v -s
else
    # Run specific test file with live output
    docker exec -e PYTHONPATH=/opt/flink:/opt/flink/agentics:/opt/flink/udfs flink-jobmanager python3 -m pytest /opt/flink/tests/"$1" -v -s
fi

TEST_EXIT_CODE=$?

# Cleanup
echo -e "${YELLOW}Cleaning up...${NC}"
docker exec -u root flink-jobmanager rm -rf /opt/flink/tests/

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ Tests passed!${NC}"
else
    echo -e "${RED}✗ Tests failed with exit code $TEST_EXIT_CODE${NC}"
fi

exit $TEST_EXIT_CODE

# Made with Bob
