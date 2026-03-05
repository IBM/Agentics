#!/bin/bash

# Flink SQL Client Launcher
# Connects to containerized Flink cluster and opens SQL client

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Flink SQL Client for AGStream Manager${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if Flink JobManager container is running
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}✗${NC} Flink JobManager container is not running!"
    echo ""
    echo "Start Flink with:"
    echo "  cd /Users/gliozzo/Code/agentics911/agentics"
    echo "  docker compose -f docker-compose-karapace-flink.yml up -d"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} Flink JobManager is running"
echo ""
echo "Opening Flink SQL Client..."
echo "You can now run SQL queries against Kafka topics"
echo ""
echo "Example commands:"
echo "  SHOW TABLES;"
echo "  CREATE TABLE my_topic (...) WITH ('connector' = 'kafka', ...);"
echo "  SELECT * FROM my_topic LIMIT 10;"
echo ""
echo "Type 'QUIT;' or 'EXIT;' to exit"
echo ""
echo "=" * 80

# Connect to Flink SQL client in the container
docker exec -it flink-jobmanager ./bin/sql-client.sh

# Made with Bob
