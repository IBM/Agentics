#!/bin/bash
# Flink SQL Client Wrapper for AGStream Manager
# Uses the Flink SQL client from the Docker container

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "🚀 Flink SQL Client for AGStream Manager"
echo "========================================================================"
echo "Kafka Bootstrap: kafka:9092 (inside container)"
echo "Schema Registry: http://schema-registry:8081 (inside container)"
echo "========================================================================"
echo ""

# Check if Flink container is running
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}✗ Flink JobManager container is not running!${NC}"
    echo "Start services with: ./manage_services.sh start"
    exit 1
fi

echo -e "${GREEN}✓ Flink JobManager is running${NC}"
echo ""

echo -e "${BLUE}📋 Quick Start Guide:${NC}"
echo ""
echo "⚠️  IMPORTANT: Execute ONE statement at a time, press Enter after each semicolon"
echo ""
echo "1. Create a table for a Kafka topic (copy line by line):"
echo "   CREATE TABLE Q ("
echo "     question STRING,"
echo "     \`timestamp\` BIGINT"
echo "   ) WITH ("
echo "     'connector' = 'kafka',"
echo "     'topic' = 'Q',"
echo "     'properties.bootstrap.servers' = 'kafka:9092',"
echo "     'properties.group.id' = 'flink-sql-client',"
echo "     'scan.startup.mode' = 'earliest-offset',"
echo "     'format' = 'json'"
echo "   );"
echo "   [Press Enter after the semicolon]"
echo ""
echo "2. Then query the topic (separate statement):"
echo "   SELECT * FROM Q LIMIT 10;"
echo "   [Press Enter]"
echo ""
echo "3. For Avro topics with Schema Registry, use:"
echo "     'format' = 'avro-confluent',"
echo "     'avro-confluent.url' = 'http://schema-registry:8081'"
echo ""
echo "4. Reserved keywords (use backticks):"
echo "   timestamp, time, date, order, value, key, etc."
echo "   Example: \`timestamp\` BIGINT, \`order\` STRING"
echo ""
echo "5. Other useful commands:"
echo "   SHOW TABLES;     -- List all tables"
echo "   HELP;            -- Show help"
echo "   Press Ctrl+D     -- Exit the client"
echo ""
echo "========================================================================"
echo ""

echo -e "${YELLOW}📝 Starting Flink SQL Client...${NC}"
echo ""

# Run SQL client in the container
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh

echo ""
echo -e "${GREEN}👋 Flink SQL Client closed${NC}"

# Made with Bob
