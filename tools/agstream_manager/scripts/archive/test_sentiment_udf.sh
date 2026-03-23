#!/bin/bash
# Test sentiment UDF on topic Q and show logs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🧪 Testing Sentiment UDF on Topic Q"
echo "===================================="
echo ""

# Check if Flink is running
if ! docker ps | grep -q flink-taskmanager; then
    echo "❌ Error: Flink containers not running"
    echo "Start with: ./manage_services_full.sh start"
    exit 1
fi

echo "📋 Step 1: Reinstalling UDF with debug logging..."
cd "$SCRIPT_DIR"
./install_udfs.sh
echo ""

echo "📋 Step 2: Opening Flink SQL Client..."
echo ""
echo "Run these commands in Flink SQL:"
echo ""
echo "-- 1. Create table Q from Kafka topic"
echo "CREATE TABLE Q ("
echo "    text STRING"
echo ") WITH ("
echo "    'connector' = 'kafka',"
echo "    'topic' = 'Q',"
echo "    'properties.bootstrap.servers' = 'kafka:9092',"
echo "    'properties.group.id' = 'test-group',"
echo "    'scan.startup.mode' = 'earliest-offset',"
echo "    'format' = 'json'"
echo ");"
echo ""
echo "-- 2. Register the UDF"
echo "CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment AS 'udfs_example.generate_sentiment' LANGUAGE PYTHON;"
echo ""
echo "-- 3. Test on topic Q"
echo "SELECT text, generate_sentiment(text) as sentiment FROM Q LIMIT 5;"
echo ""
echo "-- Exit SQL client"
echo "quit;"
echo ""
echo "Press Enter to open Flink SQL client..."
read

# Open Flink SQL client
docker exec -it flink-jobmanager ./bin/sql-client.sh

echo ""
echo "📋 Step 3: Viewing TaskManager logs for debug output..."
echo ""
echo "Press Ctrl+C to stop viewing logs"
echo ""
sleep 2

# Show logs
docker logs -f flink-taskmanager 2>&1 | grep -E "(DEBUG|ERROR|sentiment)"

# Made with Bob
