#!/bin/bash

# Generate Jokes from Questions using AGmap UDF
# This script uses the agmap UDF to transform questions from Q topic into jokes in J topic
#
# IMPORTANT: Run this ONCE to rebuild the Docker image with all dependencies:
#   ./scripts/rebuild_flink_image.sh
#   ./manage_services_full.sh restart
#
# After that, you can use ./scripts/quick_joke_generation.sh for faster access

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "=================================================="
echo "Generate Jokes from Questions using AGmap UDF"
echo "=================================================="
echo ""

# Check if Flink is running
if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: Flink JobManager is not running"
    echo "Please start the services first:"
    echo "  cd tools/agstream_manager"
    echo "  ./manage_services_full.sh start"
    exit 1
fi

echo "✓ Flink is running"
echo ""

# Check if Agentics is installed in Flink
echo "Checking if Agentics is installed in Flink..."
if docker exec flink-taskmanager python3 -c "from agentics import AG; import hnswlib" 2>/dev/null; then
    echo "✓ Agentics and dependencies are installed"
else
    echo "❌ Agentics or dependencies (hnswlib) not found in Flink containers"
    echo ""
    echo "You need to rebuild the Docker image with dependencies:"
    echo "  cd tools/agstream_manager"
    echo "  ./scripts/rebuild_flink_image.sh"
    echo "  ./manage_services_full.sh restart"
    echo ""
    exit 1
fi
echo ""

# Step 1: Install the agmap UDF (lightweight, always do this)
echo "Step 1: Installing agmap UDF..."
cd "$SCRIPT_DIR/.."
./scripts/install_udfs.sh
echo "✓ UDF installed"
echo ""

# Step 3: Create SQL initialization file
echo "Step 3: Creating Flink SQL initialization file..."
SQL_INIT_FILE="/tmp/agmap_joke_init.sql"

cat > "$SQL_INIT_FILE" << 'EOF'
-- Register the agmap UDF
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap
AS 'agmap.agmap' LANGUAGE PYTHON;

-- Create Q (Questions) table
CREATE TABLE IF NOT EXISTS Q (
  text STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'Q',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);

-- Create J (Jokes) table
CREATE TABLE IF NOT EXISTS J (
  joke_text STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'J',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);

EOF

echo "✓ SQL initialization file created"
echo ""

# Step 2: Create the query file
echo "Step 2: Creating joke generation query..."
QUERY_FILE="/tmp/agmap_joke_query.sql"

cat > "$QUERY_FILE" << 'EOF'
-- Generate jokes from questions using agmap UDF
-- This reads from Q topic, transforms to jokes, and writes to J topic

INSERT INTO J
SELECT
  JSON_VALUE(agmap('Joke', text), '$.joke_text') as joke_text
FROM Q;

EOF

echo "✓ Query file created"
echo ""

# Step 4: Show the user what will be executed
echo "=================================================="
echo "The following SQL will be executed:"
echo "=================================================="
echo ""
echo "1. Register agmap UDF"
echo "2. Create Q (Questions) table"
echo "3. Create J (Jokes) table"
echo "4. Run transformation query:"
cat "$QUERY_FILE"
echo ""
echo "=================================================="
echo ""

# Step 3: Copy SQL files to container
echo "Step 3: Copying SQL files to Flink container..."
docker cp "$SQL_INIT_FILE" flink-jobmanager:/tmp/agmap_joke_init.sql
docker cp "$QUERY_FILE" flink-jobmanager:/tmp/agmap_joke_query.sql
echo "✓ SQL files copied to container"
echo ""

# Step 4: Execute the SQL
echo "Step 4: Executing Flink SQL..."
echo ""
echo "Opening Flink SQL client with initialization..."
echo "The tables and UDF will be registered automatically."
echo ""
echo "To run the joke generation query, paste this in the SQL client:"
echo ""
cat "$QUERY_FILE"
echo ""
echo "Or to test with a single question:"
echo "  SELECT agmap('Joke', 'Why did the chicken cross the road?') as joke FROM Q LIMIT 1;"
echo ""
echo "To view results:"
echo "  SELECT * FROM J LIMIT 10;"
echo ""

# Open Flink SQL client with initialization
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -i /tmp/agmap_joke_init.sql

echo ""
echo "✓ Done!"

# Made with Bob
