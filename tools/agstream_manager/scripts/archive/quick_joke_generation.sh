#!/bin/bash

# Quick Joke Generation - Assumes Agentics is already installed
# For first-time setup, use generate_jokes_from_questions.sh instead

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=================================================="
echo "Quick Joke Generation (Assumes Setup Complete)"
echo "=================================================="
echo ""

# Check if Flink is running
if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: Flink JobManager is not running"
    echo "Please start the services first:"
    echo "  cd tools/agstream_manager"
    echo "  docker compose -f docker-compose-karapace-flink.yml up -d"
    exit 1
fi

echo "✓ Flink is running"
echo ""

# Create SQL initialization file
SQL_INIT_FILE="/tmp/agmap_joke_init.sql"

cat > "$SQL_INIT_FILE" << 'EOF'
-- ============================================
-- AGmap UDF Registration
-- ============================================
-- Register the agmap UDF for semantic transformations
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap
AS 'agmap.agmap' LANGUAGE PYTHON;

-- ============================================
-- Table Definitions
-- ============================================

-- Q (Questions) table
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

-- J (Jokes) table
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

-- ============================================
-- Ready to Use!
-- ============================================
-- The agmap function is now registered and ready to use.
--
-- Example queries:
--   SELECT agmap('Joke', text) FROM Q LIMIT 3;
--   INSERT INTO J SELECT JSON_VALUE(agmap('Joke', text), '$.joke_text') as joke_text FROM Q;
--   SELECT * FROM J LIMIT 10;

EOF

# Copy to container
docker cp "$SQL_INIT_FILE" flink-jobmanager:/tmp/agmap_joke_init.sql

echo "✓ SQL initialization file created and copied"
echo ""
echo "=================================================="
echo "Opening Flink SQL Client"
echo "=================================================="
echo ""
echo "Quick commands to use:"
echo ""
echo "1. Test with single question:"
echo "   SELECT agmap('Joke', 'Why did the chicken cross the road?') FROM Q LIMIT 1;"
echo ""
echo "2. Generate jokes from all questions:"
echo "   INSERT INTO J"
echo "   SELECT JSON_VALUE(agmap('Joke', text), '\$.joke_text') as joke_text"
echo "   FROM Q;"
echo ""
echo "3. View generated jokes:"
echo "   SELECT * FROM J LIMIT 10;"
echo ""
echo "4. See question + joke together:"
echo "   SELECT text, JSON_VALUE(agmap('Joke', text), '\$.joke_text') as joke"
echo "   FROM Q LIMIT 5;"
echo ""

# Open Flink SQL client
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -i /tmp/agmap_joke_init.sql

echo ""
echo "✓ Done!"

# Made with Bob
