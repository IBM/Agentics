#!/bin/bash
# Restart Flink containers to load new JARs

echo "🔄 Restarting Flink containers..."
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGSTREAM_DIR="$SCRIPT_DIR/.."

cd "$AGSTREAM_DIR" || exit 1

echo "⏹️  Stopping Flink containers..."
docker stop flink-taskmanager flink-jobmanager 2>/dev/null || true

echo "🗑️  Removing Flink containers..."
docker rm flink-taskmanager flink-jobmanager 2>/dev/null || true

echo "🚀 Starting Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml up -d flink-jobmanager flink-taskmanager

echo ""
echo "⏳ Waiting for Flink to be ready..."
sleep 10

echo ""
echo "📝 Generating table definitions..."
INIT_SCRIPT="$SCRIPT_DIR/init_flink_tables.py"
SQL_FILE="/tmp/flink_init_tables.sql"

# Generate SQL file from AGStream Manager channels
if [ -f "$INIT_SCRIPT" ]; then
    python3 "$INIT_SCRIPT" > "$SQL_FILE" 2>/dev/null || {
        echo "⚠️  Could not generate table definitions (AGStream Manager may not be running)"
        echo "-- Flink SQL Initialization" > "$SQL_FILE"
    }
else
    echo "-- Flink SQL Initialization" > "$SQL_FILE"
fi

# Copy to container
docker cp "$SQL_FILE" flink-jobmanager:/tmp/init_tables.sql 2>/dev/null || {
    docker exec flink-jobmanager bash -c "echo '-- Flink SQL Initialization' > /tmp/init_tables.sql" 2>/dev/null || true
}

echo ""
echo "📦 Reinstalling UDFs..."
"$SCRIPT_DIR/install_udfs.sh"

echo ""
echo "✅ Flink restarted and UDFs installed!"
echo ""
echo "📊 Flink Web UI: http://localhost:8085"
echo ""
echo "🔍 Check Flink logs:"
echo "   docker logs flink-jobmanager"
echo "   docker logs flink-taskmanager"
echo ""
echo "📝 Now you can use Flink SQL with Avro format!"
echo "   cd tools/agstream_manager/scripts && ./flink_sql.sh"

# Made with Bob
