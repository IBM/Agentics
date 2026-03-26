#!/bin/bash
# Flink SQL Client Launcher
# This script mimics what the AGStream Manager UI does to start the Flink SQL shell

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "🚀 Starting Flink SQL Client..."
echo ""

# Step 1: Generate SQL initialization file
echo "📝 Generating table initialization SQL..."
INIT_SCRIPT="$SCRIPT_DIR/init_flink_tables.py"
SQL_FILE="/tmp/flink_init_tables.sql"

if [ -f "$INIT_SCRIPT" ]; then
    # Try to run the init script
    cd "$PROJECT_ROOT"
    if python3 "$INIT_SCRIPT" 2>/dev/null > "$SQL_FILE"; then
        echo "✅ Generated initialization SQL"
        echo "   Tables will be auto-created from AGStream Manager channels"
    else
        echo "⚠️  Warning: Could not generate init SQL (AGStream Manager may not be running)"
        echo "   Starting SQL client without pre-loaded tables..."
        echo "-- No initialization SQL" > "$SQL_FILE"
    fi
else
    echo "⚠️  Warning: init_flink_tables.py not found"
    echo "   Starting SQL client without pre-loaded tables..."
    echo "-- No initialization SQL" > "$SQL_FILE"
fi

# Step 2: Ensure init file exists in container
echo ""
echo "📋 Setting up initialization SQL in Flink container..."

# Always create the init file in the container first (ensures it exists)
if docker exec flink-jobmanager bash -c "echo '-- Flink SQL Initialization' > /tmp/init_tables.sql" 2>/dev/null; then
    echo "✅ Created empty init file in container"

    # Try to copy our generated SQL if it exists
    if [ -f "$SQL_FILE" ]; then
        if docker cp "$SQL_FILE" flink-jobmanager:/tmp/init_tables.sql 2>/dev/null; then
            echo "✅ Initialization SQL copied successfully"
        else
            echo "⚠️  Could not copy SQL file, using empty init file"
        fi
    else
        echo "⚠️  No SQL file generated, using empty init file"
    fi
else
    echo "❌ Error: Could not create init file in container"
    echo "   Flink SQL client may fail to start"
    echo "   Please ensure flink-jobmanager container is running"
fi

# Step 3: Start Flink SQL Client
echo ""
echo "🎯 Starting Flink SQL Client..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Start SQL client with init file
docker exec -it flink-jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -i /tmp/init_tables.sql"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "👋 Flink SQL Client closed"

# Made with Bob
