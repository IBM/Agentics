#!/bin/bash
# Restart Flink containers to load new JARs

echo "🔄 Restarting Flink containers..."
echo ""

cd "$(dirname "$0")/../../.." || exit 1

echo "⏹️  Stopping Flink containers..."
docker stop flink-taskmanager flink-jobmanager 2>/dev/null || true

echo "🗑️  Removing Flink containers..."
docker rm flink-taskmanager flink-jobmanager 2>/dev/null || true

echo "🚀 Starting Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml up -d flink-jobmanager flink-taskmanager

echo ""
echo "⏳ Waiting for Flink to be ready..."
sleep 5

echo ""
echo "✅ Flink restarted!"
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
