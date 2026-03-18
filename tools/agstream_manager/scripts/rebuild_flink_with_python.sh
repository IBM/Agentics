#!/bin/bash

# Rebuild Flink with Python Support
# This script builds a custom Flink image with Python and PyFlink pre-installed

set -e

echo "🔨 Building custom Flink image with Python support..."
echo ""

# Navigate to project root
cd "$(dirname "$0")/../../.."

# Stop existing Flink containers
echo "⏹️  Stopping existing Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml stop flink-jobmanager flink-taskmanager

# Remove old containers
echo "🗑️  Removing old containers..."
docker-compose -f docker-compose-karapace-flink.yml rm -f flink-jobmanager flink-taskmanager

# Build new image
echo ""
echo "🏗️  Building new Flink image with Python..."
docker-compose -f docker-compose-karapace-flink.yml build flink-jobmanager flink-taskmanager

# Start new containers
echo ""
echo "🚀 Starting new Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml up -d flink-jobmanager flink-taskmanager

# Wait for containers to be ready
echo ""
echo "⏳ Waiting for Flink to be ready..."
sleep 10

# Verify Python installation
echo ""
echo "✅ Verifying Python installation..."
docker exec flink-jobmanager python3 --version
docker exec flink-taskmanager python3 --version

echo ""
echo "✅ Verifying PyFlink installation..."
docker exec flink-jobmanager python3 -c "import pyflink; print(f'PyFlink version: {pyflink.__version__}')"

echo ""
echo "✅ Verifying UDF files..."
docker exec flink-jobmanager ls -la /opt/flink/udfs/

echo ""
echo "✅ Verifying Flink configuration..."
docker exec flink-jobmanager grep python /opt/flink/conf/flink-conf.yaml

echo ""
echo "🎉 Flink with Python support is ready!"
echo ""
echo "📝 Next steps:"
echo "   1. Open Flink SQL client: cd tools/agstream_manager/scripts && ./flink_sql.sh"
echo "   2. Register UDF: CREATE TEMPORARY SYSTEM FUNCTION add_prefix AS 'udfs_example.add_prefix' LANGUAGE PYTHON;"
echo "   3. Use UDF: SELECT add_prefix(text) FROM Q3;"
echo ""

# Made with Bob
