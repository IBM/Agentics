#!/bin/bash
# fix_agreduce.sh - Fix common agreduce UDF issues

set -e

echo "========================================="
echo "AGReduce UDF Fix Script"
echo "========================================="
echo ""

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    exit 1
fi

# Check if containers exist
if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: flink-jobmanager container not found"
    echo "Please start Flink first with: docker-compose up -d"
    exit 1
fi

echo "Step 1: Installing libgomp in Flink containers..."
echo "------------------------------------------------"
docker exec flink-jobmanager bash -c "apt-get update && apt-get install -y libgomp1" || {
    echo "⚠️  Warning: Could not install libgomp in jobmanager (may already be installed)"
}

if docker ps | grep -q flink-taskmanager; then
    docker exec flink-taskmanager bash -c "apt-get update && apt-get install -y libgomp1" || {
        echo "⚠️  Warning: Could not install libgomp in taskmanager (may already be installed)"
    }
fi

echo "✓ libgomp installation complete"
echo ""

echo "Step 2: Copying agreduce.py to Flink containers..."
echo "------------------------------------------------"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UDF_FILE="$SCRIPT_DIR/../udfs/agreduce.py"

if [ ! -f "$UDF_FILE" ]; then
    echo "❌ Error: agreduce.py not found at $UDF_FILE"
    exit 1
fi

docker cp "$UDF_FILE" flink-jobmanager:/opt/flink/agreduce.py
echo "✓ Copied to jobmanager"

if docker ps | grep -q flink-taskmanager; then
    docker cp "$UDF_FILE" flink-taskmanager:/opt/flink/agreduce.py
    echo "✓ Copied to taskmanager"
fi

echo ""

echo "Step 3: Verifying installation..."
echo "------------------------------------------------"
if docker exec flink-jobmanager test -f /opt/flink/agreduce.py; then
    echo "✓ agreduce.py exists in jobmanager"
else
    echo "❌ agreduce.py not found in jobmanager"
    exit 1
fi

if docker exec flink-jobmanager dpkg -l | grep -q libgomp1; then
    echo "✓ libgomp1 installed in jobmanager"
else
    echo "⚠️  libgomp1 not found in jobmanager"
fi

echo ""

echo "Step 4: Restarting Flink containers..."
echo "------------------------------------------------"
docker-compose restart flink-jobmanager flink-taskmanager

echo "✓ Containers restarted"
echo ""

echo "========================================="
echo "✓ Fix complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Wait 30 seconds for Flink to fully start"
echo "2. Open Flink SQL terminal"
echo "3. Register the UDF:"
echo "   CREATE TEMPORARY SYSTEM FUNCTION agreduce AS 'agreduce.agreduce' LANGUAGE PYTHON;"
echo "4. Test with:"
echo "   SELECT agreduce(customer_review, 'summary', 'str', 'Summarize') FROM pr LIMIT 10;"
echo ""
echo "If issues persist, check logs:"
echo "  docker logs flink-taskmanager 2>&1 | tail -50"
echo ""

# Made with Bob
