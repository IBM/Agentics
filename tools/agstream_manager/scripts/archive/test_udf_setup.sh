#!/bin/bash
# Test Python UDF Setup in Flink

set -e

echo "🔍 Testing Python UDF Setup in Flink Containers"
echo "================================================"
echo ""

# Test 1: Check if containers are running
echo "1️⃣ Checking Flink containers..."
if docker ps | grep -q flink-jobmanager; then
    echo "   ✅ flink-jobmanager is running"
else
    echo "   ❌ flink-jobmanager is NOT running"
    exit 1
fi

if docker ps | grep -q flink-taskmanager; then
    echo "   ✅ flink-taskmanager is running"
else
    echo "   ❌ flink-taskmanager is NOT running"
    exit 1
fi
echo ""

# Test 2: Check Python installation
echo "2️⃣ Checking Python in JobManager..."
docker exec flink-jobmanager python3 --version || echo "   ❌ Python3 not found in JobManager"
echo ""

echo "3️⃣ Checking Python in TaskManager..."
docker exec flink-taskmanager python3 --version || echo "   ❌ Python3 not found in TaskManager"
echo ""

# Test 3: Check PyFlink installation
echo "4️⃣ Checking PyFlink in JobManager..."
docker exec flink-jobmanager python3 -c "import pyflink; print(f'PyFlink version: {pyflink.__version__}')" 2>/dev/null || echo "   ❌ PyFlink not installed in JobManager"
echo ""

echo "5️⃣ Checking PyFlink in TaskManager..."
docker exec flink-taskmanager python3 -c "import pyflink; print(f'PyFlink version: {pyflink.__version__}')" 2>/dev/null || echo "   ❌ PyFlink not installed in TaskManager"
echo ""

# Test 4: Check if UDF file exists
echo "6️⃣ Checking UDF file in JobManager..."
if docker exec flink-jobmanager test -f /opt/flink/udfs_example.py; then
    echo "   ✅ udfs_example.py exists in JobManager"
    docker exec flink-jobmanager ls -lh /opt/flink/udfs_example.py
else
    echo "   ❌ udfs_example.py NOT found in JobManager"
fi
echo ""

echo "7️⃣ Checking UDF file in TaskManager..."
if docker exec flink-taskmanager test -f /opt/flink/udfs_example.py; then
    echo "   ✅ udfs_example.py exists in TaskManager"
    docker exec flink-taskmanager ls -lh /opt/flink/udfs_example.py
else
    echo "   ❌ udfs_example.py NOT found in TaskManager"
fi
echo ""

# Test 5: Try to import the UDF module
echo "8️⃣ Testing UDF import in JobManager..."
docker exec flink-jobmanager python3 -c "import sys; sys.path.insert(0, '/opt/flink'); import udfs_example; print('✅ UDF module imports successfully')" 2>/dev/null || echo "   ❌ Failed to import UDF module"
echo ""

echo "9️⃣ Testing UDF import in TaskManager..."
docker exec flink-taskmanager python3 -c "import sys; sys.path.insert(0, '/opt/flink'); import udfs_example; print('✅ UDF module imports successfully')" 2>/dev/null || echo "   ❌ Failed to import UDF module"
echo ""

# Test 6: Check PYTHONPATH
echo "🔟 Checking PYTHONPATH in TaskManager..."
docker exec flink-taskmanager bash -c 'echo "PYTHONPATH=$PYTHONPATH"'
echo ""

echo "================================================"
echo "📋 Summary:"
echo ""
echo "If all tests pass, the UDF should work."
echo "If Python or PyFlink is missing, you need to use a Flink image with Python support."
echo ""
echo "Recommended Flink image: flink:1.18.1-scala_2.12-java11"
echo "Or use: apache/flink:1.18.1-python"
echo ""

# Made with Bob
