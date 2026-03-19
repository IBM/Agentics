#!/bin/bash

# Check for --force flag
FORCE_INSTALL=false
if [ "$1" = "--force" ]; then
    FORCE_INSTALL=true
    echo "📦 Force installing Agentics in Flink containers..."
else
    echo "📦 Installing Agentics in Flink containers..."
fi
echo ""

# Get the agentics source directory (parent of tools/agstream_manager)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENTICS_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "Agentics source directory: $AGENTICS_DIR"
echo ""

# Check if containers are running
if ! docker ps | grep -q flink-taskmanager; then
    echo "❌ Error: flink-taskmanager container is not running"
    echo "Start it with: cd tools/agstream_manager && ./manage_services_full.sh start"
    exit 1
fi

if ! docker ps | grep -q flink-jobmanager; then
    echo "❌ Error: flink-jobmanager container is not running"
    echo "Start it with: cd tools/agstream_manager && ./manage_services_full.sh start"
    exit 1
fi

echo "✓ Flink containers are running"
echo ""

# Check if Agentics is already installed (skip if --force)
if [ "$FORCE_INSTALL" = false ]; then
    echo "🔍 Checking if Agentics is already installed..."
    TASKMANAGER_HAS_AGENTICS=$(docker exec flink-taskmanager python3 -c "from agentics import AG; print('yes')" 2>/dev/null || echo "no")
    JOBMANAGER_HAS_AGENTICS=$(docker exec flink-jobmanager python3 -c "from agentics import AG; print('yes')" 2>/dev/null || echo "no")

    if [ "$TASKMANAGER_HAS_AGENTICS" = "yes" ] && [ "$JOBMANAGER_HAS_AGENTICS" = "yes" ]; then
        echo "✅ Agentics is already installed in both containers"
        echo ""
        echo "To force reinstall, run with --force flag:"
        echo "  ./install_agentics_in_flink.sh --force"
        echo ""
        exit 0
    fi
    echo "⚠️  Agentics not found or incomplete, proceeding with installation..."
    echo ""
fi

echo "📋 Building wheel with uv..."
cd "$AGENTICS_DIR"
uv build --wheel --out-dir /tmp 2>/dev/null || {
    echo "❌ Failed to build wheel"
    exit 1
}

WHEEL_FILE=$(ls -t /tmp/agentics_py-*.whl 2>/dev/null | head -1)
if [ -z "$WHEEL_FILE" ]; then
    echo "❌ No wheel file found"
    exit 1
fi

echo "📋 Built wheel: $(basename $WHEEL_FILE)"

echo ""

# Install uv in containers if not present
echo "🔧 Installing uv in TaskManager..."
docker exec flink-taskmanager bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh" 2>/dev/null || true

echo "🔧 Installing uv in JobManager..."
docker exec flink-jobmanager bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh" 2>/dev/null || true

echo ""

# Copy wheel to containers and install with uv (preserve original filename)
WHEEL_NAME=$(basename "$WHEEL_FILE")

echo "🔧 Cleaning up old agentics directories..."
docker exec -u root flink-taskmanager rm -rf /opt/flink/agentics /opt/agentics
docker exec -u root flink-jobmanager rm -rf /opt/flink/agentics /opt/agentics

echo "🔧 Installing Agentics in TaskManager with uv..."
docker cp "$WHEEL_FILE" flink-taskmanager:/tmp/"$WHEEL_NAME"
docker exec -u root flink-taskmanager bash -c "/opt/flink/.local/bin/uv pip install /tmp/$WHEEL_NAME --system --reinstall"
docker exec -u root flink-taskmanager rm -f /tmp/"$WHEEL_NAME"

echo ""

echo "🔧 Installing Agentics in JobManager with uv..."
docker cp "$WHEEL_FILE" flink-jobmanager:/tmp/"$WHEEL_NAME"
docker exec -u root flink-jobmanager bash -c "/opt/flink/.local/bin/uv pip install /tmp/$WHEEL_NAME --system --reinstall"
docker exec -u root flink-jobmanager rm -f /tmp/"$WHEEL_NAME"

# Cleanup
rm -f "$WHEEL_FILE"

echo ""

# Install additional dependencies required by agentics
echo "🔧 Installing additional dependencies (hnswlib, scikit-learn)..."
docker exec -u root flink-taskmanager pip install hnswlib scikit-learn
docker exec -u root flink-jobmanager pip install hnswlib scikit-learn

echo ""

# Copy .env file to Flink containers
echo "📋 Copying .env file to Flink containers..."
# Try root .env first (has API keys), then agstream_manager .env
ROOT_ENV_FILE="$AGENTICS_DIR/.env"
LOCAL_ENV_FILE="$SCRIPT_DIR/../.env"

if [ -f "$ROOT_ENV_FILE" ]; then
    echo "  Found root .env at: $ROOT_ENV_FILE"
    docker cp "$ROOT_ENV_FILE" flink-taskmanager:/opt/flink/.env
    docker cp "$ROOT_ENV_FILE" flink-jobmanager:/opt/flink/.env
    echo "  ✅ Root .env copied to both containers"
elif [ -f "$LOCAL_ENV_FILE" ]; then
    echo "  Found local .env at: $LOCAL_ENV_FILE"
    docker cp "$LOCAL_ENV_FILE" flink-taskmanager:/opt/flink/.env
    docker cp "$LOCAL_ENV_FILE" flink-jobmanager:/opt/flink/.env
    echo "  ✅ Local .env copied to both containers"
else
    echo "  ⚠️  Warning: No .env file found"
    echo "  Checked: $ROOT_ENV_FILE"
    echo "  Checked: $LOCAL_ENV_FILE"
    echo "  UDFs may not work without API keys"
    echo "  Create .env from .env.example and rerun this script"
fi

echo ""

# Verify installation
echo "🧪 Verifying installation..."
echo ""

echo "TaskManager:"
if docker exec -u root flink-taskmanager python3 -c "from agentics import AG; print('  ✅ Agentics imported successfully')"; then
    docker exec -u root flink-taskmanager python3 -c "import agentics; print(f'  Version: {getattr(agentics, \"__version__\", \"unknown\")}')" 2>/dev/null || echo "  Version: installed"
else
    echo "  ❌ Failed to import Agentics"
    echo "  Showing error details:"
    docker exec -u root flink-taskmanager python3 -c "from agentics import AG" 2>&1 || true
    exit 1
fi

echo ""
echo "JobManager:"
if docker exec -u root flink-jobmanager python3 -c "from agentics import AG; print('  ✅ Agentics imported successfully')"; then
    docker exec -u root flink-jobmanager python3 -c "import agentics; print(f'  Version: {getattr(agentics, \"__version__\", \"unknown\")}')" 2>/dev/null || echo "  Version: installed"
else
    echo "  ❌ Failed to import Agentics"
    echo "  Showing error details:"
    docker exec -u root flink-jobmanager python3 -c "from agentics import AG" 2>&1 || true
    exit 1
fi

echo ""
echo "🎉 Agentics installed successfully in both Flink containers!"
echo ""
echo "Next steps:"
echo "1. Install your UDFs: ./install_udfs.sh"
echo "2. Register in SQL: CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment AS 'udfs_example.generate_sentiment' LANGUAGE PYTHON;"
echo "3. Use it: SELECT generate_sentiment(text) FROM Q LIMIT 10;"

# Made with Bob
