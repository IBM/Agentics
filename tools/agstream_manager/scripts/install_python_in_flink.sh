#!/bin/bash
# Install Python and PyFlink in existing Flink containers using uv

set -e

echo "🐍 Installing Python in Flink Containers (using uv)"
echo "===================================================="
echo ""

# Start containers if not running
echo "1️⃣ Starting Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml up -d flink-jobmanager flink-taskmanager
sleep 5
echo ""

# Install Python, JDK headers, and uv in JobManager
echo "2️⃣ Installing Python, JDK headers, and uv in JobManager..."
docker exec -u root flink-jobmanager bash -c "apt-get update && apt-get install -y python3 python3-dev curl openjdk-11-jdk-headless" || {
    echo "   ⚠️  Note: Some warnings are normal during apt-get install"
}
docker exec -u root flink-jobmanager bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
docker exec -u root flink-jobmanager bash -c "ln -sf /root/.local/bin/uv /usr/local/bin/uv"
echo ""

# Install Python, JDK headers, and uv in TaskManager
echo "3️⃣ Installing Python, JDK headers, and uv in TaskManager..."
docker exec -u root flink-taskmanager bash -c "apt-get update && apt-get install -y python3 python3-dev curl openjdk-11-jdk-headless" || {
    echo "   ⚠️  Note: Some warnings are normal during apt-get install"
}
docker exec -u root flink-taskmanager bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
docker exec -u root flink-taskmanager bash -c "ln -sf /root/.local/bin/uv /usr/local/bin/uv"
echo ""

# Create symlink for Java headers (pemja expects /opt/java/openjdk)
echo "4️⃣ Creating Java symlinks for pemja..."
docker exec -u root flink-jobmanager bash -c "mkdir -p /opt/java && ln -sf /usr/lib/jvm/java-11-openjdk-arm64 /opt/java/openjdk"
docker exec -u root flink-taskmanager bash -c "mkdir -p /opt/java && ln -sf /usr/lib/jvm/java-11-openjdk-arm64 /opt/java/openjdk"
echo ""

# Install PyFlink using uv in JobManager
echo "5️⃣ Installing PyFlink with uv in JobManager..."
docker exec flink-jobmanager bash -c "export JAVA_HOME=/opt/java/openjdk && uv pip install --system apache-flink==1.18.1"
echo ""

# Install PyFlink using uv in TaskManager
echo "6️⃣ Installing PyFlink with uv in TaskManager..."
docker exec flink-taskmanager bash -c "export JAVA_HOME=/opt/java/openjdk && uv pip install --system apache-flink==1.18.1"
echo ""

# Verify installation
echo "7️⃣ Verifying Python installation..."
echo ""
echo "JobManager Python version:"
docker exec flink-jobmanager python3 --version
echo ""
echo "TaskManager Python version:"
docker exec flink-taskmanager python3 --version
echo ""

echo "8️⃣ Verifying uv installation..."
echo ""
echo "JobManager uv version:"
docker exec flink-jobmanager uv --version
echo ""
echo "TaskManager uv version:"
docker exec flink-taskmanager uv --version
echo ""

echo "9️⃣ Verifying PyFlink installation..."
echo ""
echo "JobManager PyFlink:"
docker exec flink-jobmanager python3 -c "import pyflink; print(f'PyFlink {pyflink.__version__}')"
echo ""
echo "TaskManager PyFlink:"
docker exec flink-taskmanager python3 -c "import pyflink; print(f'PyFlink {pyflink.__version__}')"
echo ""

echo "===================================================="
echo "✅ Python, uv, and PyFlink installed successfully!"
echo ""
echo "📝 Next steps:"
echo "   1. Install UDFs: ./install_udfs.sh"
echo "   2. Test setup: ./test_udf_setup.sh"
echo "   3. Use UDFs in Flink SQL!"
echo ""

# Made with Bob
