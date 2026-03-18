#!/bin/bash
# Download Flink Avro format connector for Flink SQL

FLINK_VERSION="1.18.1"
AVRO_JAR="flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar"
DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/${AVRO_JAR}"

echo "📥 Downloading Flink Avro Confluent Registry connector..."
echo "   Version: ${FLINK_VERSION}"
echo "   URL: ${DOWNLOAD_URL}"
echo ""

cd "$(dirname "$0")/.." || exit 1

if [ -f "flink-lib/${AVRO_JAR}" ]; then
    echo "✅ ${AVRO_JAR} already exists"
else
    echo "⏳ Downloading..."
    mkdir -p flink-lib
    curl -L -o "flink-lib/${AVRO_JAR}" "${DOWNLOAD_URL}"

    if [ $? -eq 0 ]; then
        echo "✅ Downloaded successfully!"
    else
        echo "❌ Download failed"
        exit 1
    fi
fi

echo ""
echo "📦 Flink JARs in flink-lib/:"
ls -lh flink-lib/*.jar

echo ""
echo "🔄 Next steps:"
echo "   1. Restart Flink containers: ./manage_services.sh restart"
echo "   2. Then try your Flink SQL query again"

# Made with Bob
