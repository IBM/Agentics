#!/bin/bash
# Full reset: Stop Kafka, clean data AND volumes, restart, recreate topics with Avro

echo "🛑 Stopping Kafka and Karapace..."
docker-compose -f docker-compose-karapace-flink.yml stop kafka karapace-schema-registry

echo "🗑️  Removing containers and volumes..."
docker-compose -f docker-compose-karapace-flink.yml rm -f -v kafka karapace-schema-registry

echo "🚀 Starting Kafka and Karapace..."
docker-compose -f docker-compose-karapace-flink.yml up -d kafka karapace-schema-registry

echo "⏳ Waiting 20 seconds for services to be ready..."
sleep 20

echo "✅ Services restarted with clean volumes. Now run:"
echo "   python examples/agstream_sql_example.py"

# Made with Bob
