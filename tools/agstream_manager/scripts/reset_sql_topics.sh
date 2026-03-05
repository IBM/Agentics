#!/bin/bash
# Reset SQL topics by deleting and recreating them

echo "🗑️  Deleting old topics..."
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic questions_sql 2>/dev/null || true
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic answers_sql 2>/dev/null || true

echo "⏳ Waiting for deletion to complete..."
sleep 2

echo "✅ Topics deleted. Now run the example to create fresh topics:"
echo "   python examples/agstream_sql_example.py"

# Made with Bob
