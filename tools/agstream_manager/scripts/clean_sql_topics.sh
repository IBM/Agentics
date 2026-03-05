#!/bin/bash
# Clean SQL topics completely - delete topics and schemas

echo "🗑️  Step 1: Deleting Kafka topics..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic questions_sql 2>/dev/null || echo "  (questions_sql not found)"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic answers_sql 2>/dev/null || echo "  (answers_sql not found)"

echo ""
echo "🗑️  Step 2: Deleting Schema Registry subjects..."
curl -X DELETE http://localhost:8081/subjects/Question-value 2>/dev/null || echo "  (Question-value not found)"
curl -X DELETE http://localhost:8081/subjects/Answer-value 2>/dev/null || echo "  (Answer-value not found)"

echo ""
echo "⏳ Step 3: Waiting 5 seconds for cleanup..."
sleep 5

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Run: python examples/agstream_sql_example.py"
echo "   2. Then query with Flink SQL"

# Made with Bob
