#!/bin/bash

# Quick command to generate fake instances for testing
# Usage: ./generate_fake_instances.sh <topic> <type_name> <n_instances> [instructions]

set -e

# Default values
TOPIC="${1:-Joke}"
TYPE_NAME="${2:-Joke}"
N_INSTANCES="${3:-10}"
INSTRUCTIONS="${4:-Generate diverse and interesting examples}"
API_URL="${AGSTREAM_API_URL:-http://localhost:5003}"

echo "============================================================"
echo "🎲 Generate Fake Instances"
echo "============================================================"
echo "Topic: $TOPIC"
echo "Type: $TYPE_NAME"
echo "Count: $N_INSTANCES"
echo "Instructions: $INSTRUCTIONS"
echo "API URL: $API_URL"
echo "============================================================"
echo ""

# Make the API call
response=$(curl -s -X POST "$API_URL/api/kafka/generate_and_produce" \
  -H "Content-Type: application/json" \
  -d "{
    \"topic\": \"$TOPIC\",
    \"type_name\": \"$TYPE_NAME\",
    \"n_instances\": $N_INSTANCES,
    \"instructions\": \"$INSTRUCTIONS\"
  }")

# Check if response contains error
if echo "$response" | grep -q '"error"'; then
    echo "❌ Error generating instances:"
    echo "$response" | python3 -m json.tool
    exit 1
else
    echo "✅ Success!"
    echo "$response" | python3 -m json.tool
fi

echo ""
echo "============================================================"
echo "💡 Quick Commands:"
echo "============================================================"
echo "# Generate 5 jokes:"
echo "./generate_fake_instances.sh Joke Joke 5 'Generate funny programming jokes'"
echo ""
echo "# Generate 10 product reviews:"
echo "./generate_fake_instances.sh Product_reviews InputText 10 'Generate electronics reviews'"
echo ""
echo "# Generate 20 custom instances:"
echo "./generate_fake_instances.sh MyTopic MyType 20 'Custom instructions here'"
echo ""
echo "# Set custom max limit (default 100):"
echo "export MAX_GENERATE_INSTANCES=500"
echo "============================================================"

# Made with Bob
