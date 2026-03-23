#!/bin/bash

# Add test questions to Q topic using Kafka console producer

echo "Adding test questions to Q topic..."
echo ""

# Questions to add
questions=(
    '{"text": "Why did the chicken cross the road?"}'
    '{"text": "What is the meaning of life?"}'
    '{"text": "How does a computer work?"}'
    '{"text": "Why do programmers prefer dark mode?"}'
    '{"text": "What is the best programming language?"}'
    '{"text": "Why did the developer go broke?"}'
    '{"text": "How do you comfort a JavaScript bug?"}'
    '{"text": "Why do Java developers wear glasses?"}'
)

# Add each question
for i in "${!questions[@]}"; do
    echo "Adding question $((i+1)): ${questions[$i]}"
    echo "${questions[$i]}" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server localhost:9092 \
        --topic Q \
        --property "value.serializer=org.apache.kafka.common.serialization.StringSerializer"
done

echo ""
echo "✓ Added ${#questions[@]} questions to Q topic"
echo ""
echo "Now you can generate jokes in Flink SQL:"
echo "  SELECT agmap('Joke', text) FROM Q LIMIT 3;"

# Made with Bob
