#!/usr/bin/env python3
"""
Query AGStream Kafka Topics
Properly deserializes messages using Schema Registry
"""

import json
import sys

import requests
from kafka import KafkaConsumer


def get_schema(topic_name, registry_url="http://localhost:8081"):
    """Get schema for a topic"""
    try:
        response = requests.get(
            f"{registry_url}/subjects/{topic_name}-value/versions/latest"
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"Error getting schema: {e}")
        return None


def query_topic(topic_name, limit=10):
    """Query a topic and display messages"""
    print(f"\n🔍 Querying topic: {topic_name}")
    print(f"   Limit: {limit} messages\n")

    # Get schema
    schema_info = get_schema(topic_name)
    if schema_info:
        schema = json.loads(schema_info["schema"])
        print(f"📋 Schema: {schema.get('title', 'Unknown')}")
        if schema.get("properties"):
            fields = list(schema["properties"].keys())
            print(f"   Fields: {', '.join(fields)}")
        print()

    # Create consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: m,  # Get raw bytes
    )

    print("📊 Messages:")
    print("-" * 80)

    count = 0
    for message in consumer:
        try:
            # Skip Schema Registry header (first 5 bytes: magic byte + schema ID)
            if len(message.value) > 5:
                # Try to decode as JSON (skip first 5 bytes)
                json_data = message.value[5:].decode("utf-8")
                data = json.loads(json_data)

                print(
                    f"[{count+1}] Offset: {message.offset}, Partition: {message.partition}"
                )
                print(f"    Data: {json.dumps(data, indent=2)}")
                print()
            else:
                print(f"[{count+1}] Offset: {message.offset} - Message too short")
                print()

            count += 1
            if count >= limit:
                break

        except Exception as e:
            print(f"[{count+1}] Error: {e}")
            print(f"    Raw (hex): {message.value.hex()[:100]}...")
            print()
            count += 1
            if count >= limit:
                break

    consumer.close()

    if count == 0:
        print("📭 No messages found")
    else:
        print("-" * 80)
        print(f"Total messages: {count}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python query_agstream_topic.py <topic_name> [limit]")
        print("Example: python query_agstream_topic.py Q 10")
        sys.exit(1)

    topic = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    query_topic(topic, limit)

# Made with Bob
