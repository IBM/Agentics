#!/usr/bin/env python3
"""
Simple Kafka Topic Query Tool
Query Kafka topics without PyFlink complexity
"""

import json
import os
import sys
from pathlib import Path

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    print("❌ kafka-python not installed!")
    print("Install with: pip install kafka-python")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("❌ requests not installed!")
    print("Install with: pip install requests")
    sys.exit(1)


def query_topic(
    topic_name,
    kafka_bootstrap="localhost:9092",
    schema_registry_url="http://localhost:8081",
    limit=10,
):
    """Query a Kafka topic and display results"""

    print(f"🔍 Querying topic: {topic_name}")
    print(f"   Kafka: {kafka_bootstrap}")
    print(f"   Limit: {limit} messages")
    print()

    try:
        # Create consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,  # 5 second timeout
            value_deserializer=lambda m: m.decode("utf-8") if m else None,
        )

        print("📊 Messages:")
        print("=" * 80)

        count = 0
        for message in consumer:
            try:
                # Try to parse as JSON
                data = json.loads(message.value)
                print(f"\nMessage {count + 1}:")
                print(json.dumps(data, indent=2))
            except:
                # If not JSON, print raw
                print(f"\nMessage {count + 1}: {message.value}")

            count += 1
            if count >= limit:
                break

        print("=" * 80)

        if count == 0:
            print("📭 No messages found in topic (topic may be empty)")
        else:
            print(f"\nTotal messages shown: {count}")

        consumer.close()
        return True

    except KafkaError as e:
        print(f"✗ Kafka error: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def list_topics(kafka_bootstrap="localhost:9092"):
    """List all available Kafka topics"""
    try:
        from kafka import KafkaAdminClient

        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_bootstrap, client_id="kafka-query-tool"
        )

        topics = admin_client.list_topics()
        user_topics = [t for t in topics if not t.startswith("_")]

        print(f"📋 Available Topics ({len(user_topics)}):")
        print("=" * 80)
        for topic in sorted(user_topics):
            print(f"  - {topic}")
        print("=" * 80)

        admin_client.close()
        return user_topics

    except Exception as e:
        print(f"✗ Error listing topics: {e}")
        return []


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Query Kafka topics")
    parser.add_argument("topic", nargs="?", help="Topic name to query")
    parser.add_argument(
        "--limit", type=int, default=10, help="Number of messages to show (default: 10)"
    )
    parser.add_argument(
        "--kafka",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--schema-registry",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Schema Registry URL",
    )
    parser.add_argument("--list", action="store_true", help="List all topics")

    args = parser.parse_args()

    print("=" * 80)
    print("🔍 Kafka Topic Query Tool")
    print("=" * 80)
    print()

    if args.list or not args.topic:
        list_topics(args.kafka)
        if not args.topic:
            print("\nUsage: kafka_query.py <topic_name> [--limit 10]")
            return
        print()

    if args.topic:
        query_topic(args.topic, args.kafka, args.schema_registry, args.limit)


if __name__ == "__main__":
    main()

# Made with Bob
