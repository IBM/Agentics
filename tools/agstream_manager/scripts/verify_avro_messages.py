#!/usr/bin/env python3
"""
Verify Avro messages in Kafka topics
"""

import sys

from confluent_kafka.avro import AvroConsumer


def verify_topic(topic_name, schema_registry_url="http://localhost:8081"):
    """Verify and display messages from a topic"""

    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"verify_{topic_name}",
        "auto.offset.reset": "earliest",
        "schema.registry.url": schema_registry_url,
    }

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe([topic_name])

    print(f"\n{'='*70}")
    print(f"Verifying topic: {topic_name}")
    print(f"{'='*70}\n")

    messages = []
    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # Get the deserialized value
            value = msg.value()
            messages.append(value)

            print(f"Message {len(messages)}:")
            for key, val in value.items():
                print(f"  {key}: {val}")
            print()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\n✅ Found {len(messages)} messages in '{topic_name}'\n")
    return messages


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "questions_sql"
    verify_topic(topic)

# Made with Bob
