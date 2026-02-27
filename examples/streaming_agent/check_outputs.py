"""
Check the output topics to verify both agents processed the message
"""

import json
import time

from kafka import KafkaConsumer


def check_topic(topic_name):
    print(f"\n{'='*60}")
    print(f"Checking topic: {topic_name}")
    print(f"{'='*60}")

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: m.decode("utf-8"),
    )

    message_count = 0
    for message in consumer:
        message_count += 1
        print(f"\nMessage {message_count}:")
        print(f"  Key: {message.key}")
        print(f"  Timestamp: {message.timestamp}")
        print(f"  Value: {message.value[:200]}...")  # First 200 chars

    if message_count == 0:
        print("  No messages found")
    else:
        print(f"\n✓ Found {message_count} message(s)")

    consumer.close()


if __name__ == "__main__":
    print("Checking output topics from multi-listener test...")
    check_topic("multi-output-1")  # Summarizer output
    check_topic("multi-output-2")  # Analyzer output
    print(f"\n{'='*60}")
    print("Done!")

# Made with Bob
