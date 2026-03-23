#!/usr/bin/env python3
"""Test script to check topic deletion behavior"""

import pytest
import requests
from kafka.admin import KafkaAdminClient

pytestmark = pytest.mark.agstream

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
API_URL = "http://localhost:5003/api"


def list_kafka_topics():
    """List topics directly from Kafka"""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    topics = admin.list_topics()
    admin.close()
    return sorted(topics)


def list_schema_subjects():
    """List subjects from schema registry"""
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
    return sorted(response.json())


def list_api_topics():
    """List topics from API"""
    response = requests.get(f"{API_URL}/topics")
    return sorted(response.json()["topics"])


print("=" * 70)
print("BEFORE DELETION")
print("=" * 70)

kafka_topics = list_kafka_topics()
print(f"\nKafka Topics ({len(kafka_topics)}):")
for topic in kafka_topics:
    print(f"  - {topic}")

schema_subjects = list_schema_subjects()
print(f"\nSchema Registry Subjects ({len(schema_subjects)}):")
for subject in schema_subjects:
    print(f"  - {subject}")

api_topics = list_api_topics()
print(f"\nAPI Topics ({len(api_topics)}):")
for topic in api_topics:
    print(f"  - {topic}")

# Test deleting a topic
test_topic = "songs"
print(f"\n{'=' * 70}")
print(f"DELETING TOPIC: {test_topic}")
print("=" * 70)

response = requests.delete(f"{API_URL}/topics/{test_topic}")
print(f"Status: {response.status_code}")
print(f"Response: {response.json()}")

print(f"\n{'=' * 70}")
print("AFTER DELETION")
print("=" * 70)

kafka_topics_after = list_kafka_topics()
print(f"\nKafka Topics ({len(kafka_topics_after)}):")
for topic in kafka_topics_after:
    marker = " [DELETED]" if topic not in kafka_topics else ""
    print(f"  - {topic}{marker}")

schema_subjects_after = list_schema_subjects()
print(f"\nSchema Registry Subjects ({len(schema_subjects_after)}):")
for subject in schema_subjects_after:
    marker = " [STILL EXISTS!]" if subject == f"{test_topic}-value" else ""
    print(f"  - {subject}{marker}")

api_topics_after = list_api_topics()
print(f"\nAPI Topics ({len(api_topics_after)}):")
for topic in api_topics_after:
    marker = " [STILL SHOWS!]" if topic == test_topic else ""
    print(f"  - {topic}{marker}")

# Check if the issue exists
if test_topic in api_topics_after:
    print(
        f"\n⚠️  ISSUE FOUND: Topic '{test_topic}' still appears in API after deletion!"
    )
if f"{test_topic}-value" in schema_subjects_after:
    print(f"⚠️  ISSUE FOUND: Schema '{test_topic}-value' still exists in registry!")
if test_topic not in kafka_topics_after:
    print(f"✓ Topic '{test_topic}' successfully removed from Kafka")

# Made with Bob
