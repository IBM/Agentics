#!/usr/bin/env python3
"""
Diagnose Schema Registry Issues

This script helps identify and resolve schema registry mismatches, particularly
when messages reference deleted schema IDs.

Features:
- List all registered schemas with their IDs
- Identify orphaned schema IDs in Kafka topics
- Provide recovery options for missing schemas
- Skip messages with unknown schemas (error handling)

Usage:
    python3 diagnose_schema_registry.py [--registry-url URL] [--kafka-server SERVER]
    python3 diagnose_schema_registry.py --check-topic TOPIC_NAME
    python3 diagnose_schema_registry.py --list-schemas
    python3 diagnose_schema_registry.py --check-schema-id ID
"""

import argparse
import json
import sys
from typing import Dict, List, Optional, Set

import requests
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient


class SchemaRegistryDiagnostics:
    """Diagnostic tool for Schema Registry issues"""

    def __init__(
        self,
        registry_url: str = "http://localhost:8081",
        kafka_server: str = "localhost:9092",
    ):
        self.registry_url = registry_url.rstrip("/")
        self.kafka_server = kafka_server
        self.schema_registry_client = SchemaRegistryClient({"url": registry_url})

    def list_all_schemas(self) -> Dict[int, Dict]:
        """List all schemas with their IDs and subjects"""
        print(f"\n{'='*70}")
        print("📋 REGISTERED SCHEMAS")
        print(f"{'='*70}\n")

        schemas = {}
        try:
            # Get all subjects
            subjects = requests.get(f"{self.registry_url}/subjects").json()

            for subject in subjects:
                # Get latest version
                versions_url = f"{self.registry_url}/subjects/{subject}/versions"
                versions = requests.get(versions_url).json()

                if versions:
                    latest_version = versions[-1]
                    schema_url = f"{self.registry_url}/subjects/{subject}/versions/{latest_version}"
                    schema_data = requests.get(schema_url).json()

                    schema_id = schema_data.get("id")
                    schemas[schema_id] = {
                        "subject": subject,
                        "version": latest_version,
                        "schema": schema_data.get("schema"),
                    }

                    print(f"Schema ID: {schema_id}")
                    print(f"  Subject: {subject}")
                    print(f"  Version: {latest_version}")
                    print()

            print(f"✅ Found {len(schemas)} registered schemas\n")
            return schemas

        except Exception as e:
            print(f"❌ Error listing schemas: {e}")
            return {}

    def check_schema_id(self, schema_id: int) -> Optional[Dict]:
        """Check if a specific schema ID exists"""
        print(f"\n{'='*70}")
        print(f"🔍 CHECKING SCHEMA ID: {schema_id}")
        print(f"{'='*70}\n")

        try:
            # Try to get schema by ID
            url = f"{self.registry_url}/schemas/ids/{schema_id}"
            response = requests.get(url)

            if response.status_code == 200:
                schema_data = response.json()
                print(f"✅ Schema ID {schema_id} EXISTS")
                print(f"Schema: {json.dumps(schema_data, indent=2)}\n")
                return schema_data
            elif response.status_code == 404:
                print(f"❌ Schema ID {schema_id} NOT FOUND (404)")
                print(f"This schema has been deleted from the registry.\n")
                return None
            else:
                print(f"⚠️  Unexpected response: {response.status_code}")
                print(f"Response: {response.text}\n")
                return None

        except Exception as e:
            print(f"❌ Error checking schema ID: {e}\n")
            return None

    def scan_topic_for_schema_ids(
        self, topic: str, max_messages: int = 100
    ) -> Set[int]:
        """Scan a topic to find all schema IDs used in messages"""
        print(f"\n{'='*70}")
        print(f"🔍 SCANNING TOPIC: {topic}")
        print(f"{'='*70}\n")

        schema_ids = set()

        # Use raw consumer to read message headers
        consumer_config = {
            "bootstrap.servers": self.kafka_server,
            "group.id": f"schema_diagnostic_{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])

        message_count = 0
        error_count = 0

        try:
            print(f"Reading up to {max_messages} messages...\n")

            while message_count < max_messages:
                msg = consumer.poll(timeout=2.0)

                if msg is None:
                    break

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    error_count += 1
                    continue

                message_count += 1

                # Extract schema ID from message (first 5 bytes for Avro)
                value = msg.value()
                if value and len(value) >= 5:
                    # Avro wire format: [0x00][4-byte schema ID][data...]
                    if value[0] == 0:
                        schema_id = int.from_bytes(value[1:5], byteorder="big")
                        schema_ids.add(schema_id)

                        if message_count % 10 == 0:
                            print(
                                f"  Processed {message_count} messages, found {len(schema_ids)} unique schema IDs"
                            )

            print(f"\n✅ Scanned {message_count} messages")
            print(f"📊 Found {len(schema_ids)} unique schema IDs: {sorted(schema_ids)}")

            if error_count > 0:
                print(f"⚠️  Encountered {error_count} errors while reading")

            print()
            return schema_ids

        except Exception as e:
            print(f"❌ Error scanning topic: {e}\n")
            return schema_ids
        finally:
            consumer.close()

    def diagnose_topic(self, topic: str):
        """Full diagnostic of a topic's schema issues"""
        print(f"\n{'='*70}")
        print(f"🏥 FULL DIAGNOSTIC: {topic}")
        print(f"{'='*70}\n")

        # 1. Get all registered schemas
        registered_schemas = self.list_all_schemas()
        registered_ids = set(registered_schemas.keys())

        # 2. Scan topic for schema IDs
        topic_schema_ids = self.scan_topic_for_schema_ids(topic)

        # 3. Find orphaned schema IDs
        orphaned_ids = topic_schema_ids - registered_ids

        if orphaned_ids:
            print(f"{'='*70}")
            print("⚠️  ORPHANED SCHEMA IDs FOUND")
            print(f"{'='*70}\n")
            print(f"The following schema IDs are used in topic '{topic}'")
            print(f"but are NOT registered in the schema registry:\n")

            for schema_id in sorted(orphaned_ids):
                print(f"  ❌ Schema ID: {schema_id}")

            print(f"\n{'='*70}")
            print("💡 RECOVERY OPTIONS")
            print(f"{'='*70}\n")

            print("Option 1: Clean the topic (removes all messages)")
            print(f"  ./tools/agstream_manager/scripts/clean_sql_topics.sh {topic}\n")

            print("Option 2: Reset consumer offsets to skip old messages")
            print(
                f"  ./tools/agstream_manager/scripts/reset_consumer_offsets.sh <consumer-group>\n"
            )

            print("Option 3: Implement error handling in consumers")
            print("  - Modify consumers to skip messages with unknown schema IDs")
            print("  - See: verify_avro_messages_with_error_handling.py\n")

            print("Option 4: Recreate schemas (if schema definition is known)")
            print("  - Manually register the missing schema with the same ID")
            print("  - Requires knowing the original schema definition\n")

        else:
            print(f"✅ No orphaned schema IDs found in topic '{topic}'")
            print(
                f"All schema IDs in the topic are registered in the schema registry.\n"
            )

    def generate_error_handling_consumer(
        self, output_file: str = "verify_avro_messages_with_error_handling.py"
    ):
        """Generate a consumer script with error handling for missing schemas"""
        script_content = '''#!/usr/bin/env python3
"""
Verify Avro messages with error handling for missing schemas
"""

import sys
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


def verify_topic_with_error_handling(topic_name,
                                     schema_registry_url="http://localhost:8081",
                                     kafka_server="localhost:9092"):
    """Verify messages and skip those with unknown schema IDs"""

    consumer_config = {
        "bootstrap.servers": kafka_server,
        "group.id": f"verify_{topic_name}_safe",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

    print(f"\\n{'='*70}")
    print(f"Verifying topic: {topic_name} (with error handling)")
    print(f"{'='*70}\\n")

    messages = []
    skipped = 0
    errors = 0

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                print(f"Error: {msg.error()}")
                errors += 1
                continue

            # Try to extract schema ID
            value = msg.value()
            if value and len(value) >= 5 and value[0] == 0:
                schema_id = int.from_bytes(value[1:5], byteorder='big')

                try:
                    # Try to get schema
                    schema = schema_registry_client.get_schema(schema_id)

                    # Deserialize message
                    deserializer = AvroDeserializer(schema_registry_client, schema.schema_str)
                    deserialized = deserializer(value[5:], None)

                    messages.append(deserialized)
                    print(f"Message {len(messages)} (Schema ID: {schema_id}):")
                    for key, val in deserialized.items():
                        print(f"  {key}: {val}")
                    print()

                except Exception as e:
                    skipped += 1
                    print(f"⚠️  Skipped message with schema ID {schema_id}: {e}")
                    continue

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\\n{'='*70}")
    print(f"✅ Successfully processed: {len(messages)} messages")
    print(f"⚠️  Skipped (unknown schema): {skipped} messages")
    print(f"❌ Errors: {errors}")
    print(f"{'='*70}\\n")

    return messages


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "questions_sql"
    verify_topic_with_error_handling(topic)
'''

        with open(output_file, "w") as f:
            f.write(script_content)

        print(f"✅ Generated error-handling consumer script: {output_file}")
        print(f"   Make it executable: chmod +x {output_file}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose Schema Registry issues",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--registry-url",
        default="http://localhost:8081",
        help="Schema Registry URL (default: http://localhost:8081)",
    )

    parser.add_argument(
        "--kafka-server",
        default="localhost:9092",
        help="Kafka server (default: localhost:9092)",
    )

    parser.add_argument(
        "--list-schemas", action="store_true", help="List all registered schemas"
    )

    parser.add_argument(
        "--check-schema-id",
        type=int,
        metavar="ID",
        help="Check if a specific schema ID exists",
    )

    parser.add_argument(
        "--check-topic", metavar="TOPIC", help="Scan a topic for schema IDs"
    )

    parser.add_argument(
        "--diagnose-topic",
        metavar="TOPIC",
        help="Full diagnostic of a topic's schema issues",
    )

    parser.add_argument(
        "--generate-error-handler",
        action="store_true",
        help="Generate a consumer script with error handling",
    )

    args = parser.parse_args()

    diagnostics = SchemaRegistryDiagnostics(
        registry_url=args.registry_url, kafka_server=args.kafka_server
    )

    if args.list_schemas:
        diagnostics.list_all_schemas()

    elif args.check_schema_id:
        diagnostics.check_schema_id(args.check_schema_id)

    elif args.check_topic:
        diagnostics.scan_topic_for_schema_ids(args.check_topic)

    elif args.diagnose_topic:
        diagnostics.diagnose_topic(args.diagnose_topic)

    elif args.generate_error_handler:
        diagnostics.generate_error_handling_consumer()

    else:
        # Default: check schema ID 26 and list all schemas
        print("🏥 Schema Registry Diagnostics\n")
        diagnostics.check_schema_id(26)
        diagnostics.list_all_schemas()

        print("\n💡 To diagnose a specific topic, run:")
        print("   python3 diagnose_schema_registry.py --diagnose-topic <topic_name>\n")


if __name__ == "__main__":
    main()

# Made with Bob
