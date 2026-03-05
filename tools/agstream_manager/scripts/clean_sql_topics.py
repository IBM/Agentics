#!/usr/bin/env python3
"""
Clean SQL topics and schemas completely
"""

import time

import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError


def delete_topics():
    """Delete Kafka topics"""
    print("🗑️  Step 1: Deleting Kafka topics...")

    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", client_id="cleanup_client"
    )

    topics_to_delete = ["questions_sql", "answers_sql"]

    try:
        admin_client.delete_topics(topics=topics_to_delete, timeout_ms=5000)
        print(f"  ✓ Deleted topics: {', '.join(topics_to_delete)}")
    except UnknownTopicOrPartitionError:
        print("  (Topics not found - already deleted)")
    except Exception as e:
        print(f"  ⚠️  Error: {e}")
    finally:
        admin_client.close()


def delete_schemas():
    """Delete Schema Registry subjects (soft + permanent delete)"""
    print("\n🗑️  Step 2: Deleting Schema Registry subjects...")

    schema_registry_url = "http://localhost:8081"
    subjects = ["Question-value", "Answer-value"]

    for subject in subjects:
        try:
            # Soft delete first
            response = requests.delete(f"{schema_registry_url}/subjects/{subject}")
            if response.status_code in (200, 404):
                print(f"  ✓ Soft deleted: {subject}")
            else:
                print(f"  ⚠️  Soft delete {subject}: {response.status_code}")

            # Permanent delete (required to fully remove schema)
            response = requests.delete(
                f"{schema_registry_url}/subjects/{subject}?permanent=true"
            )
            if response.status_code in (200, 404):
                print(f"  ✓ Permanently deleted: {subject}")
            else:
                print(f"  ⚠️  Permanent delete {subject}: {response.status_code}")
        except Exception as e:
            print(f"  ⚠️  Error deleting {subject}: {e}")


def main():
    delete_topics()
    delete_schemas()

    print("\n⏳ Step 3: Waiting 3 seconds for cleanup...")
    time.sleep(3)

    print("\n✅ Cleanup complete!")
    print("\n📝 Next steps:")
    print("   1. Run: python examples/agstream_sql_example.py")
    print("   2. Then query with Flink SQL\n")


if __name__ == "__main__":
    main()

# Made with Bob
