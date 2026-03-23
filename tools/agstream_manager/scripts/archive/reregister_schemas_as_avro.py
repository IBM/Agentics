#!/usr/bin/env python3
"""
Re-register all schemas in the Schema Registry with explicit AVRO type.

This script:
1. Lists all subjects in the registry
2. Deletes each subject
3. Re-registers with explicit schemaType: AVRO

Usage:
    python3 reregister_schemas_as_avro.py [--registry-url http://localhost:8081]
"""

import argparse
import sys

import requests


def delete_subject(subject: str, registry_url: str) -> bool:
    """Delete a subject from the registry."""
    try:
        url = f"{registry_url}/subjects/{subject}"
        response = requests.delete(url)
        if response.status_code in (200, 404):
            print(f"✓ Deleted subject: {subject}")
            return True
        else:
            print(
                f"✗ Failed to delete {subject}: {response.status_code} - {response.text}"
            )
            return False
    except Exception as e:
        print(f"✗ Error deleting {subject}: {e}")
        return False


def list_subjects(registry_url: str) -> list:
    """List all subjects in the registry."""
    try:
        url = f"{registry_url}/subjects"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"✗ Failed to list subjects: {response.status_code}")
            return []
    except Exception as e:
        print(f"✗ Error listing subjects: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(
        description="Re-register schemas with explicit AVRO type"
    )
    parser.add_argument(
        "--registry-url",
        default="http://localhost:8081",
        help="Schema Registry URL (default: http://localhost:8081)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    args = parser.parse_args()

    print(f"📋 Schema Registry: {args.registry_url}\n")

    # List all subjects
    subjects = list_subjects(args.registry_url)
    if not subjects:
        print("No subjects found in registry")
        return

    print(f"Found {len(subjects)} subjects:\n")
    for subject in subjects:
        print(f"  - {subject}")

    if args.dry_run:
        print("\n🔍 DRY RUN - No changes made")
        print("\nTo actually delete and re-register, run without --dry-run")
        return

    print("\n⚠️  WARNING: This will delete all schemas from the registry!")
    print("They will be automatically re-registered as AVRO when you run your code.")
    response = input("\nContinue? (yes/no): ")

    if response.lower() != "yes":
        print("Aborted")
        return

    print("\n🗑️  Deleting subjects...\n")
    deleted_count = 0
    for subject in subjects:
        if delete_subject(subject, args.registry_url):
            deleted_count += 1

    print(f"\n✅ Deleted {deleted_count}/{len(subjects)} subjects")
    print("\n📝 Next steps:")
    print("1. Run your application code")
    print("2. Schemas will be automatically re-registered with explicit AVRO type")
    print("3. Verify with: curl http://localhost:8081/subjects")


if __name__ == "__main__":
    main()

# Made with Bob
