#!/usr/bin/env python3
"""
Setup Question and Answer schemas with correct naming convention.

This script creates the Question and Answer types with proper class names
that will be used consistently by both UI and listeners.
"""

from pydantic import BaseModel, Field

from agentics.core.streaming_utils import register_atype_schema


# Define Question and Answer with full class names
class Question(BaseModel):
    """A question to be answered"""

    text: str = Field(..., description="The question text")


class Answer(BaseModel):
    """An answer to a question"""

    text: str = Field(..., description="The answer text")


def main():
    print("=" * 70)
    print("Setting up Question and Answer schemas")
    print("=" * 70)
    print()

    REGISTRY_URL = "http://localhost:8081"

    # Register Question schema
    print("Registering Question schema...")
    try:
        schema_id = register_atype_schema(
            atype=Question, schema_registry_url=REGISTRY_URL, compatibility="NONE"
        )
        print(f"✓ Question registered with schema ID: {schema_id}")
        print(f"  Subject name: Question-value")
    except Exception as e:
        print(f"✗ Failed to register Question: {e}")
        return False

    # Register Answer schema
    print("\nRegistering Answer schema...")
    try:
        schema_id = register_atype_schema(
            atype=Answer, schema_registry_url=REGISTRY_URL, compatibility="NONE"
        )
        print(f"✓ Answer registered with schema ID: {schema_id}")
        print(f"  Subject name: Answer-value")
    except Exception as e:
        print(f"✗ Failed to register Answer: {e}")
        return False

    print()
    print("=" * 70)
    print("✅ Setup complete!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Clean topics to remove old messages:")
    print("   ./tools/agstream_manager/scripts/clean_sql_topics.sh")
    print()
    print("2. Restart AGStream Manager:")
    print("   ./tools/agstream_manager/scripts/restart_agstream_manager.sh")
    print()
    print("3. In the UI, use these type names:")
    print("   - Question (not Q)")
    print("   - Answer (not A)")
    print()

    return True


if __name__ == "__main__":
    import sys

    success = main()
    sys.exit(0 if success else 1)

# Made with Bob
