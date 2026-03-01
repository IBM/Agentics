"""
Example demonstrating Schema Registry integration with AGStream.

This example shows how to:
1. Register Pydantic model schemas in Karapace Schema Registry
2. Retrieve schemas from the registry
3. Use registered schemas for type-safe streaming
"""

import time

from pydantic import BaseModel, Field

from agentics.core.streaming import AGStream
from agentics.core.streaming_utils import (
    create_kafka_topic,
    get_atype_from_registry,
    kafka_topic_exists,
    list_schema_versions,
    register_atype_schema,
)


# Define example types
class Question(BaseModel):
    """A question to be answered"""

    text: str = Field(description="The question text")
    category: str = Field(description="Question category")
    priority: int = Field(default=1, description="Priority level (1-5)")


class Answer(BaseModel):
    """An answer to a question"""

    text: str = Field(description="The answer text")
    confidence: float = Field(description="Confidence score (0-1)")
    sources: list[str] = Field(default_factory=list, description="Source references")


def example_1_register_schemas():
    """Example 1: Register schemas in the registry"""
    print("\n" + "=" * 60)
    print("Example 1: Registering Schemas")
    print("=" * 60)

    # Create AGStream instances with atypes
    question_stream = AGStream(
        atype=Question,
        input_topic="questions-topic",
        schema_registry_url="http://localhost:8081",
    )

    answer_stream = AGStream(
        atype=Answer,
        output_topic="answers-topic",
        schema_registry_url="http://localhost:8081",
    )

    # Register schemas
    print("\n📝 Registering Question schema...")
    question_schema_id = register_atype_schema(
        atype=Question, schema_registry_url="http://localhost:8081"
    )

    print("\n📝 Registering Answer schema...")
    answer_schema_id = register_atype_schema(
        atype=Answer, schema_registry_url="http://localhost:8081"
    )

    if question_schema_id and answer_schema_id:
        print(f"\n✅ Successfully registered schemas!")
        print(f"   Question schema ID: {question_schema_id}")
        print(f"   Answer schema ID: {answer_schema_id}")
    else:
        print("\n❌ Failed to register schemas")


def example_2_retrieve_schemas():
    """Example 2: Retrieve schemas from the registry"""
    print("\n" + "=" * 60)
    print("Example 2: Retrieving Schemas")
    print("=" * 60)

    # Create AGStream without atype
    stream = AGStream(
        input_topic="questions-topic", schema_registry_url="http://localhost:8081"
    )

    # Retrieve schema from registry
    print("\n🔍 Retrieving Question schema from registry...")
    RetrievedQuestion = get_atype_from_registry(
        atype_name="Question", schema_registry_url="http://localhost:8081"
    )

    if RetrievedQuestion:
        print(f"\n✅ Successfully retrieved schema: {RetrievedQuestion.__name__}")
        print(f"   Fields: {list(RetrievedQuestion.model_fields.keys())}")

        # Use the retrieved type
        stream.atype = RetrievedQuestion

        # Create an instance
        q = RetrievedQuestion(
            text="What is machine learning?", category="AI", priority=3
        )
        print(f"\n   Created instance: {q}")
    else:
        print("\n❌ Failed to retrieve schema")


def example_3_list_versions():
    """Example 3: List schema versions"""
    print("\n" + "=" * 60)
    print("Example 3: Listing Schema Versions")
    print("=" * 60)

    stream = AGStream(
        input_topic="questions-topic", schema_registry_url="http://localhost:8081"
    )

    print("\n📋 Listing all versions of Question schema...")
    versions = list_schema_versions(
        atype_name="Question", schema_registry_url="http://localhost:8081"
    )

    if versions:
        print(f"\n✅ Found {len(versions)} version(s): {versions}")

        # Retrieve specific version
        if len(versions) > 0:
            print(f"\n🔍 Retrieving version {versions[0]}...")
            schema = get_atype_from_registry(
                atype_name="Question",
                schema_registry_url="http://localhost:8081",
                version=str(versions[0]),
            )
            if schema:
                print(f"   Retrieved: {schema.__name__}")
    else:
        print("\n❌ No versions found or error occurred")


def example_4_end_to_end():
    """Example 4: End-to-end workflow with schema registry"""
    print("\n" + "=" * 60)
    print("Example 4: End-to-End Workflow")
    print("=" * 60)

    # Step 1: Register schemas
    print("\n📝 Step 1: Registering schemas...")
    question_stream = AGStream(
        atype=Question,
        input_topic="demo-questions",
        output_topic="demo-answers",
        schema_registry_url="http://localhost:8081",
    )

    register_atype_schema(atype=Question, schema_registry_url="http://localhost:8081")

    # Step 2: Create and send a question
    print("\n📤 Step 2: Sending a question...")
    question_stream.states = [
        Question(
            text="What is the capital of France?", category="Geography", priority=2
        )
    ]

    # Create topic if it doesn't exist
    if not kafka_topic_exists("demo-questions"):
        create_kafka_topic("demo-questions")

    key = question_stream.produce()
    print(f"   Sent with key: {key}")

    # Step 3: Retrieve schema and collect messages
    print("\n📥 Step 3: Retrieving schema and collecting messages...")
    collector = AGStream(
        input_topic="demo-questions", schema_registry_url="http://localhost:8081"
    )

    # Get atype from registry
    RetrievedQuestion = get_atype_from_registry(
        atype_name="Question", schema_registry_url="http://localhost:8081"
    )
    if RetrievedQuestion:
        collector.atype = RetrievedQuestion

        # Collect messages
        time.sleep(2)  # Wait for message to be available
        messages = collector.collect_sources(
            mode="latest", max_messages=1, timeout_ms=5000
        )

        if messages:
            print(f"\n✅ Collected {len(messages)} message(s)")
            for msg in messages:
                if msg.states:
                    print(f"   Question: {msg.states[0]}")
        else:
            print(
                "\n⚠️  No messages collected (this is normal if topic was just created)"
            )


def main():
    """Run all examples"""
    print("\n" + "=" * 60)
    print("🚀 AGStream Schema Registry Integration Examples")
    print("=" * 60)
    print("\nPrerequisites:")
    print("  - Kafka running on localhost:9092")
    print("  - Karapace Schema Registry on localhost:8081")
    print("  - Start with: ./manage_services.sh start")

    try:
        # Run examples
        example_1_register_schemas()
        example_2_retrieve_schemas()
        example_3_list_versions()
        example_4_end_to_end()

        print("\n" + "=" * 60)
        print("✅ All examples completed!")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n⚠️  Examples interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error running examples: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

# Made with Bob
