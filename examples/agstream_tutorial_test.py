#!/usr/bin/env python3
"""
AGStream Tutorial Test - Simplified version to verify AGStream works
"""

import logging
import time

from pydantic import BaseModel, Field

from agentics.core.streaming import AGStream
from agentics.core.streaming_utils import (
    create_kafka_topic,
    get_atype_from_registry,
    kafka_topic_exists,
    register_atype_schema,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Define data models
class Question(BaseModel):
    """A question with metadata"""

    text: str
    category: str = "general"
    priority: int = 1


class Answer(BaseModel):
    """An answer with metadata"""

    text: str
    question_id: str
    confidence: float = 1.0


def main():
    print("=" * 70)
    print("AGStream Tutorial Test")
    print("=" * 70)
    print()

    # Create AGStream for questions
    print("📝 Creating AGStream for questions...")
    question_stream = AGStream(
        input_topic="tutorial-questions", schema_registry_url="http://localhost:8081"
    )
    question_stream.atype = Question

    # Register schema
    print("📝 Registering Question schema...")
    register_atype_schema(Question, "http://localhost:8081")

    # Create some questions
    questions = [
        Question(text="What is machine learning?", category="AI", priority=3),
        Question(text="How does blockchain work?", category="Technology", priority=2),
        Question(
            text="What is the capital of France?", category="Geography", priority=2
        ),
    ]

    # Send questions
    print("\n📤 Sending questions to Kafka...")
    question_keys = []
    for i, question in enumerate(questions, 1):
        question_stream.states = [question]
        msg_ids = question_stream.produce(register_if_missing=True)
        key = msg_ids[0] if msg_ids else None
        question_keys.append(key)
        print(f"  ✅ [{i}/3] Sent: {question.text[:40]}... (key: {key})")
        time.sleep(0.5)

    print("\n✅ Successfully sent 3 questions!")
    print(f"   Message keys: {question_keys}")

    # Try to consume back
    print("\n📥 Consuming questions back...")
    time.sleep(2)  # Wait for messages to be available

    consumer_stream = AGStream(
        input_topic="tutorial-questions", schema_registry_url="http://localhost:8081"
    )

    # Retrieve Question type from registry
    RetrievedQuestion = get_atype_from_registry(
        atype_name="Question", schema_registry_url="http://localhost:8081"
    )

    if RetrievedQuestion:
        consumer_stream.atype = RetrievedQuestion
        print(f"✅ Retrieved Question type from registry")

        # Collect messages
        sources = consumer_stream.collect_sources(
            max_messages=5, timeout_ms=5000, mode="latest", verbose=True
        )

        print(f"\n📋 Collected {len(sources)} messages:")
        for i, source in enumerate(sources, 1):
            if source.states:
                q = source.states[0]
                print(
                    f"  {i}. {q.text} (Category: {q.category}, Priority: {q.priority})"
                )
    else:
        print("❌ Could not retrieve Question type from registry")

    print("\n" + "=" * 70)
    print("✅ AGStream Tutorial Test Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()

# Made with Bob
