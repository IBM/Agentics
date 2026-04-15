#!/usr/bin/env python3
"""
AGStreamSQL Example - Flink SQL Compatible Streaming

Demonstrates how to use AGStreamSQL for Flink SQL-compatible data streaming.
AGStreamSQL uses Avro format and sends only state data (no envelope) for direct SQL access.
"""

import logging
import os

# Suppress Kafka connection warnings (IPv6 fallback to IPv4)
logging.getLogger("kafka").setLevel(logging.CRITICAL)

# Suppress librdkafka (confluent-kafka) connection warnings
os.environ["LIBRDKAFKA_LOG_LEVEL"] = "0"  # 0=EMERG, 7=DEBUG

from pydantic import BaseModel

from agentics.core import AGStreamSQL


# Define your data model
class Question(BaseModel):
    """A question with metadata"""

    text: str
    timestamp: int
    category: str = "general"


class Answer(BaseModel):
    """An answer with metadata"""

    text: str
    question_id: str
    timestamp: int
    confidence: float = 1.0


def main():
    print("=" * 70)
    print("AGStreamSQL Example - Flink SQL Compatible Streaming")
    print("=" * 70)
    print()

    # Create AGStreamSQL instance for questions
    print("📝 Creating AGStreamSQL for questions...")
    questions_stream = AGStreamSQL(
        atype=Question,
        topic="questions_sql",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
        num_partitions=3,
    )
    print()

    # Create some question states
    questions = [
        Question(
            text="What is artificial intelligence?", timestamp=1234567890, category="AI"
        ),
        Question(
            text="How does machine learning work?", timestamp=1234567891, category="ML"
        ),
        Question(text="What is deep learning?", timestamp=1234567892, category="DL"),
    ]

    # Produce questions to Kafka
    print("📤 Producing questions...")
    message_ids = questions_stream.produce(questions)
    print(f"✅ Produced {len(message_ids)} questions")
    print()

    # Create AGStreamSQL instance for answers
    print("📝 Creating AGStreamSQL for answers...")
    answers_stream = AGStreamSQL(
        atype=Answer,
        topic="answers_sql",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
        num_partitions=3,
    )
    print()

    # Create some answer states
    answers = [
        Answer(
            text="AI is the simulation of human intelligence by machines.",
            question_id=message_ids[0],
            timestamp=1234567893,
            confidence=0.95,
        ),
        Answer(
            text="ML is a subset of AI that learns from data.",
            question_id=message_ids[1],
            timestamp=1234567894,
            confidence=0.92,
        ),
    ]

    # Produce answers to Kafka
    print("📤 Producing answers...")
    answers_stream.produce(answers)
    print()

    print("=" * 70)
    print("✅ Data produced successfully!")
    print("=" * 70)
    print()

    print("🔍 Now you can query with Flink SQL:")
    print()
    print("1. Start Flink SQL client (refer to Flink documentation)")
    print()
    print("2. Create tables:")
    print()
    print("   CREATE TABLE questions_sql (")
    print("     text STRING,")
    print("     `timestamp` BIGINT,")
    print("     category STRING")
    print("   ) WITH (")
    print("     'connector' = 'kafka',")
    print("     'topic' = 'questions_sql',")
    print("     'properties.bootstrap.servers' = 'kafka:29092',")
    print("     'scan.startup.mode' = 'earliest-offset',")
    print("     'format' = 'avro-confluent',")
    print("     'avro-confluent.url' = 'http://karapace-schema-registry:8081'")
    print("   );")
    print()
    print("   Note: Use backticks around `timestamp` (reserved keyword)")
    print()
    print("   CREATE TABLE answers_sql (")
    print("     text STRING,")
    print("     question_id STRING,")
    print("     `timestamp` BIGINT,")
    print("     confidence DOUBLE")
    print("   ) WITH (")
    print("     'connector' = 'kafka',")
    print("     'topic' = 'answers_sql',")
    print("     'properties.bootstrap.servers' = 'kafka:29092',")
    print("     'scan.startup.mode' = 'earliest-offset',")
    print("     'format' = 'avro-confluent',")
    print("     'avro-confluent.url' = 'http://karapace-schema-registry:8081'")
    print("   );")
    print()
    print("3. Query the data:")
    print()
    print("   -- Get all questions")
    print("   SELECT * FROM questions_sql;")
    print()
    print("   -- Filter by category")
    print("   SELECT text, `timestamp` FROM questions_sql WHERE category = 'AI';")
    print()
    print("   -- Get high-confidence answers")
    print("   SELECT text, confidence FROM answers_sql WHERE confidence > 0.9;")
    print()
    print("   -- Join questions and answers")
    print("   SELECT q.text as question, a.text as answer, a.confidence")
    print("   FROM questions_sql q")
    print("   JOIN answers_sql a ON q.`timestamp` < a.`timestamp`;")
    print()
    print("=" * 70)
    print()

    # Note: Consumption is commented out because the main purpose is Flink SQL queries
    # If you want to test consumption, ensure the topic only has Avro-formatted messages
    #
    # print("📥 Consuming questions back (optional)...")
    # consumed_questions = questions_stream.consume(limit=10)
    # print(f"✅ Consumed {len(consumed_questions)} questions:")
    # for q in consumed_questions:
    #     print(f"   - {q.text} ({q.category})")
    # print()


if __name__ == "__main__":
    main()


# Made with Bob
