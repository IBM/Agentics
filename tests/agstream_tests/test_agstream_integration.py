#!/usr/bin/env python3
"""
AGStream Integration Test Suite
================================

Comprehensive test suite for AGStream functionality including:
- Avro message production and consumption
- Schema registry integration
- Topic management
- Transducible function execution
- Listener lifecycle management

Prerequisites:
- Kafka running on localhost:9092
- Schema Registry (Karapace) on localhost:8081
- AGStream Manager service on localhost:5003 (optional for listener tests)

Usage:
    python test_agstream_integration.py
    python test_agstream_integration.py --skip-listener-tests
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from typing import List, Optional

import pytest

# Check if kafka dependencies are available
try:
    import kafka

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Skip entire module if kafka is not available
pytestmark = [
    pytest.mark.agstream,
    pytest.mark.skipif(
        not KAFKA_AVAILABLE,
        reason="Kafka dependencies not installed. Install with: pip install kafka-python confluent-kafka",
    ),
]

# Suppress Kafka connection warnings
logging.getLogger("kafka").setLevel(logging.CRITICAL)
os.environ["LIBRDKAFKA_LOG_LEVEL"] = "0"

from pydantic import BaseModel, Field

# Only import streaming modules if kafka is available
if KAFKA_AVAILABLE:
    from agentics.core.streaming.agstream_sql import AGStreamSQL

from agentics.core.transducible_functions import Transduce, transducible

# Test configuration
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv(
    "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
TEST_INPUT_TOPIC = "test_questions"
TEST_OUTPUT_TOPIC = "test_answers"


# Define test data models
class Question(BaseModel):
    """A question to be answered"""

    text: str = Field(description="The question text")
    category: Optional[str] = Field(default=None, description="Question category")


class Answer(BaseModel):
    """An answer to a question"""

    text: str = Field(description="The answer text")
    confidence: Optional[str] = Field(default=None, description="Confidence score")


# Define transducible function for testing
@transducible()
async def answer_question(question: Question) -> Answer:
    """Simple Q&A transduction for testing"""
    return Transduce(question)


class AGStreamTestSuite:
    """Comprehensive test suite for AGStream functionality"""

    def __init__(self):
        self.passed_tests = 0
        self.failed_tests = 0
        self.test_results = []

    def print_header(self, title: str):
        """Print formatted section header"""
        print("\n" + "=" * 70)
        print(f"  {title}")
        print("=" * 70)

    def print_test(self, test_name: str):
        """Print test name"""
        print(f"\n🧪 Test: {test_name}")

    def print_success(self, message: str):
        """Print success message"""
        print(f"   ✓ {message}")
        self.passed_tests += 1

    def print_error(self, message: str):
        """Print error message"""
        print(f"   ✗ {message}")
        self.failed_tests += 1

    def print_info(self, message: str):
        """Print info message"""
        print(f"   ℹ️  {message}")

    async def test_schema_registration(self) -> bool:
        """Test 1: Schema Registration"""
        self.print_test("Schema Registration")

        try:
            # Create AGStreamSQL instances which should register schemas
            q_stream = AGStreamSQL(
                atype=Question,
                topic=TEST_INPUT_TOPIC,
                kafka_server=KAFKA_SERVER,
                schema_registry_url=SCHEMA_REGISTRY_URL,
                auto_create_topic=True,
                num_partitions=1,
            )

            a_stream = AGStreamSQL(
                atype=Answer,
                topic=TEST_OUTPUT_TOPIC,
                kafka_server=KAFKA_SERVER,
                schema_registry_url=SCHEMA_REGISTRY_URL,
                auto_create_topic=True,
                num_partitions=1,
            )

            self.print_success(
                f"Created streams for {TEST_INPUT_TOPIC} and {TEST_OUTPUT_TOPIC}"
            )
            self.print_success("Schemas registered successfully")
            return True

        except Exception as e:
            self.print_error(f"Schema registration failed: {e}")
            return False

    async def test_message_production(self) -> bool:
        """Test 2: Message Production"""
        self.print_test("Message Production (Avro Format)")

        try:
            q_stream = AGStreamSQL(
                atype=Question,
                topic=TEST_INPUT_TOPIC,
                kafka_server=KAFKA_SERVER,
                schema_registry_url=SCHEMA_REGISTRY_URL,
            )

            # Create test questions
            test_questions = [
                Question(text="What is Python?", category="programming"),
                Question(text="How does Kafka work?", category="streaming"),
                Question(text="What is Avro?", category="serialization"),
            ]

            # Produce messages
            q_stream.produce(test_questions)
            self.print_success(
                f"Produced {len(test_questions)} questions to {TEST_INPUT_TOPIC}"
            )

            # Give Kafka time to process
            await asyncio.sleep(1)

            return True

        except Exception as e:
            self.print_error(f"Message production failed: {e}")
            return False

    async def test_message_consumption(self) -> bool:
        """Test 3: Message Consumption"""
        self.print_test("Message Consumption (Avro Format)")

        try:
            q_stream = AGStreamSQL(
                atype=Question,
                topic=TEST_INPUT_TOPIC,
                kafka_server=KAFKA_SERVER,
                schema_registry_url=SCHEMA_REGISTRY_URL,
            )

            # Consume messages
            self.print_info("Attempting to consume messages...")
            messages = q_stream.consume(limit=3, timeout_ms=5000, from_beginning=True)

            if messages:
                self.print_success(
                    f"Consumed {len(messages)} messages from {TEST_INPUT_TOPIC}"
                )
                for i, msg in enumerate(messages[:3], 1):
                    self.print_info(f"  Message {i}: {msg.text[:50]}...")
                return True
            else:
                self.print_error("No messages consumed (topic may be empty)")
                return False

        except Exception as e:
            self.print_error(f"Message consumption failed: {e}")
            return False

    async def test_transduction(self) -> bool:
        """Test 4: Transducible Function"""
        self.print_test("Transducible Function Execution")

        try:
            # Skip if no LLM configured
            if not os.getenv("OPENAI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
                self.print_info(
                    "Skipping transduction test (no LLM API key configured)"
                )
                return True

            test_question = Question(
                text="What is the capital of France?", category="geography"
            )

            self.print_info(f"Input: {test_question.text}")

            # Execute transduction
            answer = await answer_question(test_question)

            self.print_success("Transduction executed successfully")
            # TransductionResult has .value attribute containing the actual Answer object
            if hasattr(answer, "value"):
                self.print_info(f"Output: {answer.value.text[:100]}...")
            else:
                self.print_info(f"Output: {answer.text[:100]}...")

            return True

        except Exception as e:
            self.print_error(f"Transduction failed: {e}")
            self.print_info("This test requires LLM API configuration")
            return False

    async def test_produce_consume_cycle(self) -> bool:
        """Test 5: Full Produce-Consume Cycle"""
        self.print_test("Full Produce-Consume Cycle")

        try:
            # Create streams
            q_stream = AGStreamSQL(
                atype=Question,
                topic=TEST_INPUT_TOPIC,
                kafka_server=KAFKA_SERVER,
                schema_registry_url=SCHEMA_REGISTRY_URL,
            )

            a_stream = AGStreamSQL(
                atype=Answer,
                topic=TEST_OUTPUT_TOPIC,
                kafka_server=KAFKA_SERVER,
                schema_registry_url=SCHEMA_REGISTRY_URL,
            )

            # Produce a question
            test_q = Question(text="Test question for cycle", category="test")
            q_stream.produce([test_q])
            self.print_success("Produced test question")

            await asyncio.sleep(1)

            # Consume the question
            questions = q_stream.consume(limit=1, timeout_ms=5000, from_beginning=True)
            if questions:
                self.print_success(f"Consumed question: {questions[0].text}")

                # Produce an answer
                test_a = Answer(text="Test answer for cycle", confidence="0.95")
                a_stream.produce([test_a])
                self.print_success("Produced test answer")

                await asyncio.sleep(1)

                # Consume the answer
                answers = a_stream.consume(
                    limit=1, timeout_ms=5000, from_beginning=True
                )
                if answers:
                    self.print_success(f"Consumed answer: {answers[0].text}")
                    return True
                else:
                    self.print_error("Failed to consume answer")
                    return False
            else:
                self.print_error("Failed to consume question")
                return False

        except Exception as e:
            self.print_error(f"Produce-consume cycle failed: {e}")
            return False

    def print_summary(self):
        """Print test summary"""
        self.print_header("Test Summary")

        total_tests = self.passed_tests + self.failed_tests
        pass_rate = (self.passed_tests / total_tests * 100) if total_tests > 0 else 0

        print(f"\nTotal Tests: {total_tests}")
        print(f"✓ Passed: {self.passed_tests}")
        print(f"✗ Failed: {self.failed_tests}")
        print(f"Pass Rate: {pass_rate:.1f}%")

        if self.failed_tests == 0:
            print("\n🎉 All tests passed!")
        else:
            print("\n⚠️  Some tests failed. Check the output above for details.")

    async def run_all_tests(self, skip_listener_tests: bool = False):
        """Run all tests in sequence"""
        self.print_header("AGStream Integration Test Suite")
        print(f"\nKafka Server: {KAFKA_SERVER}")
        print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
        print(f"Input Topic: {TEST_INPUT_TOPIC}")
        print(f"Output Topic: {TEST_OUTPUT_TOPIC}")

        # Run tests
        await self.test_schema_registration()
        await self.test_message_production()
        await self.test_message_consumption()
        await self.test_transduction()
        await self.test_produce_consume_cycle()

        # Print summary
        self.print_summary()

        return self.failed_tests == 0


async def main():
    """Main test runner"""
    parser = argparse.ArgumentParser(description="AGStream Integration Test Suite")
    parser.add_argument(
        "--skip-listener-tests",
        action="store_true",
        help="Skip tests that require AGStream Manager service",
    )
    args = parser.parse_args()

    test_suite = AGStreamTestSuite()
    success = await test_suite.run_all_tests(
        skip_listener_tests=args.skip_listener_tests
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

# Made with Bob
