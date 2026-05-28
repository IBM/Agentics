#!/usr/bin/env python3
"""
AGStream Produce & Collect Demo
================================

Demonstrates producing and collecting messages using AGStreamSQL and AGStream
with the full AGStream Manager infrastructure.

Services Required:
- Kafka Broker (localhost:9092)
- Schema Registry (localhost:8081)
- AGStream Manager (localhost:5003)
- Flink Cluster (localhost:8085) - optional for SQL queries

This script shows:
1. Producing messages with AGStreamSQL
2. Collecting messages with different modes
3. Request-response pattern with collect_by_key
4. Schema registry integration
"""

import asyncio
import logging
import os
import sys
import time
from typing import Optional

from pydantic import BaseModel, Field

# Suppress Kafka connection warnings
logging.getLogger("kafka").setLevel(logging.CRITICAL)
os.environ["LIBRDKAFKA_LOG_LEVEL"] = "0"

from agentics.core.streaming import AGStream

# ============================================================================
# Data Models
# ============================================================================


class SensorReading(BaseModel):
    """IoT sensor reading"""

    sensor_id: str
    temperature: float
    humidity: float
    timestamp: int
    location: str = "unknown"


class Alert(BaseModel):
    """Alert generated from sensor data"""

    sensor_id: str
    alert_type: str
    severity: str
    message: str
    timestamp: int


class Question(BaseModel):
    """A question to be answered"""

    text: str
    category: str = "general"
    timestamp: int


class Answer(BaseModel):
    """An answer to a question"""

    text: str
    confidence: float = 1.0
    timestamp: int


# ============================================================================
# Demo Functions
# ============================================================================


def demo_1_basic_produce_collect():
    """Demo 1: Basic produce and collect with AGStream"""
    print("\n" + "=" * 70)
    print("DEMO 1: Basic Produce & Collect")
    print("=" * 70)

    # Create stream for sensor readings
    print("\n📝 Creating AGStream for sensor readings...")
    sensor_stream = AGStream(
        atype=SensorReading,
        topic="sensor_readings",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
        num_partitions=3,
    )

    # Produce some sensor readings
    print("\n📤 Producing sensor readings...")
    readings = [
        SensorReading(
            sensor_id="sensor-001",
            temperature=22.5,
            humidity=45.0,
            timestamp=int(time.time()),
            location="warehouse-a",
        ),
        SensorReading(
            sensor_id="sensor-002",
            temperature=28.3,
            humidity=60.5,
            timestamp=int(time.time()),
            location="warehouse-b",
        ),
        SensorReading(
            sensor_id="sensor-003",
            temperature=19.8,
            humidity=40.2,
            timestamp=int(time.time()),
            location="warehouse-c",
        ),
    ]

    message_ids = sensor_stream.produce(readings)
    print(f"✅ Produced {len(message_ids)} sensor readings")
    for i, msg_id in enumerate(message_ids):
        print(f"   [{i+1}] ID: {msg_id[:8]}... → {readings[i].sensor_id}")

    # Wait a moment for messages to be available
    print("\n⏳ Waiting for messages to be available...")
    time.sleep(2)

    # Collect the messages back
    print("\n📥 Collecting sensor readings...")
    collected = sensor_stream.consume(limit=10, from_beginning=True)
    print(f"✅ Collected {len(collected)} sensor readings:")
    for reading in collected:
        print(
            f"   - {reading.sensor_id}: {reading.temperature}°C, "
            f"{reading.humidity}% @ {reading.location}"
        )


def demo_2_collect_modes():
    """Demo 2: Different collection modes"""
    print("\n" + "=" * 70)
    print("DEMO 2: Collection Modes")
    print("=" * 70)

    # Create stream
    alert_stream = AGStream(
        atype=Alert,
        topic="system_alerts",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
    )

    # Produce some alerts
    print("\n📤 Producing alerts...")
    alerts = [
        Alert(
            sensor_id="sensor-001",
            alert_type="temperature",
            severity="warning",
            message="Temperature above threshold",
            timestamp=int(time.time()),
        ),
        Alert(
            sensor_id="sensor-002",
            alert_type="humidity",
            severity="critical",
            message="Humidity critically high",
            timestamp=int(time.time()),
        ),
    ]
    alert_stream.produce(alerts)
    print(f"✅ Produced {len(alerts)} alerts")

    time.sleep(1)

    # Collect from beginning
    print("\n📥 Mode 1: Collect from beginning...")
    collected = alert_stream.consume(limit=5, from_beginning=True)
    print(f"   Collected {len(collected)} alerts")

    # Produce one more alert
    print("\n📤 Producing one more alert...")
    new_alert = Alert(
        sensor_id="sensor-003",
        alert_type="connection",
        severity="info",
        message="Sensor reconnected",
        timestamp=int(time.time()),
    )
    alert_stream.produce([new_alert])

    time.sleep(1)

    # Collect only latest (should get the new one)
    print("\n📥 Mode 2: Collect latest only...")
    latest_stream = AGStream(
        atype=Alert,
        topic="system_alerts",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        consumer_group=f"latest-consumer-{int(time.time())}",
    )
    latest = latest_stream.consume(limit=1, from_beginning=False)
    print(f"   Collected {len(latest)} latest alert(s)")
    if latest:
        print(f"   Latest: {latest[0].alert_type} - {latest[0].message}")


def demo_3_continuous_listener():
    """Demo 3: Continuous listener with async transduction"""
    print("\n" + "=" * 70)
    print("DEMO 3: Continuous Listener with LLM (5 seconds)")
    print("=" * 70)

    from agentics.core.transducible_functions import make_transducible_function

    # Create input and output streams
    question_stream = AGStream(
        atype=Question,
        topic="questions_demo",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
    )

    answer_stream = AGStream(
        atype=Answer,
        topic="answers_demo",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
    )

    # Create async transducible function using LLM
    print("\n🤖 Creating LLM-based transducible function...")
    async_answer_fn = make_transducible_function(
        InputModel=Question,
        OutputModel=Answer,
        instructions="""Answer the question concisely and accurately.
        Provide a confidence score between 0 and 1 based on how certain you are of the answer.""",
    )

    # Wrapper to run async function synchronously in the listener
    def answer_question(question: Question) -> Answer:
        """Wrapper that runs async transducible function synchronously"""
        print(f"   🔄 Processing: {question.text}")
        result = asyncio.run(async_answer_fn(question))
        return result.value  # Extract the Answer from TransductionResult

    # Produce some questions
    print("\n📤 Producing questions...")
    questions = [
        Question(
            text="What is AGStream?", category="technical", timestamp=int(time.time())
        ),
        Question(
            text="How does Kafka work?",
            category="technical",
            timestamp=int(time.time()),
        ),
    ]
    question_stream.produce(questions)
    print(f"✅ Produced {len(questions)} questions")

    # Start listener in background (will run for 5 seconds)
    print("\n🎧 Starting listener for 5 seconds...")
    print("   (Processing questions → answers)")

    try:
        question_stream.listen(
            transduction_fn=answer_question,
            output_stream=answer_stream,
            timeout_ms=1000,
            max_iterations=5,  # Run for ~5 seconds
            verbose=True,
            auto_offset_reset="earliest",
        )
    except KeyboardInterrupt:
        print("\n⚠️  Listener interrupted")

    # Collect the answers
    print("\n📥 Collecting generated answers...")
    time.sleep(1)
    answers = answer_stream.consume(limit=10, from_beginning=True)
    print(f"✅ Collected {len(answers)} answers:")
    for answer in answers:
        print(f"   - {answer.text[:50]}... (confidence: {answer.confidence})")


def demo_4_schema_registry_info():
    """Demo 4: Schema Registry Information"""
    print("\n" + "=" * 70)
    print("DEMO 4: Schema Registry Information")
    print("=" * 70)

    import requests

    schema_registry_url = "http://localhost:8081"

    try:
        # List all subjects
        print("\n📋 Registered Schemas:")
        response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
        if response.status_code == 200:
            subjects = response.json()
            print(f"   Found {len(subjects)} schema(s):")
            for subject in subjects:
                print(f"   - {subject}")

                # Get schema details
                detail_response = requests.get(
                    f"{schema_registry_url}/subjects/{subject}/versions/latest",
                    timeout=5,
                )
                if detail_response.status_code == 200:
                    details = detail_response.json()
                    print(
                        f"     ID: {details.get('id')}, Version: {details.get('version')}"
                    )
        else:
            print(f"   ⚠️  Could not fetch schemas: {response.status_code}")

    except Exception as e:
        print(f"   ⚠️  Error connecting to Schema Registry: {e}")


def demo_5_service_health_check():
    """Demo 5: Check all service health"""
    print("\n" + "=" * 70)
    print("DEMO 5: Service Health Check")
    print("=" * 70)

    import requests

    services = {
        "AGStream Manager": "http://localhost:5003/health",
        "Schema Registry": "http://localhost:8081/subjects",
        "Kafka Connect": "http://localhost:8083/",
        "Control Center": "http://localhost:9021/",
        "Flink JobManager": "http://localhost:8085/overview",
    }

    print("\n🏥 Checking service health...")
    for name, url in services.items():
        try:
            response = requests.get(url, timeout=3)
            if response.status_code in (200, 201):
                print(f"   ✅ {name}: Online")
            else:
                print(f"   ⚠️  {name}: Responded with {response.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"   ❌ {name}: Offline (connection refused)")
        except requests.exceptions.Timeout:
            print(f"   ⏱️  {name}: Timeout")
        except Exception as e:
            print(f"   ❌ {name}: Error - {e}")

    # Check Kafka broker separately (not HTTP)
    print("\n🧱 Checking Kafka Broker...")
    try:
        from kafka import KafkaAdminClient

        admin = KafkaAdminClient(
            bootstrap_servers="localhost:9092", client_id="health-check"
        )
        topics = admin.list_topics()
        print(f"   ✅ Kafka Broker: Online ({len(topics)} topics)")
        admin.close()
    except Exception as e:
        print(f"   ❌ Kafka Broker: Error - {e}")


# ============================================================================
# Main
# ============================================================================


def main():
    """Run all demos"""
    print("\n" + "=" * 70)
    print("AGStream Produce & Collect Demo")
    print("=" * 70)
    print("\nThis demo requires the following services to be running:")
    print("  - Kafka Broker (localhost:9092)")
    print("  - Schema Registry (localhost:8081)")
    print("  - AGStream Manager (localhost:5003)")
    print("\nPress Ctrl+C at any time to stop.")
    print("=" * 70)

    try:
        # Run demos
        demo_5_service_health_check()
        demo_1_basic_produce_collect()
        demo_2_collect_modes()
        demo_3_continuous_listener()
        demo_4_schema_registry_info()

        print("\n" + "=" * 70)
        print("✅ All demos completed successfully!")
        print("=" * 70)
        print("\n💡 Next Steps:")
        print("   1. Check Schema Registry UI: http://localhost:8081")
        print("   2. Check Control Center: http://localhost:9021")
        print("   3. Check AGStream Manager: http://localhost:5003")
        print("   4. Query data with Flink SQL (see examples/agstream_sql_example.py)")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Error running demo: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


# Made with Bob
