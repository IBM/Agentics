#!/usr/bin/env python3
"""
AGStream Full Demo - DEPRECATED
================================

⚠️  WARNING: This demo uses the old Flink-based AGStream implementation which has been removed.

For current AGStream functionality (Avro format, Flink SQL compatible), see:
- examples/agstream_produce_collect_demo.py - Main AGStream demo
- examples/agstream_sql_example.py - AGStream with Flink SQL queries

This file is kept for reference but will not work without the Flink dependencies.

Services Required:
- Kafka Broker (localhost:9092)
- Schema Registry (localhost:8081)
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

from agentics.core.streaming import AGStream, ListenerManager
from agentics.core.transducible_functions import make_transducible_function

# ============================================================================
# Data Models
# ============================================================================


class MovieReview(BaseModel):
    """A movie review"""

    title: str
    review_text: str
    rating: int = Field(ge=1, le=5)
    reviewer: str


class MovieSummary(BaseModel):
    """A summarized movie review"""

    title: str
    summary: str
    sentiment: str  # positive, negative, neutral
    key_points: str


class Task(BaseModel):
    """A task to be processed"""

    task_id: str
    description: str
    priority: str = "medium"


class TaskResult(BaseModel):
    """Result of processing a task"""

    task_id: str
    status: str
    output: str
    processing_time: float


# ============================================================================
# Demo Functions
# ============================================================================


def demo_1_basic_agstream():
    """Demo 1: Basic AGStream produce and collect"""
    print("\n" + "=" * 70)
    print("DEMO 1: Basic AGStream (JSON Schema with Envelope)")
    print("=" * 70)

    # Create AGStream instance
    print("\n📝 Creating AGStream for movie reviews...")
    ag = AGStream(
        atype=MovieReview,
        kafka_server="localhost:9092",
        input_topic="movie_reviews",
        output_topic="movie_summaries",
        schema_registry_url="http://localhost:8081",
        instructions="Summarize the movie review in 2-3 sentences.",
    )

    # Create some reviews
    reviews = [
        MovieReview(
            title="The Matrix",
            review_text="Mind-bending sci-fi masterpiece with groundbreaking visual effects.",
            rating=5,
            reviewer="Alice",
        ),
        MovieReview(
            title="Inception",
            review_text="Complex and layered thriller that keeps you guessing.",
            rating=5,
            reviewer="Bob",
        ),
    ]

    # Set states and produce
    print("\n📤 Producing movie reviews...")
    ag.states = reviews
    message_ids = ag.produce(register_if_missing=True)
    print(f"✅ Produced {len(message_ids)} reviews")
    for i, msg_id in enumerate(message_ids):
        print(f"   [{i+1}] {reviews[i].title} → ID: {msg_id[:8]}...")

    # Collect back
    print("\n📥 Collecting reviews...")
    time.sleep(2)
    collected_ag = AGStream(
        atype=MovieReview,
        kafka_server="localhost:9092",
        input_topic="movie_reviews",
        schema_registry_url="http://localhost:8081",
    )
    collected = collected_ag.collect_sources(
        max_messages=10, mode="all", validate_schema=True, verbose=True
    )
    print(f"\n✅ Collected {len(collected)} AGStream objects")
    for ag_obj in collected:
        if ag_obj.states:
            review = ag_obj.states[0]
            print(f"   - {review.title}: {review.rating}⭐ by {review.reviewer}")


async def demo_2_aproduce_and_collect():
    """Demo 2: Async produce and collect with ordered results"""
    print("\n" + "=" * 70)
    print("DEMO 2: aproduce_and_collect (Ordered Batch Processing)")
    print("=" * 70)

    print("\n⚠️  This demo requires:")
    print("   1. Flink connector JARs (for AGStream.listen())")
    print("   2. A running listener to process messages")
    print("   3. Complex setup with ListenerManager")
    print("\n💡 For current streaming demos, see:")
    print("   - examples/agstream_produce_collect_demo.py (AGStream)")
    print("   - Demo 3 below (ListenerManager basics)")
    print("\n⏭️  Skipping this demo to avoid Flink dependencies...")

    # Note: To make this work, you would need:
    # 1. Download Flink connector JARs
    # 2. Use transducible_function_listener instead of listen()
    # 3. Or use AGStream (formerly AGStreamSQL) which doesn't require Flink


def demo_3_listener_manager():
    """Demo 3: ListenerManager for managing multiple listeners"""
    print("\n" + "=" * 70)
    print("DEMO 3: ListenerManager (Multiple Concurrent Listeners)")
    print("=" * 70)

    # Create AGStream factory
    def make_ag(input_topic, output_topic, **kwargs):
        return AGStream(
            atype=Task,
            kafka_server="localhost:9092",
            input_topic=input_topic,
            output_topic=output_topic,
            schema_registry_url="http://localhost:8081",
        )

    # Create listener manager
    print("\n🎧 Creating listener manager...")
    mgr = ListenerManager(agstream_factory=make_ag)

    # Define processing functions
    def process_high_priority(task: Task) -> TaskResult:
        """Process high priority tasks"""
        time.sleep(0.5)  # Simulate processing
        return TaskResult(
            task_id=task.task_id,
            status="completed",
            output=f"High priority: {task.description}",
            processing_time=0.5,
        )

    def process_normal_priority(task: Task) -> TaskResult:
        """Process normal priority tasks"""
        time.sleep(1.0)  # Simulate processing
        return TaskResult(
            task_id=task.task_id,
            status="completed",
            output=f"Normal priority: {task.description}",
            processing_time=1.0,
        )

    # Start multiple listeners
    print("\n🚀 Starting multiple listeners...")
    listener1 = mgr.start(
        fn=process_high_priority,
        input_topic="tasks_high",
        output_topic="results_high",
        name="HighPriorityWorker",
        verbose=False,
    )
    print(f"   ✅ Started: HighPriorityWorker ({listener1})")

    listener2 = mgr.start(
        fn=process_normal_priority,
        input_topic="tasks_normal",
        output_topic="results_normal",
        name="NormalPriorityWorker",
        verbose=False,
    )
    print(f"   ✅ Started: NormalPriorityWorker ({listener2})")

    # Check status
    print("\n📊 Listener Status:")
    for info in mgr.list_listeners():
        print(f"   - {info.name}: {info.status}")

    # Produce some tasks
    print("\n📤 Producing tasks...")
    high_priority_ag = AGStream(
        atype=Task,
        kafka_server="localhost:9092",
        input_topic="tasks_high",
        schema_registry_url="http://localhost:8081",
    )
    high_priority_ag.states = [
        Task(task_id="T001", description="Critical bug fix", priority="high")
    ]
    high_priority_ag.produce()

    normal_priority_ag = AGStream(
        atype=Task,
        kafka_server="localhost:9092",
        input_topic="tasks_normal",
        schema_registry_url="http://localhost:8081",
    )
    normal_priority_ag.states = [
        Task(task_id="T002", description="Update documentation", priority="normal")
    ]
    normal_priority_ag.produce()

    print("✅ Tasks produced")

    # Let them process
    print("\n⏳ Processing for 5 seconds...")
    time.sleep(5)

    # Drain logs
    print("\n📋 Listener Logs:")
    for listener_id in [listener1, listener2]:
        logs = mgr.drain_logs(listener_id, max_lines=10)
        if logs:
            print(f"\n{listener_id}:")
            for log in logs[:5]:  # Show first 5 lines
                print(f"   {log.strip()}")

    # Stop all listeners
    print("\n🛑 Stopping all listeners...")
    mgr.stop_all()
    print("✅ All listeners stopped")


def demo_4_collect_by_key():
    """Demo 4: Request-response pattern with collect_by_key"""
    print("\n" + "=" * 70)
    print("DEMO 4: collect_by_key (Request-Response Pattern)")
    print("=" * 70)

    print("\n📝 This demo shows request-response pattern...")
    print("   (Requires a listener to be running - skipped in this demo)")
    print("   See demo_2_aproduce_and_collect for a working example")


# ============================================================================
# Main
# ============================================================================


async def main():
    """Run all demos"""
    print("\n" + "=" * 70)
    print("AGStream Full Demo (JSON Schema with Envelope)")
    print("=" * 70)
    print("\n⚠️  This demo uses deprecated Flink-based AGStream")
    print("  - JSON Schema format with metadata envelope (deprecated)")
    print("  - aproduce_and_collect for ordered batch processing")
    print("  - ListenerManager for managing multiple listeners")
    print("  - Advanced collection patterns")
    print("\nPress Ctrl+C at any time to stop.")
    print("=" * 70)

    try:
        # Run demos
        demo_1_basic_agstream()
        await demo_2_aproduce_and_collect()
        demo_3_listener_manager()
        demo_4_collect_by_key()

        print("\n" + "=" * 70)
        print("✅ All demos completed successfully!")
        print("=" * 70)
        print("\n💡 Key Takeaways:")
        print("   1. Old AGStream used JSON Schema (current AGStream uses Avro)")
        print("   2. aproduce_and_collect preserves order for batch processing")
        print("   3. ListenerManager handles multiple concurrent listeners")
        print("   4. Full envelope support with metadata")
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
    asyncio.run(main())


# Made with Bob
