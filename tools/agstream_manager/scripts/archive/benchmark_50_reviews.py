#!/usr/bin/env python3
"""
Benchmark processing 50 reviews using Flink SQL with timing
"""
import subprocess
import sys
import time


def create_flink_job():
    """Create a Flink SQL job that processes all reviews and writes to output topic"""

    sql_statements = """
-- Create output topic for results
CREATE TABLE IF NOT EXISTS review_sentiments (
    review_id STRING,
    text STRING,
    sentiment STRING,
    PRIMARY KEY (review_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'review_sentiments',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'raw',
    'value.format' = 'json'
);

-- Process reviews with sentiment analysis
INSERT INTO review_sentiments
SELECT
    review_id,
    text,
    agmap_row(text, 'Analyze the sentiment of this product review. Return only one word: positive, negative, or neutral.') as sentiment
FROM product_reviews;
"""

    # Write SQL to temp file
    with open("/tmp/benchmark_query.sql", "w") as f:
        f.write(sql_statements)

    print("=" * 80)
    print("SUBMITTING FLINK SQL JOB")
    print("=" * 80)
    print(sql_statements)
    print("=" * 80)

    # Submit job
    cmd = [
        "docker",
        "exec",
        "flink-jobmanager",
        "bash",
        "-c",
        "cat > /tmp/job.sql && /opt/flink/bin/sql-client.sh -f /tmp/job.sql",
    ]

    try:
        # Write SQL to container
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "flink-jobmanager",
                "bash",
                "-c",
                "cat > /tmp/job.sql",
            ],
            input=sql_statements.encode(),
            check=True,
        )

        # Execute SQL
        result = subprocess.run(
            [
                "docker",
                "exec",
                "flink-jobmanager",
                "bash",
                "-c",
                "cd /opt/flink && ./bin/sql-client.sh -f /tmp/job.sql",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        print("\nJob submission output:")
        print(result.stdout)
        if result.stderr:
            print("\nStderr:")
            print(result.stderr)

        return True
    except Exception as e:
        print(f"Error submitting job: {e}")
        return False


def monitor_output_topic(expected_count=50, timeout=120):
    """Monitor the output topic and measure processing time"""

    print("\n" + "=" * 80)
    print("MONITORING OUTPUT TOPIC: review_sentiments")
    print("=" * 80)

    start_time = time.time()
    last_count = 0

    while True:
        elapsed = time.time() - start_time

        if elapsed > timeout:
            print(f"\nTimeout reached after {elapsed:.2f} seconds")
            break

        # Count messages in output topic
        try:
            result = subprocess.run(
                [
                    "python3",
                    "tools/agstream_manager/scripts/kafka_query.py",
                    "review_sentiments",
                    "--limit",
                    "100",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            lines = result.stdout.strip().split("\n")
            # Count actual data lines (skip headers and empty lines)
            count = len([l for l in lines if l.strip() and "review_" in l])

            if count > last_count:
                print(
                    f"[{elapsed:.1f}s] Processed: {count}/{expected_count} reviews ({count/elapsed:.2f} reviews/sec)"
                )
                last_count = count

            if count >= expected_count:
                print(f"\n✅ All {expected_count} reviews processed!")
                print(f"Total time: {elapsed:.2f} seconds")
                print(f"Average rate: {count/elapsed:.2f} reviews/second")
                print(f"Average time per review: {elapsed/count:.2f} seconds")
                return True

        except Exception as e:
            print(f"Error checking output: {e}")

        time.sleep(2)

    return False


def check_for_stack_traces():
    """Check Flink logs for any stack traces or execution trace prompts"""

    print("\n" + "=" * 80)
    print("CHECKING FOR STACK TRACES IN LOGS")
    print("=" * 80)

    try:
        # Check taskmanager logs
        result = subprocess.run(
            ["docker", "logs", "--tail", "100", "flink-taskmanager"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        logs = result.stdout + result.stderr

        # Look for problematic patterns
        issues = []
        if "Execution Traces" in logs:
            issues.append("❌ Found 'Execution Traces' prompt")
        if "Stack trace" in logs:
            issues.append("❌ Found 'Stack trace' in logs")
        if "CREWAI_ALLOW_STACK_TRACES" in logs:
            issues.append("✅ CREWAI_ALLOW_STACK_TRACES environment variable is set")

        if issues:
            print("\n".join(issues))
        else:
            print("✅ No stack traces or execution trace prompts found")

        return len([i for i in issues if i.startswith("❌")]) == 0

    except Exception as e:
        print(f"Error checking logs: {e}")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("FLINK SQL BENCHMARK - 50 REVIEWS WITH SENTIMENT ANALYSIS")
    print("=" * 80)
    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Step 1: Submit Flink job
    if not create_flink_job():
        print("Failed to create Flink job")
        sys.exit(1)

    # Step 2: Monitor output and measure performance
    print("\nWaiting 5 seconds for job to start...")
    time.sleep(5)

    success = monitor_output_topic(expected_count=50, timeout=120)

    # Step 3: Check for stack traces
    no_traces = check_for_stack_traces()

    print("\n" + "=" * 80)
    print(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Processing: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"No stack traces: {'✅ YES' if no_traces else '❌ NO'}")
    print("=" * 80)

    sys.exit(0 if (success and no_traces) else 1)

# Made with Bob
