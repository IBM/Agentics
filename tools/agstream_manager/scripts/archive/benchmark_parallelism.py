#!/usr/bin/env python3
"""
Benchmark script to measure Flink SQL query performance with different parallelism settings.
Tests the impact of parallelism on agmap() UDF execution time.
"""

import sys
import time
from pathlib import Path

import requests

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

FLINK_URL = "http://localhost:8085"
FLINK_SQL_GATEWAY = "http://localhost:8083"


def wait_for_job_completion(job_id, timeout=300):
    """Wait for a Flink job to complete or fail."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{FLINK_URL}/jobs/{job_id}")
            if response.status_code == 200:
                job_info = response.json()
                status = job_info.get("state", "UNKNOWN")

                if status in ["FINISHED", "FAILED", "CANCELED"]:
                    return status, time.time() - start_time

            time.sleep(1)
        except Exception as e:
            print(f"Error checking job status: {e}")
            time.sleep(1)

    return "TIMEOUT", timeout


def cancel_all_jobs():
    """Cancel all running Flink jobs."""
    try:
        response = requests.get(f"{FLINK_URL}/jobs")
        if response.status_code == 200:
            jobs = response.json().get("jobs", [])
            for job in jobs:
                if job["status"] in ["RUNNING", "CREATED"]:
                    job_id = job["id"]
                    requests.patch(f"{FLINK_URL}/jobs/{job_id}?mode=cancel")
                    print(f"Canceled job: {job_id}")
            time.sleep(2)
    except Exception as e:
        print(f"Error canceling jobs: {e}")


def run_benchmark(parallelism, n_rows=10):
    """Run benchmark with specified parallelism."""
    print(f"\n{'='*70}")
    print(f"🔬 Benchmark: Parallelism={parallelism}, Rows={n_rows}")
    print(f"{'='*70}")

    # Cancel any existing jobs
    cancel_all_jobs()

    # Create SQL statements
    output_topic = f"benchmark_output_p{parallelism}"

    sql_statements = f"""
-- Set parallelism
SET 'parallelism.default' = '{parallelism}';

-- Create output table
CREATE TABLE IF NOT EXISTS {output_topic} (
    customer_review STRING,
    sentiment STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{output_topic}',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'sink.parallelism' = '{parallelism}'
);

-- Run benchmark query
INSERT INTO {output_topic}
SELECT
    customer_review,
    agmap('Sentiment', customer_review) as sentiment
FROM pr
LIMIT {n_rows};
"""

    print(f"\n📝 SQL Query:")
    print(sql_statements)

    # Execute via Flink SQL client
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_statements)
        sql_file = f.name

    print(f"\n⏱️  Starting benchmark...")

    # Get initial job count
    initial_jobs = []
    try:
        response = requests.get(f"{FLINK_URL}/jobs")
        if response.status_code == 200:
            initial_jobs = [j["id"] for j in response.json().get("jobs", [])]
    except:
        pass

    start_time = time.time()

    try:
        # Run Flink SQL (submits job asynchronously)
        result = subprocess.run(
            ["bash", "scripts/flink_sql.sh", "-f", sql_file],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Find the new job ID
        time.sleep(2)
        new_job_id = None
        try:
            response = requests.get(f"{FLINK_URL}/jobs")
            if response.status_code == 200:
                current_jobs = [j["id"] for j in response.json().get("jobs", [])]
                new_jobs = [j for j in current_jobs if j not in initial_jobs]
                if new_jobs:
                    new_job_id = new_jobs[0]
                    print(f"📋 Job ID: {new_job_id}")
        except:
            pass

        # Wait for job completion
        if new_job_id:
            print(f"⏳ Waiting for job to complete...")
            status, job_time = wait_for_job_completion(new_job_id)
            elapsed_time = time.time() - start_time

            print(f"\n✅ Job {status}!")
            print(f"⏱️  Total time: {elapsed_time:.2f} seconds")
            print(f"⏱️  Job execution time: {job_time:.2f} seconds")
            print(f"📊 Throughput: {n_rows/elapsed_time:.2f} rows/second")
        else:
            elapsed_time = time.time() - start_time
            print(f"\n⚠️  Could not track job, using submission time")
            print(f"⏱️  Submission time: {elapsed_time:.2f} seconds")

        return {
            "parallelism": parallelism,
            "n_rows": n_rows,
            "elapsed_time": elapsed_time,
            "throughput": n_rows / elapsed_time,
            "success": result.returncode == 0,
        }

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        print(f"\n⚠️  Benchmark timed out after {elapsed_time:.2f} seconds")
        return {
            "parallelism": parallelism,
            "n_rows": n_rows,
            "elapsed_time": elapsed_time,
            "throughput": 0,
            "success": False,
        }
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ Benchmark failed: {e}")
        return {
            "parallelism": parallelism,
            "n_rows": n_rows,
            "elapsed_time": elapsed_time,
            "throughput": 0,
            "success": False,
        }


def main():
    """Run benchmarks with different parallelism settings."""
    print("\n" + "=" * 70)
    print("🚀 Flink SQL Parallelism Benchmark")
    print("=" * 70)
    print("\nThis benchmark measures the performance impact of parallelism")
    print("on agmap() UDF execution for sentiment analysis.")
    print("\nTest configuration:")
    print("  - Rows to process: 10")
    print("  - Parallelism levels: 1, 10")
    print("  - UDF: agmap('Sentiment', customer_review)")

    results = []

    # Benchmark with parallelism=1
    result_p1 = run_benchmark(parallelism=1, n_rows=10)
    results.append(result_p1)

    time.sleep(5)  # Wait between benchmarks

    # Benchmark with parallelism=10
    result_p10 = run_benchmark(parallelism=10, n_rows=10)
    results.append(result_p10)

    # Print summary
    print("\n" + "=" * 70)
    print("📊 BENCHMARK RESULTS SUMMARY")
    print("=" * 70)

    for result in results:
        if result["success"]:
            print(f"\nParallelism={result['parallelism']}:")
            print(f"  ⏱️  Time: {result['elapsed_time']:.2f}s")
            print(f"  📈 Throughput: {result['throughput']:.2f} rows/s")

    # Calculate speedup
    if results[0]["success"] and results[1]["success"]:
        speedup = results[0]["elapsed_time"] / results[1]["elapsed_time"]
        print(f"\n🎯 Speedup with parallelism=10: {speedup:.2f}x")
        print(f"   (Expected: ~10x for embarrassingly parallel workloads)")

        if speedup < 2:
            print("\n⚠️  Note: Low speedup suggests bottlenecks:")
            print("   - LLM API rate limits")
            print("   - Network latency")
            print("   - Insufficient TaskManager slots")


if __name__ == "__main__":
    main()

# Made with Bob
