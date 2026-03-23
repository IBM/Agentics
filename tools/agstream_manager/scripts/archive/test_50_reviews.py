#!/usr/bin/env python3
"""
Test processing 50 reviews with Flink SQL and measure performance
"""
import subprocess
import sys
import time

# SQL query to process reviews with sentiment analysis
sql_query = """
SELECT
    review_id,
    text,
    agmap_row(text, 'Analyze the sentiment of this product review. Return only one word: positive, negative, or neutral.') as sentiment
FROM product_reviews;
"""


def run_flink_sql_query(query):
    """Execute Flink SQL query and measure time"""
    print("=" * 80)
    print("Starting Flink SQL Query Execution")
    print("=" * 80)
    print(f"Query:\n{query}")
    print("=" * 80)

    # Write query to temp file
    with open("/tmp/test_query.sql", "w") as f:
        f.write(query)

    start_time = time.time()

    # Execute query using flink_sql.sh
    cmd = [
        "bash",
        "tools/agstream_manager/scripts/flink_sql.sh",
        "-f",
        "/tmp/test_query.sql",
    ]

    print("\nExecuting query...")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 80)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300  # 5 minute timeout
        )

        elapsed = time.time() - start_time

        print("\n" + "=" * 80)
        print("QUERY OUTPUT:")
        print("=" * 80)
        print(result.stdout)

        if result.stderr:
            print("\n" + "=" * 80)
            print("STDERR:")
            print("=" * 80)
            print(result.stderr)

        print("\n" + "=" * 80)
        print("PERFORMANCE METRICS")
        print("=" * 80)
        print(f"Total execution time: {elapsed:.2f} seconds")

        # Count rows in output
        output_lines = result.stdout.strip().split("\n")
        # Filter out header lines and empty lines
        data_lines = [
            line
            for line in output_lines
            if line.strip()
            and not line.startswith("+")
            and not line.startswith("|")
            or "|" in line
            and "review_" in line
        ]
        row_count = len(data_lines)

        if row_count > 0:
            print(f"Rows processed: {row_count}")
            print(f"Processing rate: {row_count/elapsed:.2f} rows/second")
            print(f"Average time per row: {elapsed/row_count:.2f} seconds")

        print("=" * 80)

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start_time
        print(f"\nQuery timed out after {elapsed:.2f} seconds")
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\nError executing query after {elapsed:.2f} seconds: {e}")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("FLINK SQL PERFORMANCE TEST - 50 REVIEWS")
    print("=" * 80)
    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    success = run_flink_sql_query(sql_query)

    print("\n" + "=" * 80)
    print(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Test {'PASSED' if success else 'FAILED'}")
    print("=" * 80)

    sys.exit(0 if success else 1)

# Made with Bob
