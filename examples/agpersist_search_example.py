"""
AGPersist Search Example

Demonstrates how to use persistent vector search indexes with AGStream.

This example shows:
1. Building a persistent index from data
2. Searching the persisted index
3. Reusing the index across multiple queries
4. Performance benefits of persistent indexes
"""

import os
import time
from pathlib import Path

from agentics.core.streaming import AGStreamSQL

# Set custom index storage path (optional)
os.environ["AGSTREAM_INDEX_PATH"] = str(Path.home() / "agstream_indexes")


def main():
    """Main example function."""

    print("=" * 60)
    print("AGPersist Search Example")
    print("=" * 60)

    # Initialize AGStream SQL
    agstream = AGStreamSQL()

    # Register the persistent search functions
    print("\n1. Registering persistent search functions...")
    agstream.execute_sql(
        """
        CREATE TEMPORARY FUNCTION IF NOT EXISTS build_search_index
        AS 'agpersist_search.build_search_index'
        LANGUAGE PYTHON
    """
    )

    agstream.execute_sql(
        """
        CREATE TEMPORARY FUNCTION IF NOT EXISTS search_persisted_index
        AS 'agpersist_search.search_persisted_index'
        LANGUAGE PYTHON
    """
    )
    print("✓ Functions registered")

    # Example 1: Build a persistent index
    print("\n2. Building persistent index from sample data...")
    print("   (This may take a moment on first run to download the model)")

    # Create sample data table
    agstream.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS sample_reviews (
            review_id INT,
            review_text STRING,
            rating INT
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.review_id.kind' = 'sequence',
            'fields.review_id.start' = '1',
            'fields.review_id.end' = '100'
        )
    """
    )

    # Build the index
    start_time = time.time()
    result = agstream.execute_sql(
        """
        SELECT build_search_index(review_text, 'sample_reviews_index') as status
        FROM sample_reviews
        LIMIT 50
    """
    )
    build_time = time.time() - start_time

    print(f"✓ Index built in {build_time:.2f} seconds")
    print(f"   Result: {result}")

    # Example 2: Search the persisted index
    print("\n3. Searching the persisted index...")

    queries = [
        "excellent product quality",
        "fast shipping",
        "customer service",
        "value for money",
    ]

    for query in queries:
        print(f"\n   Query: '{query}'")
        start_time = time.time()

        results = agstream.execute_sql(
            f"""
            SELECT T.text, T.score
            FROM LATERAL TABLE(search_persisted_index('sample_reviews_index', '{query}', 5))
            AS T(text, index, score)
            WHERE T.score > 0.5
            ORDER BY T.score DESC
        """
        )

        search_time = time.time() - start_time
        print(f"   Search time: {search_time*1000:.2f}ms")
        print(f"   Results: {len(results)} matches")

        for i, (text, score) in enumerate(results[:3], 1):
            print(f"      {i}. [{score:.3f}] {text[:60]}...")

    # Example 3: Compare with non-persistent search
    print("\n4. Performance comparison: Persistent vs Non-persistent")
    print("   (Searching same data multiple times)")

    # Persistent search (index already built)
    persistent_times = []
    for i in range(5):
        start_time = time.time()
        agstream.execute_sql(
            """
            SELECT T.text, T.score
            FROM LATERAL TABLE(search_persisted_index('sample_reviews_index', 'quality', 10))
            AS T(text, index, score)
        """
        )
        persistent_times.append(time.time() - start_time)

    avg_persistent = sum(persistent_times) / len(persistent_times)
    print(f"   Persistent search avg: {avg_persistent*1000:.2f}ms")

    # Non-persistent search (rebuilds index each time)
    print(
        "   Non-persistent search: Would rebuild index each time (~{build_time:.2f}s)"
    )
    print(f"   Speedup: {build_time/avg_persistent:.1f}x faster!")

    # Example 4: Multiple indexes
    print("\n5. Working with multiple indexes...")

    # Build category-specific indexes
    categories = ["electronics", "clothing", "books"]

    for category in categories:
        print(f"   Building index for: {category}")
        agstream.execute_sql(
            f"""
            SELECT build_search_index(review_text, '{category}_reviews_index') as status
            FROM sample_reviews
            WHERE category = '{category}'
            LIMIT 20
        """
        )

    print("   ✓ Multiple indexes created")

    # Search across all indexes
    print("\n   Searching across all category indexes:")
    query = "great quality"

    for category in categories:
        results = agstream.execute_sql(
            f"""
            SELECT '{category}' as category, T.text, T.score
            FROM LATERAL TABLE(search_persisted_index('{category}_reviews_index', '{query}', 3))
            AS T(text, index, score)
            ORDER BY T.score DESC
        """
        )

        if results:
            print(
                f"      {category}: {len(results)} results (top score: {results[0][2]:.3f})"
            )

    # Example 5: Index management
    print("\n6. Index management:")
    index_path = Path(os.environ.get("AGSTREAM_INDEX_PATH", "/tmp/agstream_indexes"))

    if index_path.exists():
        indexes = [d.name for d in index_path.iterdir() if d.is_dir()]
        print(f"   Available indexes: {len(indexes)}")
        for idx_name in indexes[:5]:  # Show first 5
            print(f"      - {idx_name}")

        # Show storage size
        total_size = sum(f.stat().st_size for f in index_path.rglob("*") if f.is_file())
        print(f"   Total storage used: {total_size / 1024 / 1024:.2f} MB")

    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)

    # Cleanup instructions
    print("\nTo clean up indexes:")
    print(f"  rm -rf {index_path}")
    print("\nTo rebuild an index:")
    print("  Just run build_search_index() again with the same index name")


def simple_example():
    """Simple standalone example."""

    print("\nSimple Example: Build and Search")
    print("-" * 40)

    agstream = AGStreamSQL()

    # Register functions
    agstream.execute_sql(
        """
        CREATE TEMPORARY FUNCTION IF NOT EXISTS build_search_index
        AS 'agpersist_search.build_search_index' LANGUAGE PYTHON
    """
    )
    agstream.execute_sql(
        """
        CREATE TEMPORARY FUNCTION IF NOT EXISTS search_persisted_index
        AS 'agpersist_search.search_persisted_index' LANGUAGE PYTHON
    """
    )

    # Build index
    print("Building index...")
    agstream.execute_sql(
        """
        SELECT build_search_index(customer_review, 'my_index') as status
        FROM pr
    """
    )

    # Search index
    print("Searching index...")
    results = agstream.execute_sql(
        """
        SELECT T.text, T.score
        FROM LATERAL TABLE(search_persisted_index('my_index', 'great service', 5))
        AS T(text, index, score)
    """
    )

    print(f"Found {len(results)} results")
    for text, score in results:
        print(f"  [{score:.3f}] {text[:50]}...")


if __name__ == "__main__":
    # Run the main example
    try:
        main()
    except Exception as e:
        print(f"\nError: {e}")
        print("\nNote: This example requires:")
        print("  1. Flink SQL environment configured")
        print("  2. UDFs registered in Flink")

    # Uncomment to run simple example
    # simple_example()

# Made with Bob
