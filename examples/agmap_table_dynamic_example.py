#!/usr/bin/env python3
"""
AGmap Table Dynamic Example

Demonstrates the agmap_table_dynamic UDF that generates types on-the-fly
and performs semantic transductions with dynamic column generation.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pydantic import BaseModel

from agentics.core import AGStreamSQL


class Review(BaseModel):
    """A customer review"""

    text: str
    product_id: str


def main():
    print("=" * 70)
    print("AGmap Table Dynamic Example")
    print("=" * 70)
    print()

    # Create sample reviews
    reviews = [
        Review(
            text="This coffee maker is amazing! Best purchase ever. 5 stars!",
            product_id="CM-001",
        ),
        Review(
            text="The laptop is okay, nothing special. Works fine but overpriced.",
            product_id="LP-002",
        ),
        Review(
            text="Terrible headphones. Broke after one week. Very disappointed.",
            product_id="HP-003",
        ),
        Review(
            text="Love these running shoes! Super comfortable and great quality.",
            product_id="RS-004",
        ),
    ]

    # Create AGStreamSQL and produce reviews
    print("📝 Creating AGStreamSQL for reviews...")
    reviews_stream = AGStreamSQL(
        atype=Review,
        topic="product_reviews_dynamic",
        kafka_server="localhost:9092",
        schema_registry_url="http://localhost:8081",
        auto_create_topic=True,
    )

    print("📤 Producing reviews...")
    reviews_stream.produce(reviews)
    print(f"✅ Produced {len(reviews)} reviews")
    print()

    print("=" * 70)
    print("✅ Data produced successfully!")
    print("=" * 70)
    print()

    print("🔍 Now you can use agmap_table_dynamic in Flink SQL:")
    print()
    print("Example 1: Extract sentiment (string)")
    print("-" * 70)
    print(
        """
SELECT
    text,
    T.sentiment
FROM product_reviews_dynamic,
LATERAL TABLE(agmap_table_dynamic('sentiment', text, 'str',
              'The sentiment: positive, negative, or neutral')) AS T(sentiment)
LIMIT 5;
"""
    )

    print()
    print("Example 2: Extract rating (float)")
    print("-" * 70)
    print(
        """
SELECT
    text,
    T.rating
FROM product_reviews_dynamic,
LATERAL TABLE(agmap_table_dynamic('rating', text, 'float',
              'Numeric rating from 1.0 to 5.0 based on the review')) AS T(rating)
LIMIT 5;
"""
    )

    print()
    print("Example 3: Extract category (string)")
    print("-" * 70)
    print(
        """
SELECT
    text,
    T.category
FROM product_reviews_dynamic,
LATERAL TABLE(agmap_table_dynamic('category', text, 'str',
              'Product category: electronics, appliances, clothing, or sports')) AS T(category)
LIMIT 5;
"""
    )

    print()
    print("Example 4: Check if urgent (boolean)")
    print("-" * 70)
    print(
        """
SELECT
    text,
    T.is_urgent
FROM product_reviews_dynamic,
LATERAL TABLE(agmap_table_dynamic('is_urgent', text, 'bool',
              'True if the review indicates an urgent issue requiring attention')) AS T(is_urgent)
WHERE T.is_urgent = 'True'
LIMIT 5;
"""
    )

    print()
    print("Example 5: Multiple dynamic columns")
    print("-" * 70)
    print(
        """
SELECT
    text,
    T1.sentiment,
    T2.rating,
    T3.category
FROM product_reviews_dynamic,
LATERAL TABLE(agmap_table_dynamic('sentiment', text, 'str')) AS T1(sentiment),
LATERAL TABLE(agmap_table_dynamic('rating', text, 'float')) AS T2(rating),
LATERAL TABLE(agmap_table_dynamic('category', text, 'str')) AS T3(category)
LIMIT 5;
"""
    )

    print()
    print("Example 6: Aggregation with dynamic fields")
    print("-" * 70)
    print(
        """
SELECT
    T.sentiment,
    COUNT(*) as count,
    AVG(CAST(T2.rating AS DOUBLE)) as avg_rating
FROM product_reviews_dynamic,
LATERAL TABLE(agmap_table_dynamic('sentiment', text, 'str')) AS T(sentiment),
LATERAL TABLE(agmap_table_dynamic('rating', text, 'float')) AS T2(rating)
GROUP BY T.sentiment
ORDER BY count DESC;
"""
    )

    print()
    print("=" * 70)
    print("📚 Key Features:")
    print("  • No schema registry required - types generated on-the-fly")
    print("  • Supports str, int, float, bool types")
    print("  • Optional descriptions guide LLM extraction")
    print("  • Perfect for ad-hoc analysis and exploration")
    print("=" * 70)


if __name__ == "__main__":
    main()

# Made with Bob
