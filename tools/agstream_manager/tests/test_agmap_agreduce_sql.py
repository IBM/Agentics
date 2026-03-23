#!/usr/bin/env python3
"""
Streamlined SQL tests for agmap and agreduce UDFs in Flink.

These tests create a Flink Table Environment, register UDFs,
and execute actual SQL queries to test the UDFs end-to-end.
Reduced to 2 tests per class for faster execution.

Run in Flink container:
    cd /opt/flink
    pytest tests/test_agmap_agreduce_sql.py -v
"""

import json
import os
import sys
from pathlib import Path

import pytest

# Add paths for imports
sys.path.insert(0, "/opt/flink")
sys.path.insert(0, "/opt/flink/udfs")

# Load environment variables
from dotenv import load_dotenv

load_dotenv("/opt/flink/.env")

# Import the UDF modules
import ag_operators
import agreduce

# Import Flink modules
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udtf

pytestmark = pytest.mark.flink


@pytest.fixture(scope="module")
def table_env():
    """Create and configure Flink Table Environment"""
    print("\n🔧 Setting up Flink Table Environment...")
    # Create table environment
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # Register agmap UDF
    t_env.create_temporary_system_function("agmap", ag_operators.agmap)

    # Register agreduce UDAF
    t_env.create_temporary_system_function("agreduce", agreduce.agreduce)

    print("   ✅ UDFs registered")
    return t_env


class TestAGMapSQL:
    """Test agmap UDF through Flink SQL"""

    def test_agmap_sentiment_sql(self, table_env):
        """Test agmap extracting sentiment via SQL"""
        print("\n🧪 Testing agmap sentiment extraction via SQL...")

        # Create and populate test table
        table_env.execute_sql(
            """
            CREATE TEMPORARY TABLE test_reviews (
                review_text STRING
            )
        """
        )

        table_env.execute_sql(
            """
            INSERT INTO test_reviews VALUES
                ('This product is amazing! I love it!'),
                ('Terrible quality, very disappointed.')
        """
        ).wait()

        # Query with agmap
        result = table_env.sql_query(
            """
            SELECT
                review_text,
                T.sentiment
            FROM test_reviews,
            LATERAL TABLE(agmap(review_text, 'sentiment', 'str', 'positive, negative, or neutral')) AS T(sentiment)
        """
        )

        # Collect results
        rows = list(result.execute().collect())

        assert len(rows) == 2
        print(f"   ✅ Processed {len(rows)} reviews")

        # Parse and verify sentiments
        sentiments = []
        for row in rows:
            data = json.loads(row[1])
            sentiments.append(data["sentiment"].lower())
            print(f"      - {row[0][:30]}... → {data['sentiment']}")

        # Should have one positive and one negative
        assert "positive" in sentiments
        assert "negative" in sentiments

    def test_agmap_rating_sql(self, table_env):
        """Test agmap extracting numeric rating via SQL"""
        print("\n🧪 Testing agmap rating extraction via SQL...")

        table_env.execute_sql(
            """
            CREATE TEMPORARY TABLE rating_reviews (
                review_text STRING
            )
        """
        )

        table_env.execute_sql(
            """
            INSERT INTO rating_reviews VALUES
                ('Excellent! 5 stars!'),
                ('Poor quality. 2 stars.')
        """
        ).wait()

        result = table_env.sql_query(
            """
            SELECT
                review_text,
                T.rating
            FROM rating_reviews,
            LATERAL TABLE(agmap(review_text, 'rating', 'float', 'Rating from 1.0 to 5.0')) AS T(rating)
        """
        )

        rows = list(result.execute().collect())

        assert len(rows) == 2
        print(f"   ✅ Processed {len(rows)} reviews")

        # Verify ratings are in valid range
        for row in rows:
            data = json.loads(row[1])
            rating = data["rating"]
            print(f"      - {row[0][:30]}... → {rating}")
            assert isinstance(rating, (int, float))
            assert 1.0 <= rating <= 5.0


class TestAGReduceSQL:
    """Test agreduce UDAF through Flink SQL"""

    def test_agreduce_summary_sql(self, table_env):
        """Test agreduce aggregating reviews via SQL"""
        print("\n🧪 Testing agreduce summary aggregation via SQL...")

        table_env.execute_sql(
            """
            CREATE TEMPORARY TABLE agg_reviews (
                review_text STRING
            )
        """
        )

        table_env.execute_sql(
            """
            INSERT INTO agg_reviews VALUES
                ('Great product! Love it!'),
                ('Good value for money.')
        """
        ).wait()

        # Aggregate with agreduce
        result = table_env.sql_query(
            """
            SELECT
                agreduce(review_text, 'summary', 'str', 'Overall summary of all reviews') as summary
            FROM agg_reviews
        """
        )

        rows = list(result.execute().collect())

        assert len(rows) == 1

        # Parse summary
        data = json.loads(rows[0][0])
        print(f"   ✅ Generated summary: {data['summary'][:100]}...")
        assert "summary" in data
        assert len(data["summary"]) > 0

        # Summary should mention positive aspects
        summary_lower = data["summary"].lower()
        assert any(
            word in summary_lower for word in ["positive", "good", "great", "excellent"]
        )

    def test_agreduce_average_rating_sql(self, table_env):
        """Test agreduce computing average rating via SQL"""
        print("\n🧪 Testing agreduce average rating via SQL...")

        table_env.execute_sql(
            """
            CREATE TEMPORARY TABLE rating_agg_reviews (
                review_text STRING
            )
        """
        )

        table_env.execute_sql(
            """
            INSERT INTO rating_agg_reviews VALUES
                ('5 stars! Amazing!'),
                ('4 stars, very good')
        """
        ).wait()

        result = table_env.sql_query(
            """
            SELECT
                agreduce(review_text, 'avg_rating', 'float', 'Average rating') as avg_rating
            FROM rating_agg_reviews
        """
        )

        rows = list(result.execute().collect())

        assert len(rows) == 1

        data = json.loads(rows[0][0])
        print(f"   ✅ Average rating: {data['avg_rating']}")
        assert "avg_rating" in data
        assert isinstance(data["avg_rating"], (int, float))
        # Average should be around 4.0-5.0
        assert 4.0 <= data["avg_rating"] <= 5.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

# Made with Bob
