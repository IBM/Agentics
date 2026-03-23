#!/usr/bin/env python3
"""
Streamlined tests for agmap and agreduce UDFs in Flink container.

These tests verify core functionality with minimal test cases (2 per class).

Run in Flink container:
    cd /opt/flink
    pytest tests/test_agmap_agreduce.py -v
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

# Import the UDF functions
from ag_operators import agmap
from agreduce import AgReduceFunction

pytestmark = pytest.mark.flink


class TestAGMapDynamic:
    """Test agmap in dynamic mode (on-the-fly type generation)"""

    def test_agmap_sentiment_string(self):
        """Test agmap extracting sentiment as string"""
        input_text = "This product is absolutely amazing! I love it so much."
        print(f"\n🧪 Testing sentiment extraction...")
        print(f"   Input: {input_text}")

        results = list(
            agmap(input_text, "sentiment", "str", "positive, negative, or neutral")
        )

        assert len(results) > 0
        result = results[0]
        assert isinstance(result, tuple)
        assert len(result) == 1

        # Parse JSON result
        data = json.loads(result[0])
        print(f"   ✅ Result: {data}")
        assert "sentiment" in data
        assert data["sentiment"].lower() in ["positive", "negative", "neutral"]

    def test_agmap_rating_float(self):
        """Test agmap extracting rating as float"""
        input_text = "Great product! Would rate it 4.5 out of 5 stars."
        print(f"\n🧪 Testing rating extraction...")
        print(f"   Input: {input_text}")

        results = list(agmap(input_text, "rating", "float", "Rating from 1.0 to 5.0"))

        assert len(results) > 0
        result = results[0]

        data = json.loads(result[0])
        print(f"   ✅ Result: {data}")
        assert "rating" in data
        assert isinstance(data["rating"], (int, float))
        assert 1.0 <= data["rating"] <= 5.0


class TestAGMapRegistry:
    """Test agmap in registry mode (using Schema Registry)"""

    def test_agmap_registry_mode_no_schema(self):
        """Test agmap registry mode when schema doesn't exist"""
        input_text = "Test review"
        print(f"\n🧪 Testing registry mode with non-existent schema...")

        results = list(agmap(input_text, "NonExistentSchema", "registry"))

        assert len(results) > 0
        result = results[0]

        # Should return error when schema not found
        data = json.loads(result[0])
        print(f"   ✅ Result: {data}")
        assert "error" in data
        assert data["error"] == "schema_not_found"

    def test_agmap_empty_input(self):
        """Test agmap with empty input"""
        print(f"\n🧪 Testing empty input handling...")
        results = list(agmap("", "sentiment", "str"))

        assert len(results) > 0
        result = results[0]
        print(f"   ✅ Result: {result[0]}")
        assert result[0] == "error"


class TestAGReduce:
    """Test agreduce aggregation functionality"""

    def test_agreduce_basic_accumulation(self):
        """Test agreduce accumulates multiple rows"""
        print(f"\n🧪 Testing agreduce accumulation...")
        reducer = AgReduceFunction()

        # Create accumulator
        acc = reducer.create_accumulator()
        assert acc == []

        # Accumulate multiple reviews
        reviews = ["Great product! Love it.", "Not bad, could be better."]

        for review in reviews:
            reducer.accumulate(acc, review, "summary", "str", "Overall summary")

        assert len(acc) == 2
        assert all(isinstance(item, str) for item in acc)
        print(f"   ✅ Accumulated {len(acc)} items")

    def test_agreduce_get_value(self):
        """Test agreduce produces final aggregated result"""
        print(f"\n🧪 Testing agreduce final result...")
        reducer = AgReduceFunction()

        acc = reducer.create_accumulator()

        # Accumulate reviews
        reviews = ["Amazing product! 5 stars!", "Good value for money."]

        for review in reviews:
            reducer.accumulate(
                acc, review, "summary", "str", "Summarize overall sentiment"
            )

        # Get final result
        result = reducer.get_value(acc)

        assert result is not None
        assert isinstance(result, str)

        # Parse JSON result
        data = json.loads(result)
        print(f"   ✅ Result: {data}")
        assert "summary" in data
        assert len(data["summary"]) > 0


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_agmap_special_characters(self):
        """Test agmap with special characters"""
        input_text = "Product is 👍! Love it ❤️ 100%!!!"
        print(f"\n🧪 Testing special characters...")
        print(f"   Input: {input_text}")

        results = list(agmap(input_text, "sentiment", "str"))

        assert len(results) > 0
        result = results[0]

        data = json.loads(result[0])
        print(f"   ✅ Result: {data}")
        assert "sentiment" in data
        assert data["sentiment"].lower() == "positive"

    def test_agreduce_mixed_sentiments(self):
        """Test agreduce with mixed positive and negative reviews"""
        print(f"\n🧪 Testing agreduce with mixed sentiments...")
        reducer = AgReduceFunction()

        acc = reducer.create_accumulator()

        reviews = [
            "Absolutely terrible! Worst purchase ever.",
            "Amazing! Best product I've bought.",
        ]

        for review in reviews:
            reducer.accumulate(
                acc, review, "summary", "str", "Overall sentiment summary"
            )

        result = reducer.get_value(acc)
        data = json.loads(result)

        print(f"   ✅ Result: {data}")
        assert "summary" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

# Made with Bob
