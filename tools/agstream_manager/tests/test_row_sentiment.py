#!/usr/bin/env python3
"""
Test script for row-level sentiment analysis UDF
"""

import json
import os
import sys

# Add agentics to path
sys.path.insert(0, "/opt/flink")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

from pydantic import BaseModel

# Import the UDF functions
from semantic_operators import Sentiment, analyze_row_sentiment, run_row_transduction


def test_simple_row():
    """Test with a simple row containing title and content"""
    print("\n=== Test 1: Simple Row ===")

    row_data = {
        "title": "Amazing Product!",
        "content": "I absolutely love this product. It exceeded all my expectations and works perfectly.",
        "author": "John Doe",
    }

    row_json = json.dumps(row_data)
    result = analyze_row_sentiment(row_json)

    print(f"Input: {row_json}")
    print(f"Result: {result}")

    result_dict = json.loads(result)
    print(f"Sentiment: {result_dict['label']}")
    print(f"Confidence: {result_dict['confidence']}")
    print(f"Reasoning: {result_dict.get('reasoning', 'N/A')}")


def test_negative_sentiment():
    """Test with negative sentiment"""
    print("\n=== Test 2: Negative Sentiment ===")

    row_data = {
        "id": 123,
        "title": "Terrible Experience",
        "content": "This was the worst purchase I've ever made. Complete waste of money.",
        "rating": 1,
    }

    row_json = json.dumps(row_data)
    result = analyze_row_sentiment(row_json)

    print(f"Input: {row_json}")
    print(f"Result: {result}")

    result_dict = json.loads(result)
    print(f"Sentiment: {result_dict['label']}")
    print(f"Confidence: {result_dict['confidence']}")


def test_neutral_sentiment():
    """Test with neutral sentiment"""
    print("\n=== Test 3: Neutral Sentiment ===")

    row_data = {
        "title": "Product Review",
        "content": "The product arrived on time. It has some good features and some areas for improvement.",
        "timestamp": "2024-01-15",
    }

    row_json = json.dumps(row_data)
    result = analyze_row_sentiment(row_json)

    print(f"Input: {row_json}")
    print(f"Result: {result}")

    result_dict = json.loads(result)
    print(f"Sentiment: {result_dict['label']}")
    print(f"Confidence: {result_dict['confidence']}")


def test_complex_row():
    """Test with a complex row containing multiple fields"""
    print("\n=== Test 4: Complex Row with Multiple Fields ===")

    row_data = {
        "id": 456,
        "title": "Great Service!",
        "content": "The customer service team was incredibly helpful and resolved my issue quickly.",
        "author": "Jane Smith",
        "category": "Customer Service",
        "rating": 5,
        "helpful_votes": 42,
        "timestamp": "2024-01-20T10:30:00Z",
    }

    row_json = json.dumps(row_data)
    result = analyze_row_sentiment(row_json)

    print(f"Input: {json.dumps(row_data, indent=2)}")
    print(f"Result: {json.dumps(json.loads(result), indent=2)}")


def test_empty_input():
    """Test with empty input"""
    print("\n=== Test 5: Empty Input ===")

    result = analyze_row_sentiment("")
    print(f"Result: {result}")

    result_dict = json.loads(result)
    print(f"Sentiment: {result_dict['label']}")


def test_direct_transduction():
    """Test the underlying run_row_transduction function directly"""
    print("\n=== Test 6: Direct Transduction ===")

    class ReviewInput(BaseModel):
        title: str
        content: str
        rating: int

        class Config:
            extra = "allow"

    row_data = {
        "title": "Excellent Product",
        "content": "This product is fantastic and I highly recommend it!",
        "rating": 5,
    }

    row_json = json.dumps(row_data)
    result = run_row_transduction(row_json, ReviewInput, Sentiment)

    print(f"Input: {row_json}")
    print(f"Result: {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Row-Level Sentiment Analysis UDF")
    print("=" * 60)

    try:
        test_simple_row()
        test_negative_sentiment()
        test_neutral_sentiment()
        test_complex_row()
        test_empty_input()
        test_direct_transduction()

        print("\n" + "=" * 60)
        print("All tests completed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

# Made with Bob
