#!/usr/bin/env python3
"""
Comprehensive test script for all UDFs in semantic_operators.py
Tests each UDF individually to identify working vs broken ones
"""

import json
import os
import sys
from pathlib import Path

# Add agentics to path
agentics_root = Path(__file__).parent.parent.parent.parent / "src"
sys.path.insert(0, str(agentics_root))

# Load environment variables
from dotenv import load_dotenv

for env_path in ["../../../.env", ".env", "/opt/flink/.env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}")
        break

# Import the UDF functions
from semantic_operators import (
    analyze_and_categorize,
    analyze_row_sentiment,
    analyze_sentiment,
    answer_question,
    categorize_text,
    extract_category,
    extract_confidence,
    extract_entities,
    extract_sentiment_label,
    extract_summary,
    summarize_text,
)


class UDFTester:
    """Test harness for UDFs"""

    def __init__(self):
        self.passed = []
        self.failed = []
        self.results = {}

    def test_udf(self, name, func, test_input, expected_keys=None):
        """Test a single UDF"""
        print(f"\n{'='*70}")
        print(f"Testing: {name}")
        print(f"{'='*70}")
        print(
            f"Input: {test_input[:100]}..."
            if len(str(test_input)) > 100
            else f"Input: {test_input}"
        )

        try:
            result = func(test_input)
            print(
                f"✓ Result: {result[:200]}..."
                if len(str(result)) > 200
                else f"✓ Result: {result}"
            )

            # Validate JSON structure if expected_keys provided
            if expected_keys and result:
                try:
                    data = json.loads(result)
                    missing_keys = [k for k in expected_keys if k not in data]
                    if missing_keys:
                        print(f"⚠ Warning: Missing keys: {missing_keys}")
                    else:
                        print(f"✓ All expected keys present: {expected_keys}")
                except json.JSONDecodeError:
                    print(f"⚠ Warning: Result is not valid JSON")

            self.passed.append(name)
            self.results[name] = {"status": "PASSED", "result": str(result)[:200]}
            return True

        except Exception as e:
            print(f"✗ Error: {str(e)}")
            import traceback

            traceback.print_exc()
            self.failed.append(name)
            self.results[name] = {"status": "FAILED", "error": str(e)}
            return False

    def print_summary(self):
        """Print test summary"""
        print(f"\n{'='*70}")
        print("TEST SUMMARY")
        print(f"{'='*70}")
        print(f"✓ Passed: {len(self.passed)}")
        print(f"✗ Failed: {len(self.failed)}")
        print(f"Total: {len(self.passed) + len(self.failed)}")

        if self.passed:
            print(f"\n✓ Passed UDFs:")
            for name in self.passed:
                print(f"  - {name}")

        if self.failed:
            print(f"\n✗ Failed UDFs:")
            for name in self.failed:
                print(f"  - {name}")
                if "error" in self.results[name]:
                    print(f"    Error: {self.results[name]['error']}")

        print(f"\n{'='*70}")

        # Recommendations
        print("\nRECOMMENDATIONS:")
        if self.failed:
            print("❌ Consider removing these failed UDFs:")
            for name in self.failed:
                print(f"  - {name}")
        else:
            print("✓ All UDFs are working!")


def main():
    """Run all UDF tests"""
    print("\n" + "=" * 70)
    print("COMPREHENSIVE UDF TEST SUITE")
    print("=" * 70)

    # Check environment
    selected_llm = os.getenv("SELECTED_LLM", "not set")
    print(f"\n🤖 SELECTED_LLM: {selected_llm}")

    if selected_llm == "watsonx":
        api_key = os.getenv("WATSONX_APIKEY", "not set")
        print(f"🔑 WATSONX_APIKEY: {'✓ set' if api_key != 'not set' else '✗ not set'}")
    elif selected_llm == "openai":
        api_key = os.getenv("OPENAI_API_KEY", "not set")
        print(f"🔑 OPENAI_API_KEY: {'✓ set' if api_key != 'not set' else '✗ not set'}")

    tester = UDFTester()

    # Test 1: analyze_sentiment
    tester.test_udf(
        "analyze_sentiment",
        analyze_sentiment,
        "This product is absolutely amazing! I love it!",
        expected_keys=["label", "confidence", "reasoning"],
    )

    # Test 2: categorize_text
    tester.test_udf(
        "categorize_text",
        categorize_text,
        "Apple announces new iPhone with advanced AI features and improved camera",
        expected_keys=["category", "confidence"],
    )

    # Test 3: summarize_text
    long_text = """
    Artificial intelligence has made remarkable progress in recent years.
    Machine learning models can now perform complex tasks like image recognition,
    natural language processing, and even creative writing. Deep learning, a subset
    of machine learning, uses neural networks with multiple layers to learn
    hierarchical representations of data. This has led to breakthroughs in various
    fields including healthcare, finance, and autonomous vehicles.
    """
    tester.test_udf(
        "summarize_text",
        summarize_text,
        long_text,
        expected_keys=["summary", "key_points", "word_count"],
    )

    # Test 4: extract_entities
    tester.test_udf(
        "extract_entities",
        extract_entities,
        "Apple CEO Tim Cook announced the new iPhone at the Steve Jobs Theater in Cupertino, California",
        expected_keys=["entities", "entity_types"],
    )

    # Test 5: answer_question
    tester.test_udf(
        "answer_question",
        answer_question,
        "What is the capital of France?",
        expected_keys=["answer", "confidence"],
    )

    # Test 6: analyze_and_categorize (composite)
    tester.test_udf(
        "analyze_and_categorize",
        analyze_and_categorize,
        "I'm so excited about the new technology features!",
        expected_keys=["sentiment", "category"],
    )

    # Test 7: analyze_row_sentiment
    row_data = {
        "title": "Great Product",
        "content": "This is an excellent product that exceeded my expectations!",
        "rating": 5,
    }
    tester.test_udf(
        "analyze_row_sentiment",
        analyze_row_sentiment,
        json.dumps(row_data),
        expected_keys=["label", "confidence", "reasoning"],
    )

    # Test 8-11: Utility functions (test with sample JSON)
    sample_sentiment = (
        '{"label": "POSITIVE", "confidence": 0.95, "reasoning": "Very positive"}'
    )
    sample_category = '{"category": "Technology", "confidence": 0.88}'
    sample_summary = (
        '{"summary": "AI is advancing", "key_points": ["ML", "DL"], "word_count": 3}'
    )

    tester.test_udf(
        "extract_sentiment_label", extract_sentiment_label, sample_sentiment
    )

    tester.test_udf("extract_confidence", extract_confidence, sample_sentiment)

    tester.test_udf("extract_category", extract_category, sample_category)

    tester.test_udf("extract_summary", extract_summary, sample_summary)

    # Print summary
    tester.print_summary()

    # Return exit code
    return 0 if not tester.failed else 1


if __name__ == "__main__":
    sys.exit(main())

# Made with Bob
