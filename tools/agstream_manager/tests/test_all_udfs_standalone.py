#!/usr/bin/env python3
"""
Standalone test for UDF logic (without PyFlink decorators)
Tests the actual transduction logic that the UDFs use
"""

import json
import os
import sys
import warnings
from pathlib import Path

# Suppress the litellm async cleanup warning
warnings.filterwarnings("ignore", message=".*close_litellm_async_clients.*")
warnings.filterwarnings(
    "ignore", category=RuntimeWarning, message=".*coroutine.*was never awaited.*"
)

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

# Import the underlying logic functions (not the decorated UDFs)
from semantic_operators import (
    Answer,
    Category,
    Entity,
    Question,
    Sentiment,
    Summary,
    TextInput,
    run_transduction,
)


class UDFLogicTester:
    """Test harness for UDF logic"""

    def __init__(self):
        self.passed = []
        self.failed = []
        self.results = {}

    def test_transduction(
        self, name, input_text, input_model, output_model, expected_keys=None
    ):
        """Test a transduction"""
        print(f"\n{'='*70}")
        print(f"Testing: {name}")
        print(f"{'='*70}")
        print(
            f"Input: {input_text[:100]}..."
            if len(str(input_text)) > 100
            else f"Input: {input_text}"
        )

        try:
            result = run_transduction(input_text, input_model, output_model)
            print(f"✓ Result: {json.dumps(result, indent=2)}")

            # Validate expected keys
            if expected_keys:
                missing_keys = [k for k in expected_keys if k not in result]
                if missing_keys:
                    print(f"⚠ Warning: Missing keys: {missing_keys}")
                else:
                    print(f"✓ All expected keys present: {expected_keys}")

            self.passed.append(name)
            self.results[name] = {"status": "PASSED", "result": result}
            return True

        except Exception as e:
            print(f"✗ Error: {str(e)}")
            import traceback

            traceback.print_exc()
            self.failed.append(name)
            self.results[name] = {"status": "FAILED", "error": str(e)}
            return False

    def test_json_extraction(self, name, func, test_input):
        """Test JSON extraction utility"""
        print(f"\n{'='*70}")
        print(f"Testing: {name}")
        print(f"{'='*70}")
        print(f"Input: {test_input}")

        try:
            result = func(test_input)
            print(f"✓ Result: {result}")

            self.passed.append(name)
            self.results[name] = {"status": "PASSED", "result": result}
            return True

        except Exception as e:
            print(f"✗ Error: {str(e)}")
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
            print(f"\n✓ Working UDFs (keep these):")
            for name in self.passed:
                print(f"  - {name}")

        if self.failed:
            print(f"\n✗ Broken UDFs (consider removing):")
            for name in self.failed:
                print(f"  - {name}")
                if "error" in self.results[name]:
                    error_msg = self.results[name]["error"]
                    print(f"    Error: {error_msg[:100]}...")

        print(f"\n{'='*70}")


def main():
    """Run all UDF logic tests"""
    print("\n" + "=" * 70)
    print("COMPREHENSIVE UDF LOGIC TEST SUITE")
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

    tester = UDFLogicTester()

    # Test 1: Sentiment Analysis
    tester.test_transduction(
        "analyze_sentiment",
        "This product is absolutely amazing! I love it!",
        TextInput,
        Sentiment,
        expected_keys=["label", "confidence", "reasoning"],
    )

    # Test 2: Text Categorization
    tester.test_transduction(
        "categorize_text",
        "Apple announces new iPhone with advanced AI features and improved camera",
        TextInput,
        Category,
        expected_keys=["category", "confidence"],
    )

    # Test 3: Text Summarization
    long_text = """
    Artificial intelligence has made remarkable progress in recent years.
    Machine learning models can now perform complex tasks like image recognition,
    natural language processing, and even creative writing. Deep learning, a subset
    of machine learning, uses neural networks with multiple layers to learn
    hierarchical representations of data. This has led to breakthroughs in various
    fields including healthcare, finance, and autonomous vehicles.
    """
    tester.test_transduction(
        "summarize_text",
        long_text,
        TextInput,
        Summary,
        expected_keys=["summary", "key_points", "word_count"],
    )

    # Test 4: Entity Extraction
    tester.test_transduction(
        "extract_entities",
        "Apple CEO Tim Cook announced the new iPhone at the Steve Jobs Theater in Cupertino, California",
        TextInput,
        Entity,
        expected_keys=["entities", "entity_types"],
    )

    # Test 5: Question Answering (special case - different input model)
    print(f"\n{'='*70}")
    print(f"Testing: answer_question")
    print(f"{'='*70}")
    question_text = "What is the capital of France?"
    print(f"Input: {question_text}")

    try:
        import asyncio

        from semantic_operators import Answer, Question

        question_obj = Question(question=question_text)
        answer_fn = Answer << Question

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(answer_fn(question_obj))
            if hasattr(result, "value"):
                result = result.value
            result_dict = (
                result.model_dump() if hasattr(result, "model_dump") else result
            )
            print(f"✓ Result: {json.dumps(result_dict, indent=2)}")
            tester.passed.append("answer_question")
            tester.results["answer_question"] = {
                "status": "PASSED",
                "result": result_dict,
            }
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except:
                pass
            loop.close()
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback

        traceback.print_exc()
        tester.failed.append("answer_question")
        tester.results["answer_question"] = {"status": "FAILED", "error": str(e)}

    # Test 6: Composite (analyze_and_categorize)
    print(f"\n{'='*70}")
    print(f"Testing: analyze_and_categorize (composite)")
    print(f"{'='*70}")
    text = "I'm so excited about the new technology features!"
    print(f"Input: {text}")

    try:
        sentiment = run_transduction(text, TextInput, Sentiment)
        category = run_transduction(text, TextInput, Category)
        result = {"sentiment": sentiment, "category": category}
        print(f"✓ Result: {json.dumps(result, indent=2)}")
        tester.passed.append("analyze_and_categorize")
        tester.results["analyze_and_categorize"] = {
            "status": "PASSED",
            "result": result,
        }
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback

        traceback.print_exc()
        tester.failed.append("analyze_and_categorize")
        tester.results["analyze_and_categorize"] = {"status": "FAILED", "error": str(e)}

    # Test 7: Row-level sentiment (using AG directly like the UDF does)
    print(f"\n{'='*70}")
    print(f"Testing: analyze_row_sentiment (AG-based)")
    print(f"{'='*70}")

    row_data = {
        "title": "Great Product",
        "content": "This is an excellent product that exceeded my expectations!",
        "rating": 5,
    }
    row_json = json.dumps(row_data)
    print(f"Input: {row_json}")

    try:
        import asyncio

        from agentics import AG

        ag = AG(atype=Sentiment)

        # Run transduction using AG
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(ag << row_json)

            # Handle result - could be list or single object
            if isinstance(result, list) and len(result) > 0:
                sentiment_obj = result[0]
            else:
                sentiment_obj = result

            # Extract sentiment data - handle different result types
            if hasattr(sentiment_obj, "model_dump"):
                result_dict = sentiment_obj.model_dump()
            elif hasattr(sentiment_obj, "label"):
                result_dict = {
                    "label": str(sentiment_obj.label),
                    "confidence": float(getattr(sentiment_obj, "confidence", 0.0)),
                    "reasoning": str(getattr(sentiment_obj, "reasoning", "")),
                }
            else:
                # Fallback - convert to string
                result_dict = {
                    "label": str(sentiment_obj),
                    "confidence": 0.0,
                    "reasoning": "AG result",
                }

            print(f"✓ Result: {json.dumps(result_dict, indent=2)}")
            tester.passed.append("analyze_row_sentiment")
            tester.results["analyze_row_sentiment"] = {
                "status": "PASSED",
                "result": result_dict,
            }
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except:
                pass
            loop.close()

    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback

        traceback.print_exc()
        tester.failed.append("analyze_row_sentiment")
        tester.results["analyze_row_sentiment"] = {"status": "FAILED", "error": str(e)}

    # Print summary
    tester.print_summary()

    # Return exit code
    return 0 if not tester.failed else 1


if __name__ == "__main__":
    sys.exit(main())

# Made with Bob
