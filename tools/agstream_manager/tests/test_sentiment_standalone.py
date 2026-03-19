#!/usr/bin/env python3
"""
Standalone test for sentiment analysis UDF logic
Tests the transduction without Flink/PyFlink context
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add agentics to path
agentics_root = Path(__file__).parent.parent.parent.parent / "src"
sys.path.insert(0, str(agentics_root))

# Load environment variables
from dotenv import load_dotenv

# Try to load from common locations
for env_path in ["../../../.env", ".env", "/opt/flink/.env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}")
        break
else:
    print("⚠ Warning: No .env file found")

from typing import Optional

# Import after loading env
from pydantic import BaseModel, Field

# Import agentics transducible functions
from agentics.core import transducible_functions


# Define the models (same as in semantic_operators.py)
class TextInput(BaseModel):
    """Generic text input for transduction"""

    text: str = Field(description="Input text to process")


class Sentiment(BaseModel):
    """Sentiment analysis result"""

    label: str = Field(description="POSITIVE, NEGATIVE, or NEUTRAL")
    confidence: float = Field(description="Confidence score between 0 and 1")
    reasoning: Optional[str] = Field(None, description="Brief explanation")


def run_transduction_sync(
    input_text: str, input_model: type[BaseModel], output_model: type[BaseModel]
) -> dict:
    """
    Run an agentics transduction synchronously (same logic as UDF).
    """
    # Create input instance
    input_obj = input_model(text=input_text)

    # Create transduction function using << operator
    transduce_fn = output_model << input_model

    # Run async function in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(transduce_fn(input_obj))

        # Handle TransductionResult - extract the value
        if hasattr(result, "value"):
            result = result.value

        return result.model_dump() if hasattr(result, "model_dump") else result
    finally:
        loop.close()


def analyze_sentiment(text: str) -> str:
    """
    Analyze sentiment of text (same logic as UDF).
    Returns JSON string with sentiment analysis.
    """
    if text is None or text.strip() == "":
        return json.dumps(
            {"label": "NEUTRAL", "confidence": 0.0, "reasoning": "Empty input"}
        )

    try:
        result = run_transduction_sync(text, TextInput, Sentiment)
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"label": "ERROR", "confidence": 0.0, "reasoning": str(e)})


def extract_sentiment_label(sentiment_json: str) -> str:
    """
    Extract sentiment label from JSON result (same logic as UDF).
    """
    if sentiment_json is None:
        return None
    try:
        data = json.loads(sentiment_json)
        return data.get("label", "UNKNOWN")
    except:
        return "ERROR"


def main():
    """Test the sentiment analysis with sample inputs"""
    print("\n" + "=" * 70)
    print("Testing Sentiment Analysis (Standalone)")
    print("=" * 70 + "\n")

    # Check environment
    selected_llm = os.getenv("SELECTED_LLM", "not set")
    print(f"🤖 SELECTED_LLM: {selected_llm}")

    if selected_llm == "watsonx":
        api_key = os.getenv("WATSONX_APIKEY", "not set")
        print(f"🔑 WATSONX_APIKEY: {'✓ set' if api_key != 'not set' else '✗ not set'}")
    elif selected_llm == "openai":
        api_key = os.getenv("OPENAI_API_KEY", "not set")
        print(f"🔑 OPENAI_API_KEY: {'✓ set' if api_key != 'not set' else '✗ not set'}")

    print("\n" + "-" * 70 + "\n")

    # Test cases
    test_cases = [
        "This product is terrible",
        "I love this! It's amazing!",
        "Who is maradona?",
        "The weather is nice today",
        "I hate waiting in long lines",
    ]

    for i, text in enumerate(test_cases, 1):
        print(f"Test {i}: {text}")
        print("-" * 70)

        try:
            # Get full JSON result
            sentiment_json = analyze_sentiment(text)
            print(f"Full JSON: {sentiment_json}")

            # Extract label
            label = extract_sentiment_label(sentiment_json)
            print(f"Label: {label}")

            # Parse and show details
            data = json.loads(sentiment_json)
            if "confidence" in data:
                print(f"Confidence: {data['confidence']:.2f}")
            if "reasoning" in data and data["reasoning"]:
                print(f"Reasoning: {data['reasoning']}")

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback

            traceback.print_exc()

        print("\n")

    print("=" * 70)
    print("Test Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()

# Made with Bob
