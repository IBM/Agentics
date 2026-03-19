#!/usr/bin/env python3
"""
Example Python UDFs for Flink SQL
Place this file in a location accessible to the Flink container
"""

import asyncio
import os
import sys
from typing import Optional

# Load environment variables from .env file
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pyflink.table import DataTypes
from pyflink.table.udf import udf

from agentics import AG

# Try to load from common locations
for env_path in ["/opt/flink/.env", "../agentics911/agentics/.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded .env from {env_path}", file=sys.stderr)
        break


class Sentiment(BaseModel):
    sentiment_label: Optional[str] = Field(
        None,
        description="Sentiment Label for the input text (Positive Negative or Neutral)",
    )


# Global AG instance (reused across calls for efficiency)
_ag_sentiment = None


def get_ag_sentiment():
    """Get or create AG instance - uses default LLM config from environment"""
    global _ag_sentiment
    if _ag_sentiment is None:
        # Check if we have API keys
        api_key = os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError(
                "No API key found in environment. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY"
            )

        # AG will use environment variables for LLM configuration
        _ag_sentiment = AG(atype=Sentiment)
        print(f"Created AG instance with API key: {api_key[:10]}...", file=sys.stderr)
    return _ag_sentiment


def run_async(coro):
    """Run async coroutine in sync context, handling existing event loops"""
    try:
        # Try to create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    except Exception as e:
        print(f"Error running async: {e}", file=sys.stderr)
        raise


@udf(result_type=DataTypes.STRING())
def generate_sentiment(text):
    """Generate sentiment label using Agentics AG"""
    if text is None:
        return None

    try:
        # Get AG instance with LLM config
        target = get_ag_sentiment()

        # Execute async operation synchronously
        ag_result = run_async(target << text)

        # Debug: Log what we received
        print(f"DEBUG: Received AG result type: {type(ag_result)}", file=sys.stderr)

        # Extract states from AG object
        if hasattr(ag_result, "states") and ag_result.states:
            states = ag_result.states
            print(f"DEBUG: Found states: {states}", file=sys.stderr)

            if isinstance(states, list) and len(states) > 0:
                result = states[0]
                print(f"DEBUG: First state: {result}", file=sys.stderr)

                if hasattr(result, "sentiment_label") and result.sentiment_label:
                    label = result.sentiment_label
                    print(f"DEBUG: sentiment_label value: {label}", file=sys.stderr)
                    return label.upper()  # Normalize to uppercase

        # If we got here, couldn't extract sentiment
        print(
            f"DEBUG: Could not extract sentiment_label, returning NEUTRAL",
            file=sys.stderr,
        )
        return "NEUTRAL"  # Default fallback
    except Exception as e:
        # Log error with details
        import traceback

        error_msg = f"Error in generate_sentiment: {e}\n{traceback.format_exc()}"
        print(error_msg, file=sys.stderr)
        return f"ERROR: {str(e)[:50]}"


@udf(result_type=DataTypes.STRING())
def add_prefix(text):
    """Add 'Q: ' prefix to text"""
    if text is None:
        return None
    return f"Q: {text}"


@udf(result_type=DataTypes.STRING())
def uppercase_text(text):
    """Convert text to uppercase"""
    if text is None:
        return None
    return text.upper()


@udf(result_type=DataTypes.STRING())
def format_with_confidence(text, confidence):
    """Format text with confidence level"""
    if text is None:
        return None
    if confidence > 0.9:
        level = "HIGH"
    elif confidence > 0.7:
        level = "MEDIUM"
    else:
        level = "LOW"
    return f"[{level}] {text}"


@udf(result_type=DataTypes.INT())
def word_count(text):
    """Count words in text"""
    if text is None:
        return 0
    return len(text.split())


@udf(result_type=DataTypes.DOUBLE())
def normalize_score(score):
    """Normalize score to 0-1 range"""
    if score is None:
        return 0.0
    return max(0.0, min(1.0, float(score)))


# ============================================================================
# ADDITIONAL UDFs - Add your custom functions below
# ============================================================================


@udf(result_type=DataTypes.STRING())
def extract_first_sentence(text):
    """Extract the first sentence from text"""
    if text is None:
        return None
    # Split by common sentence endings
    for delimiter in [". ", "! ", "? "]:
        if delimiter in text:
            return text.split(delimiter)[0] + delimiter.strip()
    return text


@udf(result_type=DataTypes.BOOLEAN())
def contains_keyword(text, keyword):
    """Check if text contains a specific keyword (case-insensitive)"""
    if text is None or keyword is None:
        return False
    return keyword.lower() in text.lower()


@udf(result_type=DataTypes.INT())
def text_length(text):
    """Get the character length of text"""
    if text is None:
        return 0
    return len(text)


@udf(result_type=DataTypes.STRING())
def truncate_text(text, max_length):
    """Truncate text to maximum length with ellipsis"""
    if text is None:
        return None
    max_len = int(max_length) if max_length else 100
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


@udf(result_type=DataTypes.STRING())
def remove_special_chars(text):
    """Remove special characters, keep only alphanumeric and spaces"""
    if text is None:
        return None
    import re

    return re.sub(r"[^a-zA-Z0-9\s]", "", text)


@udf(result_type=DataTypes.DOUBLE())
def calculate_percentage(value, total):
    """Calculate percentage (value/total * 100)"""
    if value is None or total is None or total == 0:
        return 0.0
    return (float(value) / float(total)) * 100.0


@udf(result_type=DataTypes.STRING())
def sentiment_label(score):
    """Convert numeric sentiment score to label"""
    if score is None:
        return "NEUTRAL"
    score_val = float(score)
    if score_val > 0.6:
        return "POSITIVE"
    elif score_val < 0.4:
        return "NEGATIVE"
    else:
        return "NEUTRAL"


@udf(result_type=DataTypes.STRING())
def sentiment_label(score):
    """Convert numeric sentiment score to label"""
    if score is None:
        return "NEUTRAL"
    score_val = float(score)
    if score_val > 0.6:
        return "POSITIVE"
    elif score_val < 0.4:
        return "NEGATIVE"
    else:
        return "NEUTRAL"


@udf(result_type=DataTypes.STRING())
def extract_domain(email):
    """Extract domain from email address"""
    if email is None:
        return None
    if "@" in email:
        return email.split("@")[1]
    return None


@udf(result_type=DataTypes.ARRAY(DataTypes.STRING()))
def split_into_words(text):
    """Split text into array of words"""
    if text is None:
        return []
    return text.split()


@udf(result_type=DataTypes.STRING())
def capitalize_words(text):
    """Capitalize first letter of each word"""
    if text is None:
        return None
    return text.title()


# Made with Bob
