#!/usr/bin/env python3
"""
Semantic Operators UDFs for Flink SQL
Uses Agentics transducible functions for LLM-powered data transformation
"""

import asyncio
import json
import os
import sys
from typing import Optional

# Add agentics to path
sys.path.insert(0, "/opt/flink")

from pydantic import BaseModel, Field
from pyflink.table import DataTypes
from pyflink.table.udf import udf

# Import agentics transducible functions to enable << operator
from agentics.core import transducible_functions

# ============================================================================
# Predefined Output Types
# ============================================================================


class Sentiment(BaseModel):
    """Sentiment analysis result"""

    label: str = Field(description="POSITIVE, NEGATIVE, or NEUTRAL")
    confidence: float = Field(description="Confidence score between 0 and 1")
    reasoning: Optional[str] = Field(None, description="Brief explanation")


class Category(BaseModel):
    """Text categorization result"""

    category: str = Field(description="The primary category")
    subcategory: Optional[str] = Field(None, description="Optional subcategory")
    confidence: float = Field(description="Confidence score between 0 and 1")


class Summary(BaseModel):
    """Text summarization result"""

    summary: str = Field(description="Concise summary of the input text")
    key_points: list[str] = Field(default_factory=list, description="Main points")
    word_count: int = Field(description="Number of words in summary")


class Entity(BaseModel):
    """Named entity extraction result"""

    entities: list[str] = Field(default_factory=list, description="Extracted entities")
    entity_types: dict[str, str] = Field(
        default_factory=dict, description="Entity type mapping"
    )


class Translation(BaseModel):
    """Translation result"""

    translated_text: str = Field(description="Translated text")
    source_language: str = Field(description="Detected source language")
    target_language: str = Field(description="Target language")


class Question(BaseModel):
    """Question answering input"""

    question: str = Field(description="The question to answer")
    context: Optional[str] = Field(None, description="Context for answering")


class Answer(BaseModel):
    """Question answering result"""

    answer: str = Field(description="The answer to the question")
    confidence: float = Field(description="Confidence in the answer")
    sources: Optional[list[str]] = Field(None, description="Source references")


# ============================================================================
# Helper: Run async transduction in sync context
# ============================================================================


def run_transduction(
    input_text: str, input_model: type[BaseModel], output_model: type[BaseModel]
) -> dict:
    """
    Run an agentics transduction synchronously.

    Args:
        input_text: The input text to transduce
        input_model: Pydantic model for input
        output_model: Pydantic model for output

    Returns:
        Dictionary representation of the output model
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
        return result.model_dump() if hasattr(result, "model_dump") else result
    finally:
        loop.close()


# ============================================================================
# Generic Input Model
# ============================================================================


class TextInput(BaseModel):
    """Generic text input for transduction"""

    text: str = Field(description="Input text to process")


# ============================================================================
# Semantic Operator UDFs
# ============================================================================


@udf(result_type=DataTypes.STRING())
def analyze_sentiment(text):
    """
    Analyze sentiment of text using LLM transduction.

    Returns JSON string with sentiment analysis.

    Example:
        SELECT analyze_sentiment(text) FROM messages;
    """
    if text is None or text.strip() == "":
        return json.dumps(
            {"label": "NEUTRAL", "confidence": 0.0, "reasoning": "Empty input"}
        )

    try:
        result = run_transduction(text, TextInput, Sentiment)
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"label": "ERROR", "confidence": 0.0, "reasoning": str(e)})


@udf(result_type=DataTypes.STRING())
def categorize_text(text):
    """
    Categorize text using LLM transduction.

    Returns JSON string with category information.

    Example:
        SELECT categorize_text(text) FROM documents;
    """
    if text is None or text.strip() == "":
        return json.dumps({"category": "UNKNOWN", "confidence": 0.0})

    try:
        result = run_transduction(text, TextInput, Category)
        return json.dumps(result)
    except Exception as e:
        return json.dumps(
            {"category": "ERROR", "confidence": 0.0, "subcategory": str(e)}
        )


@udf(result_type=DataTypes.STRING())
def summarize_text(text):
    """
    Summarize text using LLM transduction.

    Returns JSON string with summary.

    Example:
        SELECT summarize_text(text) FROM articles;
    """
    if text is None or text.strip() == "":
        return json.dumps({"summary": "", "key_points": [], "word_count": 0})

    try:
        result = run_transduction(text, TextInput, Summary)
        return json.dumps(result)
    except Exception as e:
        return json.dumps(
            {"summary": f"Error: {str(e)}", "key_points": [], "word_count": 0}
        )


@udf(result_type=DataTypes.STRING())
def extract_entities(text):
    """
    Extract named entities using LLM transduction.

    Returns JSON string with extracted entities.

    Example:
        SELECT extract_entities(text) FROM news;
    """
    if text is None or text.strip() == "":
        return json.dumps({"entities": [], "entity_types": {}})

    try:
        result = run_transduction(text, TextInput, Entity)
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"entities": [], "entity_types": {"error": str(e)}})


@udf(result_type=DataTypes.STRING())
def answer_question(question_text):
    """
    Answer a question using LLM transduction.

    Returns JSON string with answer.

    Example:
        SELECT answer_question(text) FROM questions;
    """
    if question_text is None or question_text.strip() == "":
        return json.dumps({"answer": "No question provided", "confidence": 0.0})

    try:
        # Create question input
        question_obj = Question(question=question_text)

        # Create transduction function
        answer_fn = Answer << Question

        # Run transduction
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(answer_fn(question_obj))
            return json.dumps(
                result.model_dump() if hasattr(result, "model_dump") else result
            )
        finally:
            loop.close()
    except Exception as e:
        return json.dumps({"answer": f"Error: {str(e)}", "confidence": 0.0})


# ============================================================================
# Utility UDFs for JSON Extraction
# ============================================================================


@udf(result_type=DataTypes.STRING())
def extract_sentiment_label(sentiment_json):
    """
    Extract sentiment label from JSON result.

    Example:
        SELECT extract_sentiment_label(analyze_sentiment(text)) FROM messages;
    """
    if sentiment_json is None:
        return None
    try:
        data = json.loads(sentiment_json)
        return data.get("label", "UNKNOWN")
    except:
        return "ERROR"


@udf(result_type=DataTypes.DOUBLE())
def extract_confidence(json_result):
    """
    Extract confidence score from JSON result.

    Example:
        SELECT extract_confidence(analyze_sentiment(text)) FROM messages;
    """
    if json_result is None:
        return 0.0
    try:
        data = json.loads(json_result)
        return float(data.get("confidence", 0.0))
    except:
        return 0.0


@udf(result_type=DataTypes.STRING())
def extract_category(category_json):
    """
    Extract category from JSON result.

    Example:
        SELECT extract_category(categorize_text(text)) FROM documents;
    """
    if category_json is None:
        return None
    try:
        data = json.loads(category_json)
        return data.get("category", "UNKNOWN")
    except:
        return "ERROR"


@udf(result_type=DataTypes.STRING())
def extract_summary(summary_json):
    """
    Extract summary text from JSON result.

    Example:
        SELECT extract_summary(summarize_text(text)) FROM articles;
    """
    if summary_json is None:
        return None
    try:
        data = json.loads(summary_json)
        return data.get("summary", "")
    except:
        return "ERROR"


# ============================================================================
# Composite UDFs (Combining multiple operations)
# ============================================================================


@udf(result_type=DataTypes.STRING())
def analyze_and_categorize(text):
    """
    Perform both sentiment analysis and categorization.

    Returns JSON with both results.

    Example:
        SELECT analyze_and_categorize(text) FROM messages;
    """
    if text is None or text.strip() == "":
        return json.dumps(
            {
                "sentiment": {"label": "NEUTRAL", "confidence": 0.0},
                "category": {"category": "UNKNOWN", "confidence": 0.0},
            }
        )

    try:
        sentiment = run_transduction(text, TextInput, Sentiment)
        category = run_transduction(text, TextInput, Category)
        return json.dumps({"sentiment": sentiment, "category": category})
    except Exception as e:
        return json.dumps(
            {
                "sentiment": {"label": "ERROR", "confidence": 0.0, "reasoning": str(e)},
                "category": {"category": "ERROR", "confidence": 0.0},
            }
        )


# Made with Bob
