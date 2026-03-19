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

# Load environment variables from .env file
from dotenv import load_dotenv

# Try to load from common locations
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break
else:
    print(
        "⚠ Warning: No .env file found. LLM API keys may not be available.",
        file=sys.stderr,
    )

from typing import Union

from pydantic import BaseModel, Field
from pyflink.table import DataTypes
from pyflink.table.types import Row
from pyflink.table.udf import TableFunction, udf, udtf

# NOTE: Do NOT import agentics at module level to avoid heavy dependencies
# Import it lazily inside functions that need it
# ============================================================================
# Helper: Run async operations in sync context
# ============================================================================


def run_async_operation(coro):
    """
    Run an async operation synchronously.

    Handles event loop creation for Flink's synchronous UDF context.
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    except Exception as e:
        print(f"✗ Error running async operation: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        raise


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
    # Lazy import to avoid loading heavy dependencies at module level
    from agentics.core import transducible_functions

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

        # Convert to dict
        return result.model_dump() if hasattr(result, "model_dump") else result
    finally:
        loop.close()


def run_row_transduction(
    row_json: str, input_model: type[BaseModel], output_model: type[BaseModel]
) -> dict:
    """
    Run an agentics transduction on an entire row object synchronously.

    Args:
        row_json: JSON string representing the entire row
        input_model: Pydantic model for input (should match row structure)
        output_model: Pydantic model for output

    Returns:
        Dictionary representation of the output model
    """
    # Lazy import to avoid loading heavy dependencies at module level
    from agentics.core import transducible_functions

    # Parse JSON and create input instance
    try:
        row_data = json.loads(row_json) if isinstance(row_json, str) else row_json
        input_obj = input_model(**row_data)
    except Exception as e:
        raise ValueError(f"Failed to parse row data: {str(e)}")

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

        # Convert to dict
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

            # Handle TransductionResult - extract the value
            if hasattr(result, "value"):
                result = result.value

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


# Global AG instance for row sentiment
_ag_row_sentiment = None


def get_ag_row_sentiment():
    """Get or create AG instance for row sentiment analysis"""
    global _ag_row_sentiment
    if _ag_row_sentiment is None:
        from agentics import AG

        _ag_row_sentiment = AG(atype=Sentiment)
    return _ag_row_sentiment


@udf(result_type=DataTypes.STRING())
def analyze_row_sentiment(row_json):
    """
    Analyze sentiment of an entire row object using LLM transduction.

    The row is imported as an agentic object and sentiment analysis is performed
    on the complete context of all fields in the row.

    Args:
        row_json: JSON string representing the entire row with all fields

    Returns:
        JSON string with sentiment analysis result

    Example:
        SELECT analyze_row_sentiment(CONCAT('{"text":"', text, '"}'))
        FROM Q LIMIT 3;
    """
    if row_json is None or row_json.strip() == "":
        return json.dumps(
            {"label": "NEUTRAL", "confidence": 0.0, "reasoning": "Empty input"}
        )

    try:
        # Get AG instance
        ag = get_ag_row_sentiment()

        # Run transduction using AG (same as working UDF)
        result = run_async_operation(ag << row_json)

        # Debug logging
        print(f"DEBUG: AG result type: {type(result)}", file=sys.stderr)
        print(f"DEBUG: AG result: {result}", file=sys.stderr)

        # Handle result - could be list or single object
        if isinstance(result, list) and len(result) > 0:
            sentiment_obj = result[0]
        else:
            sentiment_obj = result

        print(f"DEBUG: sentiment_obj type: {type(sentiment_obj)}", file=sys.stderr)
        print(f"DEBUG: sentiment_obj attributes: {dir(sentiment_obj)}", file=sys.stderr)

        # Extract sentiment data - try different attribute names
        if hasattr(sentiment_obj, "label"):
            label = sentiment_obj.label
            confidence = getattr(sentiment_obj, "confidence", 0.0)
            reasoning = getattr(sentiment_obj, "reasoning", "")
        elif hasattr(sentiment_obj, "sentiment_label"):
            label = sentiment_obj.sentiment_label
            confidence = getattr(sentiment_obj, "confidence", 0.0)
            reasoning = getattr(sentiment_obj, "reasoning", "")
        elif hasattr(sentiment_obj, "model_dump"):
            data = sentiment_obj.model_dump()
            label = data.get("label", data.get("sentiment_label", "UNKNOWN"))
            confidence = data.get("confidence", 0.0)
            reasoning = data.get("reasoning", "")
        else:
            label = str(sentiment_obj)
            confidence = 0.0
            reasoning = ""

        return json.dumps(
            {"label": label, "confidence": confidence, "reasoning": reasoning}
        )

    except Exception as e:
        return json.dumps(
            {"label": "ERROR", "confidence": 0.0, "reasoning": f"Error: {str(e)}"}
        )


# Made with Bob


# ============================================================================
# Universal Transduction UDTF with Schema Registry Integration
# ============================================================================

# Global caches for schema and models
_target_model_cache = {}
_schema_registry_url = None


def _get_schema_registry_url():
    """Get Schema Registry URL from environment."""
    global _schema_registry_url
    if _schema_registry_url is None:
        _schema_registry_url = os.getenv(
            "AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL", "http://localhost:8081"
        )
    return _schema_registry_url


def _get_target_model(topic_name: str):
    """
    Fetch target schema from Schema Registry and create Pydantic model.

    Args:
        topic_name: Name of the output topic

    Returns:
        Pydantic model class for the target schema
    """
    if topic_name in _target_model_cache:
        return _target_model_cache[topic_name]

    # Lazy import streaming utilities
    import sys

    sys.path.insert(0, "/opt/flink")
    from agentics.core.streaming.streaming_utils import (
        create_pydantic_from_json_schema,
        get_atype_from_registry,
    )

    try:
        # Get schema from registry
        schema_data = get_atype_from_registry(
            topic_name, schema_registry_url=_get_schema_registry_url()
        )

        if not schema_data:
            raise ValueError(f"No schema found for topic: {topic_name}")

        # Get JSON schema from the atype
        if hasattr(schema_data, "model_json_schema"):
            json_schema = schema_data.model_json_schema()
        elif isinstance(schema_data, dict):
            json_schema = schema_data
        else:
            raise ValueError(f"Invalid schema data type: {type(schema_data)}")

        # Create Pydantic model from JSON schema
        target_model = create_pydantic_from_json_schema(
            json_schema, model_name=f"{topic_name}_Target"
        )

        if not target_model:
            raise ValueError(
                f"Failed to create model from schema for topic: {topic_name}"
            )

        _target_model_cache[topic_name] = target_model
        return target_model

    except Exception as e:
        raise RuntimeError(f"Error fetching schema for topic {topic_name}: {str(e)}")


@udtf(result_types=[DataTypes.STRING(), DataTypes.FLOAT(), DataTypes.STRING()])
def universal_transduce(topic_name: str, row_json: str):
    """
    Universal UDTF that performs transduction using Schema Registry for output schema.

    This UDTF:
    1. Takes a topic name and row JSON as input
    2. Fetches the target schema from Schema Registry
    3. Creates Pydantic models dynamically
    4. Performs transduction using agentics << operator
    5. Returns multiple columns matching the target schema

    Usage in SQL:
        SELECT Q.text, T.*
        FROM Q
        CROSS JOIN LATERAL TABLE(
            universal_transduce('sentiment-output', CONCAT('{"text":"', Q.text, '"}'))
        ) AS T(label, confidence, reasoning)

    Args:
        topic_name: Name of the target topic (used to fetch schema from Schema Registry)
        row_json: JSON string representing the input row

    Yields:
        Tuple of values matching the target schema fields
    """
    if not row_json or row_json.strip() == "":
        return

    try:
        # Lazy import agentics
        from agentics.core import transducible_functions

        # Get target model from Schema Registry
        target_model = _get_target_model(topic_name)

        # Create dynamic source model
        class SourceModel(BaseModel):
            """Dynamic source model"""

            class Config:
                extra = "allow"

        # Parse source data
        row_data = json.loads(row_json)
        source_obj = SourceModel(**row_data)

        # Create transduction function
        transduce_fn = target_model << SourceModel

        # Run transduction
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(transduce_fn(source_obj))

            # Handle TransductionResult
            if hasattr(result, "value"):
                result = result.value

            # Extract field values in order
            field_values = []
            for field_name in target_model.model_fields.keys():
                value = getattr(result, field_name, None)
                # Convert to string if complex type
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                field_values.append(value)

            yield tuple(field_values)

        finally:
            loop.close()

    except Exception as e:
        # Yield error row
        error_msg = f"Error: {str(e)}"
        # Return tuple with error in first field, None for rest
        try:
            num_fields = len(_get_target_model(topic_name).model_fields)
            yield tuple([error_msg] + [None] * (num_fields - 1))
        except:
            # If we can't get the model, return default 3-field error
            yield (error_msg, None, None)
