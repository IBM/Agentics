#!/usr/bin/env python3
"""
Generic Semantic UDF for Flink SQL with Schema Registry Integration
====================================================================

This module provides a flexible UDF system that:
1. Takes a dataframe (via Flink SQL table) as input
2. Retrieves Pydantic types from the schema registry
3. Performs map/reduce/generate operations using Agentics
4. Returns transformed dataframes

The UDF integrates with:
- Karapace/Confluent Schema Registry for type discovery
- AGStream Manager for schema management
- Agentics semantic operators for LLM-powered transformations

Author: Agentics Team
"""

import asyncio
import json
import os
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union

# Add agentics to path for Flink container
sys.path.insert(0, "/opt/flink")

# Load environment variables
from dotenv import load_dotenv

for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break

import pandas as pd
from pydantic import BaseModel, Field
from pyflink.table import DataTypes
from pyflink.table.udf import udtf

# Agentics imports
from agentics import AG
from agentics.core.semantic_operators import sem_agg, sem_filter, sem_map
from agentics.core.streaming.streaming_utils import get_atype_from_registry

# ============================================================================
# Operation Types
# ============================================================================


class OperationType(str, Enum):
    """Supported operation types for the generic UDF"""

    MAP = "map"
    REDUCE = "reduce"
    GENERATE = "generate"
    FILTER = "filter"
    AGGREGATE = "aggregate"


# ============================================================================
# Schema Registry Client
# ============================================================================


class SchemaRegistryClient:
    """
    Lightweight client for fetching Pydantic types from schema registry.

    This client provides a simple interface to retrieve registered types
    that can be used in semantic operations.
    """

    def __init__(
        self,
        registry_url: Optional[str] = None,
        kafka_server: Optional[str] = None,
    ):
        self.registry_url = registry_url or os.getenv(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        )
        self.kafka_server = kafka_server or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self._type_cache: Dict[str, Type[BaseModel]] = {}

    def get_type(
        self,
        type_name: str,
        version: str = "latest",
        add_suffix: bool = True,
    ) -> Optional[Type[BaseModel]]:
        """
        Retrieve a Pydantic type from the schema registry.

        Args:
            type_name: Name of the registered type (e.g., "Sentiment", "Question")
            version: Schema version to retrieve (default: "latest")
            add_suffix: Whether to add "-value" suffix to type name

        Returns:
            Pydantic BaseModel class or None if not found
        """
        cache_key = f"{type_name}:{version}:{add_suffix}"

        if cache_key in self._type_cache:
            return self._type_cache[cache_key]

        try:
            atype = get_atype_from_registry(
                atype_name=type_name,
                schema_registry_url=self.registry_url,
                version=version,
                add_suffix=add_suffix,
            )

            if atype:
                self._type_cache[cache_key] = atype
                print(f"✓ Retrieved type '{type_name}' from registry", file=sys.stderr)
            else:
                print(f"✗ Type '{type_name}' not found in registry", file=sys.stderr)

            return atype
        except Exception as e:
            print(f"✗ Error retrieving type '{type_name}': {e}", file=sys.stderr)
            return None

    def list_types(self) -> List[str]:
        """List all available types in the schema registry."""
        import requests

        try:
            response = requests.get(f"{self.registry_url}/subjects", timeout=5)
            if response.status_code == 200:
                subjects = response.json()
                # Remove -value/-key suffixes for cleaner names
                return sorted(
                    set(s.replace("-value", "").replace("-key", "") for s in subjects)
                )
            return []
        except Exception as e:
            print(f"✗ Error listing types: {e}", file=sys.stderr)
            return []


# Global registry client (reused across calls)
_registry_client: Optional[SchemaRegistryClient] = None


def get_registry_client() -> SchemaRegistryClient:
    """Get or create the global schema registry client."""
    global _registry_client
    if _registry_client is None:
        _registry_client = SchemaRegistryClient()
    return _registry_client


# ============================================================================
# Operation Executors
# ============================================================================


async def execute_map_operation(
    df: pd.DataFrame,
    target_type: Type[BaseModel],
    instructions: str,
    merge_output: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    Execute a semantic map operation on a dataframe.

    Args:
        df: Input pandas DataFrame
        target_type: Target Pydantic type for mapping
        instructions: Natural language instructions for the mapping
        merge_output: Whether to merge results with original data
        **kwargs: Additional arguments for AG configuration

    Returns:
        Transformed DataFrame
    """
    result = await sem_map(
        source=df,
        target_type=target_type,
        instructions=instructions,
        merge_output=merge_output,
        **kwargs,
    )
    return result if isinstance(result, pd.DataFrame) else result.to_dataframe()


async def execute_reduce_operation(
    df: pd.DataFrame,
    target_type: Type[BaseModel],
    instructions: str,
    **kwargs,
) -> pd.DataFrame:
    """
    Execute a semantic reduce/aggregate operation on a dataframe.

    Args:
        df: Input pandas DataFrame
        target_type: Target Pydantic type for aggregation
        instructions: Natural language instructions for aggregation
        **kwargs: Additional arguments for AG configuration

    Returns:
        Aggregated DataFrame (typically single row)
    """
    result = await sem_agg(
        source=df,
        target_type=target_type,
        instructions=instructions,
        **kwargs,
    )
    return result if isinstance(result, pd.DataFrame) else result.to_dataframe()


async def execute_filter_operation(
    df: pd.DataFrame,
    predicate: str,
    sensitivity: float = 0.8,
    **kwargs,
) -> pd.DataFrame:
    """
    Execute a semantic filter operation on a dataframe.

    Args:
        df: Input pandas DataFrame
        predicate: Natural language predicate for filtering
        sensitivity: Threshold for filter decision (0-1)
        **kwargs: Additional arguments for AG configuration

    Returns:
        Filtered DataFrame
    """
    result = await sem_filter(
        source=df,
        predicate_template=predicate,
        sensitivity=sensitivity,
        **kwargs,
    )
    return result if isinstance(result, pd.DataFrame) else result.to_dataframe()


async def execute_generate_operation(
    df: pd.DataFrame,
    target_type: Type[BaseModel],
    instructions: str,
    num_samples: int = 1,
    **kwargs,
) -> pd.DataFrame:
    """
    Execute a semantic generation operation based on input dataframe.

    Args:
        df: Input pandas DataFrame (used as context)
        target_type: Target Pydantic type for generation
        instructions: Natural language instructions for generation
        num_samples: Number of samples to generate
        **kwargs: Additional arguments for AG configuration

    Returns:
        DataFrame with generated samples
    """
    # Create AG from dataframe as context
    ag_source = AG.from_dataframe(df)

    # Create target AG for generation
    target_ag = AG(
        atype=target_type,
        instructions=instructions,
        **kwargs,
    )

    # Generate samples using the context
    generated_states = []
    for _ in range(num_samples):
        result = await (target_ag << ag_source)
        if hasattr(result, "states") and result.states:
            generated_states.extend(result.states)

    # Convert to dataframe
    if generated_states:
        # Convert states to dataframe directly
        data_dicts = [
            state.model_dump() if hasattr(state, "model_dump") else state
            for state in generated_states
        ]
        return pd.DataFrame(data_dicts)
    else:
        # Return empty dataframe with correct schema
        return pd.DataFrame()


# ============================================================================
# Synchronous Wrapper
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
# Main Generic UDF
# ============================================================================


def semantic_transform(
    input_data: str,
    operation: str,
    target_type_name: str,
    instructions: str = "",
    merge_output: bool = True,
    sensitivity: float = 0.8,
    num_samples: int = 1,
    **kwargs,
) -> pd.DataFrame:
    """
    Generic semantic transformation UDF.

    This function performs semantic operations on data using registered
    Pydantic types from the schema registry.

    Args:
        input_data: JSON string representing input data (list of dicts)
        operation: Operation type ("map", "reduce", "filter", "generate")
        target_type_name: Name of target type in schema registry
        instructions: Natural language instructions for the operation
        merge_output: For map operations, whether to merge with input
        sensitivity: For filter operations, decision threshold
        num_samples: For generate operations, number of samples
        **kwargs: Additional AG configuration parameters

    Returns:
        Transformed pandas DataFrame

    Examples:
        Map operation:
        >>> result = semantic_transform(
        ...     input_data='[{"text": "Great product!"}]',
        ...     operation="map",
        ...     target_type_name="Sentiment",
        ...     instructions="Analyze the sentiment of the text"
        ... )

        Reduce operation:
        >>> result = semantic_transform(
        ...     input_data='[{"review": "Good"}, {"review": "Bad"}]',
        ...     operation="reduce",
        ...     target_type_name="Summary",
        ...     instructions="Summarize all reviews"
        ... )

        Filter operation:
        >>> result = semantic_transform(
        ...     input_data='[{"text": "spam"}, {"text": "valid"}]',
        ...     operation="filter",
        ...     target_type_name="",
        ...     instructions="Keep only valid messages"
        ... )
    """
    try:
        # Parse input data
        data_list = json.loads(input_data)
        df = pd.DataFrame(data_list)

        if df.empty:
            print("⚠ Warning: Empty input dataframe", file=sys.stderr)
            return df

        # Get operation type
        op_type = OperationType(operation.lower())

        # Get target type from registry (if needed)
        target_type: Optional[Type[BaseModel]] = None
        if target_type_name and op_type != OperationType.FILTER:
            registry = get_registry_client()
            target_type = registry.get_type(target_type_name)

            if target_type is None:
                raise ValueError(
                    f"Target type '{target_type_name}' not found in schema registry"
                )

        # Execute operation
        if op_type == OperationType.MAP:
            if target_type is None:
                raise ValueError("Target type is required for map operation")
            result_df = run_async_operation(
                execute_map_operation(
                    df, target_type, instructions, merge_output, **kwargs
                )
            )
        elif op_type in (OperationType.REDUCE, OperationType.AGGREGATE):
            if target_type is None:
                raise ValueError("Target type is required for reduce operation")
            result_df = run_async_operation(
                execute_reduce_operation(df, target_type, instructions, **kwargs)
            )
        elif op_type == OperationType.FILTER:
            result_df = run_async_operation(
                execute_filter_operation(df, instructions, sensitivity, **kwargs)
            )
        elif op_type == OperationType.GENERATE:
            if target_type is None:
                raise ValueError("Target type is required for generate operation")
            result_df = run_async_operation(
                execute_generate_operation(
                    df, target_type, instructions, num_samples, **kwargs
                )
            )
        else:
            raise ValueError(f"Unsupported operation type: {operation}")

        print(
            f"✓ Operation '{operation}' completed: {len(result_df)} rows",
            file=sys.stderr,
        )
        return result_df

    except Exception as e:
        print(f"✗ Error in semantic_transform: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        # Return empty dataframe on error
        return pd.DataFrame({"error": [str(e)]})


# ============================================================================
# Flink SQL UDF Wrappers
# ============================================================================


@udtf(result_types=[DataTypes.STRING()])
def semantic_map_udf(input_json: str, target_type: str, instructions: str):
    """
    Flink UDTF for semantic map operations.

    Usage in Flink SQL:
        SELECT * FROM TABLE(semantic_map_udf(
            input_json,
            'Sentiment',
            'Analyze sentiment of each text'
        ))
    """
    try:
        result_df = semantic_transform(
            input_data=input_json,
            operation="map",
            target_type_name=target_type,
            instructions=instructions,
        )

        # Yield each row as JSON
        for _, row in result_df.iterrows():
            yield (json.dumps(row.to_dict()),)
    except Exception as e:
        yield (json.dumps({"error": str(e)}),)


@udtf(result_types=[DataTypes.STRING()])
def semantic_reduce_udf(input_json: str, target_type: str, instructions: str):
    """
    Flink UDTF for semantic reduce/aggregate operations.

    Usage in Flink SQL:
        SELECT * FROM TABLE(semantic_reduce_udf(
            input_json,
            'Summary',
            'Create a summary of all inputs'
        ))
    """
    try:
        result_df = semantic_transform(
            input_data=input_json,
            operation="reduce",
            target_type_name=target_type,
            instructions=instructions,
        )

        # Yield aggregated result
        for _, row in result_df.iterrows():
            yield (json.dumps(row.to_dict()),)
    except Exception as e:
        yield (json.dumps({"error": str(e)}),)


@udtf(result_types=[DataTypes.STRING()])
def semantic_filter_udf(input_json: str, predicate: str, sensitivity: float = 0.8):
    """
    Flink UDTF for semantic filter operations.

    Usage in Flink SQL:
        SELECT * FROM TABLE(semantic_filter_udf(
            input_json,
            'text contains positive sentiment',
            0.8
        ))
    """
    try:
        result_df = semantic_transform(
            input_data=input_json,
            operation="filter",
            target_type_name="",
            instructions=predicate,
            sensitivity=sensitivity,
        )

        # Yield filtered rows
        for _, row in result_df.iterrows():
            yield (json.dumps(row.to_dict()),)
    except Exception as e:
        yield (json.dumps({"error": str(e)}),)


@udtf(result_types=[DataTypes.STRING()])
def semantic_generate_udf(
    input_json: str, target_type: str, instructions: str, num_samples: int = 1
):
    """
    Flink UDTF for semantic generation operations.

    Usage in Flink SQL:
        SELECT * FROM TABLE(semantic_generate_udf(
            input_json,
            'Question',
            'Generate questions based on the context',
            5
        ))
    """
    try:
        result_df = semantic_transform(
            input_data=input_json,
            operation="generate",
            target_type_name=target_type,
            instructions=instructions,
            num_samples=num_samples,
        )

        # Yield generated samples
        for _, row in result_df.iterrows():
            yield (json.dumps(row.to_dict()),)
    except Exception as e:
        yield (json.dumps({"error": str(e)}),)


# ============================================================================
# Utility Functions
# ============================================================================


def list_available_types() -> List[str]:
    """
    List all available types in the schema registry.

    Returns:
        List of type names
    """
    registry = get_registry_client()
    return registry.list_types()


def validate_type_exists(type_name: str) -> bool:
    """
    Check if a type exists in the schema registry.

    Args:
        type_name: Name of the type to check

    Returns:
        True if type exists, False otherwise
    """
    registry = get_registry_client()
    return registry.get_type(type_name) is not None


# Made with Bob
