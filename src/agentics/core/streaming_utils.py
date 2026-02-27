"""
Streaming utilities for Agentics PyFlink integration.

This module provides utility functions and classes for working with
PyFlink streaming jobs in the Agentics framework.
"""

import json
import logging
from typing import Any, Dict, Type

from dotenv import load_dotenv
from pydantic import BaseModel
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction

load_dotenv()

# Configure logging for better visibility
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import ProcessSQLRow from streaming module to avoid duplication
from agentics.core.streaming import ProcessSQLRow


def create_model_from_schema(schema: Dict[str, Any]) -> Type[BaseModel]:
    """
    Create a Pydantic model from a JSON schema.

    Args:
        schema: JSON schema dictionary with 'properties' and 'title'

    Returns:
        Dynamically created Pydantic model class
    """
    from typing import Any, Optional, Union

    from pydantic import create_model

    model_name = schema.get("title", "DynamicModel")
    properties = schema.get("properties", {})

    # Build field definitions from schema properties
    field_definitions = {}
    for field_name, field_schema in properties.items():
        # Determine the field type from schema
        field_type = Any  # Default to Any

        if "anyOf" in field_schema:
            # Handle optional fields (anyOf with null)
            types = []
            for type_def in field_schema["anyOf"]:
                if type_def.get("type") == "string":
                    types.append(str)
                elif type_def.get("type") == "integer":
                    types.append(int)
                elif type_def.get("type") == "number":
                    types.append(float)
                elif type_def.get("type") == "boolean":
                    types.append(bool)
                elif type_def.get("type") == "null":
                    types.append(type(None))

            if len(types) > 1:
                field_type = Union[tuple(types)]
            elif len(types) == 1:
                field_type = types[0]
        elif "type" in field_schema:
            # Simple type mapping
            schema_type = field_schema["type"]
            if schema_type == "string":
                field_type = str
            elif schema_type == "integer":
                field_type = int
            elif schema_type == "number":
                field_type = float
            elif schema_type == "boolean":
                field_type = bool
            elif schema_type == "array":
                field_type = list
            elif schema_type == "object":
                field_type = dict

        # Get default value
        default_value = field_schema.get("default", None)

        # Create field definition
        field_definitions[field_name] = (field_type, default_value)

    # Create and return the model
    return create_model(model_name, **field_definitions)
