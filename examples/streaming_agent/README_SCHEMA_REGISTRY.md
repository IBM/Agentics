# Schema Registry Integration with AGStream

This guide explains how to use Karapace Schema Registry with AGStream for managing Pydantic model schemas.

## Overview

The schema registry integration allows you to:
- **Register** Pydantic models as JSON Schemas in Karapace
- **Retrieve** schemas dynamically from the registry
- **Version** schemas and track evolution
- **Validate** message compatibility across schema versions

## Benefits

1. **Centralized Type Management**: Store all your Pydantic schemas in one place
2. **Schema Evolution**: Track changes and ensure backward compatibility
3. **Type Discovery**: Dynamically load types without hardcoding them
4. **Validation**: Ensure messages conform to registered schemas

## Prerequisites

- Kafka running on `localhost:9092`
- Karapace Schema Registry on `localhost:8081`
- Start services: `./manage_services.sh start`

## Basic Usage

### 1. Register a Schema

```python
from pydantic import BaseModel, Field
from agentics.core.streaming import AGStream

class Question(BaseModel):
    text: str = Field(description="The question text")
    category: str
    priority: int = 1

# Create AGStream with atype
stream = AGStream(
    atype=Question,
    input_topic="questions-topic",
    schema_registry_url="http://localhost:8081"
)

# Register the schema
schema_id = stream.register_atype_schema()
print(f"Registered with ID: {schema_id}")
```

### 2. Retrieve a Schema

```python
from agentics.core.streaming import AGStream

# Create AGStream without atype
stream = AGStream(
    input_topic="questions-topic",
    schema_registry_url="http://localhost:8081"
)

# Retrieve schema from registry
Question = stream.get_atype_from_registry()

if Question:
    # Use the retrieved type
    stream.atype = Question
    q = Question(text="What is AI?", category="Technology")
    print(q)
```

### 3. List Schema Versions

```python
stream = AGStream(
    input_topic="questions-topic",
    schema_registry_url="http://localhost:8081"
)

# List all versions
versions = stream.list_registered_schemas()
print(f"Available versions: {versions}")

# Retrieve specific version
if versions:
    schema_v1 = stream.get_atype_from_registry(version="1")
    schema_latest = stream.get_atype_from_registry(version="latest")
```

## Configuration

### Schema Registry URL

Set the schema registry URL when creating AGStream:

```python
stream = AGStream(
    atype=MyType,
    input_topic="my-topic",
    schema_registry_url="http://localhost:8081"  # Default
)
```

Or set it in your `.env` file:

```bash
SCHEMA_REGISTRY_URL=http://localhost:8081
```

### Compatibility Modes

When registering schemas, you can specify compatibility mode:

```python
schema_id = stream.register_atype_schema(
    compatibility="BACKWARD"  # BACKWARD, FORWARD, FULL, NONE
)
```

**Compatibility Modes:**
- `BACKWARD`: New schema can read old data (default)
- `FORWARD`: Old schema can read new data
- `FULL`: Both backward and forward compatible
- `NONE`: No compatibility checks

## Subject Naming Convention

Schemas are registered using Kafka's subject naming convention:

- **Value schemas**: `{topic-name}-value`
- **Key schemas**: `{topic-name}-key`

Example:
```python
# Registers as "questions-topic-value"
stream.register_atype_schema(topic="questions-topic", is_key=False)

# Registers as "questions-topic-key"
stream.register_atype_schema(topic="questions-topic", is_key=True)
```

## API Reference

### `register_atype_schema()`

Register the atype's JSON Schema in the registry.

**Parameters:**
- `topic` (str, optional): Kafka topic name (uses `self.input_topic` if not provided)
- `is_key` (bool): If True, registers as key schema, otherwise value schema
- `compatibility` (str): Compatibility mode (BACKWARD, FORWARD, FULL, NONE)

**Returns:**
- `int`: Schema ID if successful, None on error

**Example:**
```python
schema_id = stream.register_atype_schema(
    topic="my-topic",
    is_key=False,
    compatibility="BACKWARD"
)
```

### `get_atype_from_registry()`

Retrieve and reconstruct atype from Schema Registry.

**Parameters:**
- `topic` (str, optional): Kafka topic name (uses `self.input_topic` if not provided)
- `is_key` (bool): If True, retrieves key schema, otherwise value schema
- `version` (str): Schema version ("latest" or specific version number)

**Returns:**
- `Type[BaseModel]`: Pydantic BaseModel class if successful, None on error

**Example:**
```python
# Get latest version
MyType = stream.get_atype_from_registry()

# Get specific version
MyTypeV1 = stream.get_atype_from_registry(version="1")
```

### `list_registered_schemas()`

List all versions of registered schemas for a topic.

**Parameters:**
- `topic` (str, optional): Kafka topic name (uses `self.input_topic` if not provided)
- `is_key` (bool): If True, lists key schemas, otherwise value schemas

**Returns:**
- `List[int]`: List of version numbers if successful, None on error

**Example:**
```python
versions = stream.list_registered_schemas()
print(f"Available versions: {versions}")
```

## Complete Example

See `schema_registry_example.py` for a complete working example:

```bash
cd examples/streaming_agent
python schema_registry_example.py
```

## Schema Evolution Example

```python
from pydantic import BaseModel, Field
from agentics.core.streaming import AGStream

# Version 1: Initial schema
class QuestionV1(BaseModel):
    text: str
    category: str

stream = AGStream(atype=QuestionV1, input_topic="questions")
v1_id = stream.register_atype_schema()

# Version 2: Add optional field (backward compatible)
class QuestionV2(BaseModel):
    text: str
    category: str
    priority: int = 1  # New optional field

stream.atype = QuestionV2
v2_id = stream.register_atype_schema()

# List versions
versions = stream.list_registered_schemas()
print(f"Versions: {versions}")  # [1, 2]

# Retrieve old version
OldQuestion = stream.get_atype_from_registry(version="1")
```

## Monitoring Schemas

### Via Karapace REST API

```bash
# List all subjects
curl http://localhost:8081/subjects

# Get schema versions for a subject
curl http://localhost:8081/subjects/questions-topic-value/versions

# Get specific version
curl http://localhost:8081/subjects/questions-topic-value/versions/1
```

### Via Schema Registry UI

Open http://localhost:8000 in your browser to view and manage schemas visually.

## Troubleshooting

### Schema Registry Not Available

```bash
# Check if Karapace is running
curl http://localhost:8081/subjects

# Start services if needed
./manage_services.sh start
```

### Schema Registration Fails

```python
# Check compatibility mode
stream._set_compatibility("my-topic-value", "NONE")

# Then try registering again
schema_id = stream.register_atype_schema()
```

### Cannot Retrieve Schema

```bash
# Verify schema exists
curl http://localhost:8081/subjects/my-topic-value/versions

# Check subject name format
# Should be: {topic-name}-value or {topic-name}-key
```

## Best Practices

1. **Register schemas early**: Register schemas when creating topics
2. **Use semantic versioning**: Track major schema changes
3. **Test compatibility**: Test schema evolution before deploying
4. **Document changes**: Add descriptions to schema fields
5. **Use BACKWARD compatibility**: Allows adding optional fields safely

## Integration with Listeners

```python
from agentics.core.streaming import AGStream
from pydantic import BaseModel

class Answer(BaseModel):
    text: str
    confidence: float

# Register output schema
answer_stream = AGStream(
    atype=Answer,
    output_topic="answers",
    schema_registry_url="http://localhost:8081"
)
answer_stream.register_atype_schema()

# Start listener - it will use registered schema
answer_stream.listen(atype=Question)
```

## Next Steps

- Explore `schema_registry_example.py` for complete examples
- Check Karapace documentation for advanced features
- Integrate with your streaming pipelines
- Set up schema validation in production

---

**Made with ❤️ for Agentics**
