# Backend Avro Format Support - Current Limitations

## ⚠️ Important Limitation

**The UI-based Avro listener feature requires `confluent-kafka` Python package**, which may not be installed in all environments.

### Current Status

- ✅ UI checkbox and backend API support implemented
- ✅ Code generation for Avro listeners complete
- ⚠️ Requires `confluent-kafka` package (used by `AGStreamSQL`)
- ⚠️ May fail if dependency not installed

### Recommended Approach

**For Flink SQL compatibility, use direct Python scripts instead of the UI:**

```python
from pydantic import BaseModel
from agentics.core import AGStreamSQL

class Answer(BaseModel):
    text: str

# Create AGStreamSQL instance
stream = AGStreamSQL(
    atype=Answer,
    topic="A",
    kafka_server="localhost:9092",
    schema_registry_url="http://localhost:8081",
    auto_create_topic=True,
)

# Produce Avro messages
answers = [Answer(text="Paris"), Answer(text="London")]
stream.produce(answers)
```

## Problem with UI-Based Approach

The AGStream Manager backend currently uses regular `AGStream` which produces JSON-formatted messages with envelope structure. These messages are **not compatible** with Flink SQL queries that expect Avro format.

When you create a listener through the backend, it produces messages like:
```json
{
  "atype_name": "Answer",
  "atype_schema": {...},
  "states": [{"text": "Paris"}],
  "instructions": "...",
  ...
}
```

But Flink SQL tables configured with `'format' = 'avro-confluent'` expect Avro-serialized state data only.

## Solution

The backend has been updated with `AGStreamSQL` support in `registry_client.py`:

### New Method: `make_agstream_sql()`

```python
def make_agstream_sql(
    self,
    topic: str,
    atype_name: str,
) -> Optional[AGStreamSQL]:
    """
    Construct an AGStreamSQL instance for Flink SQL-compatible streaming.

    Produces Avro-formatted messages that are compatible with Flink SQL queries.
    Messages contain only state data (no envelope) for direct SQL access.
    """
```

## How to Use AGStreamSQL in Your Code

### Option 1: Direct AGStreamSQL Usage (Recommended)

Instead of using the backend's listener manager, create topics directly with AGStreamSQL:

```python
from pydantic import BaseModel
from agentics.core import AGStreamSQL

# Define your model
class Answer(BaseModel):
    text: str

# Create AGStreamSQL instance
stream = AGStreamSQL(
    atype=Answer,
    topic="A",
    kafka_server="localhost:9092",
    schema_registry_url="http://localhost:8081",
    auto_create_topic=True,
)

# Produce Avro messages
answers = [Answer(text="Paris"), Answer(text="London")]
stream.produce(answers)
```

### Option 2: Use Registry Client

```python
from tools.agstream_manager.backend.registry_client import RegistryClient

# Create registry client
client = RegistryClient(
    registry_url="http://localhost:8081",
    kafka_server="localhost:9092"
)

# Create AGStreamSQL instance
stream = client.make_agstream_sql(
    topic="A",
    atype_name="Answer-value"  # Subject name in registry
)

if stream:
    # Produce messages
    from pydantic import BaseModel

    class Answer(BaseModel):
        text: str

    answers = [Answer(text="Paris")]
    stream.produce(answers)
```

## Migration Steps for Existing Topics

If you have existing topics with JSON messages that you want to query with Flink SQL:

### Step 1: Delete Old Topic
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic A
```

### Step 2: Recreate with AGStreamSQL

Create a Python script:
```python
#!/usr/bin/env python3
from pydantic import BaseModel
from agentics.core import AGStreamSQL

class Answer(BaseModel):
    text: str

stream = AGStreamSQL(
    atype=Answer,
    topic="A",
    kafka_server="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

answers = [Answer(text="Paris"), Answer(text="London")]
stream.produce(answers)
print("✅ Topic recreated with Avro format")
```

### Step 3: Verify
```bash
python tools/agstream_manager/scripts/verify_avro_messages.py A
```

### Step 4: Query in Flink SQL
```sql
SELECT * FROM A;
```

## Key Differences

| Feature | AGStream (JSON) | AGStreamSQL (Avro) |
|---------|----------------|-------------------|
| Format | JSON | Avro |
| Message Structure | Envelope + State | State only |
| Flink SQL Compatible | ❌ No | ✅ Yes |
| Use Case | General streaming | SQL analytics |
| Flink Table Format | `'format' = 'json'` | `'format' = 'avro-confluent'` |

## UI Integration Complete ✅

The AGStream Manager UI now fully supports Avro format for Flink SQL compatibility!

### What's New

1. **✅ UI Checkbox**: "Use Avro Format (Flink SQL Compatible)" checkbox in listener creation form
2. **✅ Backend API**: `use_avro` parameter accepted in `/api/transductions/listeners` endpoint
3. **✅ Code Generation**: `generate_listener_code()` conditionally generates AGStreamSQL or AGStream code
4. **✅ Listener Storage**: `use_avro` flag stored with listener configuration

### How to Use

#### Creating an Avro Listener via UI

1. Navigate to **🎧 Manage Listeners** tab
2. Click **➕ Create New Listener**
3. Fill in the form:
   - **Listener Name**: Give it a unique name
   - **Function**: Select your transduction function
   - **Input/Output Topics**: Select existing topics
   - **Parallelism**: Set thread count (1-100)
   - **✅ Use Avro Format**: Check this box for Flink SQL compatibility
4. Click **▶️ Create & Start Listener**

#### When to Use Avro Format

**Use Avro (check the box) when:**
- ✅ You want to query data with Flink SQL
- ✅ You need real-time analytics
- ✅ You're building data pipelines for BI tools
- ✅ You need schema evolution support

**Use JSON (uncheck the box) when:**
- ✅ You're doing general event streaming
- ✅ You don't need SQL queries
- ✅ You want envelope metadata (atype_name, instructions, etc.)
- ✅ You're using AGStream's built-in features

### Technical Details

#### Generated Code Differences

**Avro Format (use_avro=True)**:
```python
from agentics.core.streaming import AGStream

target = AGStream(
    target_atype_name="TargetType",
    input_topic=INPUT_TOPIC,
    output_topic=OUTPUT_TOPIC,
    use_avro=True,  # Enable Avro serialization
    kafka_server="localhost:9092",
    schema_registry_url=SCHEMA_REGISTRY_URL
)

target.transducible_function_listener(
    fn=fn,
    group_id=CONSUMER_GROUP,
    parallelism=PARALLELISM,
    ...
)
```

**JSON Format (use_avro=False)**:
```python
from agentics.core.streaming import AGStream

target = AGStream(
    target_atype_name="TargetType",
    input_topic=INPUT_TOPIC,
    output_topic=OUTPUT_TOPIC
)

target.transducible_function_listener(
    fn=fn,
    group_id=CONSUMER_GROUP,
    parallelism=PARALLELISM,
    ...
)
```

### API Reference

**POST /api/transductions/listeners**

Request body:
```json
{
  "listener_name": "my-listener",
  "function_name": "my-function",
  "input_topic": "input-topic",
  "output_topic": "output-topic",
  "lookback": 0,
  "parallelism": 1,
  "use_avro": true  // ← New parameter
}
```

Response:
```json
{
  "success": true,
  "listener_id": "my-listener",
  "message": "Listener started with parallelism=1 (single-threaded)",
  "parallelism": 1
}
```

### Migration Guide

If you have existing listeners and want to switch to Avro format:

1. **Stop the old listener** in the UI
2. **Delete the old listener**
3. **Recreate with Avro checkbox enabled**
4. The new listener will produce Avro-formatted messages

**Note**: Existing messages in topics remain in their original format. Only new messages will use the selected format.

### Direct Python Usage (Alternative)

You can still create AGStreamSQL listeners directly in Python without using the UI:

```python
from pydantic import BaseModel
from agentics.core import AGStreamSQL

class Answer(BaseModel):
    text: str

stream = AGStreamSQL(
    atype=Answer,
    topic="A",
    kafka_server="localhost:9092",
    schema_registry_url="http://localhost:8081",
    auto_create_topic=True,
)

answers = [Answer(text="Paris"), Answer(text="London")]
stream.produce(answers)
```

For complete examples, see:
- `examples/agstream_sql_example.py`
- `examples/flink_sql_auto_connector_example.py`
