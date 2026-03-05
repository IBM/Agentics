# AGStream Avro Format Migration Plan

## Overview
Convert AGStream from JSON Schema to Avro format for Flink SQL compatibility.

## Why Avro?
- **Flink SQL Compatibility**: Flink SQL has native support for Avro with Schema Registry (`avro-confluent` format)
- **Better Performance**: Binary format is more compact and faster than JSON
- **Schema Evolution**: Avro has robust schema evolution capabilities
- **Industry Standard**: Avro is the de facto standard for Kafka + Schema Registry

## Current State
AGStream uses JSON Schema with Schema Registry:
- Schema Type: `"JSON"`
- Serialization: JSON with 5-byte Schema Registry header
- Registration: `streaming_utils.py` line 309

## Migration Steps

### 1. Add Dependencies
```bash
pip install fastavro
```

### 2. Create Pydantic to Avro Converter
Create `src/agentics/core/avro_utils.py`:
```python
def pydantic_to_avro_schema(model: Type[BaseModel]) -> dict:
    """Convert Pydantic model to Avro schema"""
    # Map Pydantic types to Avro types
    # Handle nested models, optional fields, etc.
```

### 3. Update Schema Registration
Modify `src/agentics/core/streaming_utils.py`:
```python
def register_schema(atype, schema_registry_url, ...):
    # Change from:
    schema_data = {"schemaType": "JSON", "schema": json.dumps(json_schema)}

    # To:
    avro_schema = pydantic_to_avro_schema(atype)
    schema_data = {"schemaType": "AVRO", "schema": json.dumps(avro_schema)}
```

### 4. Update Serialization
Modify message production in `src/agentics/core/streaming.py`:
- Replace JSON serialization with Avro serialization
- Use `fastavro` or `confluent_kafka.avro.AvroProducer`

### 5. Update Deserialization
Modify message consumption:
- Replace JSON deserialization with Avro deserialization
- Use `fastavro` or `confluent_kafka.avro.AvroConsumer`

### 6. Update Flink SQL Documentation
Update `tools/agstream_manager/scripts/flink_sql.sh` examples:
```sql
CREATE TABLE Q (
  text STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'Q',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);
```

### 7. Migration Strategy

#### Option A: Clean Break (Recommended for Development)
1. Stop all listeners
2. Delete all topics
3. Deploy new Avro-based code
4. Recreate topics with Avro schemas
5. Restart listeners

#### Option B: Gradual Migration (Production)
1. Add format parameter to AGStream
2. Support both JSON and Avro formats
3. Migrate topics one by one
4. Deprecate JSON format after migration

### 8. Testing Checklist
- [ ] Schema registration works with Avro
- [ ] Message production serializes correctly
- [ ] Message consumption deserializes correctly
- [ ] Flink SQL can query topics
- [ ] Listeners work with Avro format
- [ ] Schema evolution works (add/remove fields)
- [ ] Performance benchmarks (Avro vs JSON)

### 9. Files to Modify
1. `src/agentics/core/streaming_utils.py` - Schema registration
2. `src/agentics/core/streaming.py` - Serialization/deserialization
3. `src/agentics/core/transducible_functions.py` - Schema validation
4. `pyproject.toml` - Add fastavro dependency
5. Documentation - Update examples

### 10. Backward Compatibility
Consider adding a configuration option:
```python
class AGStream:
    def __init__(self, ..., schema_format="avro"):  # or "json"
        self.schema_format = schema_format
```

## Estimated Effort
- Development: 2-3 days
- Testing: 1-2 days
- Documentation: 1 day
- **Total: 4-6 days**

## Risks
1. **Breaking Change**: Existing topics won't work without migration
2. **Data Loss**: If not migrated carefully
3. **Complexity**: Avro schema mapping can be tricky for complex Pydantic models
4. **Dependencies**: Additional library dependencies

## Recommendation
This is a **major architectural change** that should be:
1. Planned as a separate sprint/milestone
2. Thoroughly tested in development environment
3. Documented with migration guides
4. Rolled out with a clear migration strategy

## Alternative: Hybrid Approach
Keep JSON Schema for AGStream internal use, but add a separate "Flink-compatible" mode:
```python
ag = AGStream(..., flink_compatible=True)  # Uses Avro
```

This allows gradual adoption without breaking existing systems.
