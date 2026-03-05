# AGStreamSQL Guide - Flink SQL Compatible Streaming

## Overview

`AGStreamSQL` is a new parallel implementation of AGStream optimized for Flink SQL compatibility. It uses Avro format and sends only state data (no envelope) for direct SQL access.

## Key Differences from AGStream

| Feature | AGStream | AGStreamSQL |
|---------|----------|-------------|
| **Format** | JSON Schema | Avro |
| **Message Structure** | Full envelope with metadata | State data only |
| **Flink SQL** | Not compatible | Fully compatible |
| **Use Case** | Full agentic workflows | SQL-queryable data streams |
| **Schema Registry** | JSON Schema type | AVRO type |

## Installation

AGStreamSQL requires additional dependencies:

```bash
# Option 1: Confluent Kafka (recommended)
pip install confluent-kafka[avro]

# Option 2: FastAvro (fallback)
pip install fastavro
```

## Basic Usage

### 1. Define Your Data Model

```python
from pydantic import BaseModel

class Question(BaseModel):
    text: str
    timestamp: int
    category: str = "general"
```

### 2. Create AGStreamSQL Instance

```python
from agentics.core import AGStreamSQL

stream = AGStreamSQL(
    atype=Question,
    topic="questions",
    kafka_server="localhost:9092",
    schema_registry_url="http://localhost:8081",
    auto_create_topic=True,
    num_partitions=3
)
```

### 3. Produce Data

```python
questions = [
    Question(text="What is AI?", timestamp=123456, category="AI"),
    Question(text="How does ML work?", timestamp=123457, category="ML"),
]

message_ids = stream.produce(questions)
```

### 4. Query with Flink SQL

```bash
# Start Flink SQL client
cd tools/agstream_manager/scripts && ./flink_sql.sh
```

```sql
-- Create table
CREATE TABLE questions (
  text STRING,
  timestamp BIGINT,
  category STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'questions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);

-- Query individual fields
SELECT text, category FROM questions WHERE timestamp > 123456;

-- Aggregations
SELECT category, COUNT(*) as count FROM questions GROUP BY category;

-- Filtering
SELECT * FROM questions WHERE category = 'AI' ORDER BY timestamp DESC;
```

### 5. Consume Data (Optional)

```python
# Consume back from Kafka
questions = stream.consume(limit=10, from_beginning=True)

for q in questions:
    print(f"{q.text} - {q.category}")
```

## Complete Example

See `examples/agstream_sql_example.py` for a complete working example.

## Message Format Comparison

### AGStream (JSON Schema with Envelope)

```json
{
  "atype_name": "Question",
  "states": [
    {"text": "What is AI?", "timestamp": 123456, "category": "AI"}
  ],
  "instructions": "...",
  "transduce_fields": [...],
  ...
}
```

**Flink SQL Query:**
```sql
-- Complex: need to unnest the states array
SELECT s.text FROM questions
CROSS JOIN UNNEST(questions.states) AS s(text, timestamp, category);
```

### AGStreamSQL (Avro, State Only)

```json
{
  "text": "What is AI?",
  "timestamp": 123456,
  "category": "AI"
}
```

**Flink SQL Query:**
```sql
-- Simple: direct field access
SELECT text, timestamp, category FROM questions WHERE category = 'AI';
```

## Advanced Features

### Custom Partitioning

```python
stream = AGStreamSQL(
    atype=MyType,
    topic="my_topic",
    num_partitions=10  # More partitions for higher throughput
)
```

### Schema Evolution

Avro supports schema evolution. You can add optional fields:

```python
class QuestionV2(BaseModel):
    text: str
    timestamp: int
    category: str = "general"
    priority: int = 0  # New optional field
```

Old messages will still be readable with `priority` defaulting to 0.

### Joining Multiple Streams

```sql
-- Join questions and answers
CREATE TABLE questions (
  text STRING,
  timestamp BIGINT,
  id STRING
) WITH (...);

CREATE TABLE answers (
  text STRING,
  question_id STRING,
  timestamp BIGINT
) WITH (...);

SELECT
  q.text as question,
  a.text as answer
FROM questions q
JOIN answers a ON q.id = a.question_id;
```

## When to Use AGStreamSQL vs AGStream

### Use AGStreamSQL when:
- ✅ You need to query data with SQL
- ✅ You want direct field access in Flink
- ✅ You're building data pipelines
- ✅ You need to join multiple streams
- ✅ You want better performance (Avro is binary)

### Use AGStream when:
- ✅ You need full agentic workflows
- ✅ You want to preserve all metadata
- ✅ You're using transducible functions
- ✅ You need the complete AGStream API
- ✅ You're working with existing AGStream code

## Migration from AGStream

If you have existing AGStream code and want SQL compatibility:

```python
# Old AGStream code
from agentics.core.streaming import AGStream

ag = AGStream(
    atype=Question,
    input_topic="questions",
    output_topic="answers",
    ...
)
ag.produce_with_schema_enforcement()

# New AGStreamSQL code
from agentics.core import AGStreamSQL

stream = AGStreamSQL(
    atype=Question,
    topic="questions",
    ...
)
stream.produce(questions)
```

## Troubleshooting

### "confluent_kafka not found"

Install the Confluent Kafka library:
```bash
pip install confluent-kafka[avro]
```

### "Schema not found in registry"

The schema is automatically registered on first use. If you see this error:
1. Check Schema Registry is running: `curl http://localhost:8081/subjects`
2. Verify the topic name matches
3. Check network connectivity

### "Cannot query fields in Flink SQL"

Make sure you're using `'format' = 'avro-confluent'` in your CREATE TABLE statement, not `'format' = 'json'`.

## Performance Tips

1. **Use more partitions** for high-throughput topics
2. **Batch produce** multiple states at once
3. **Use Confluent Kafka** library (faster than fastavro)
4. **Set appropriate consumer timeouts** for your use case

## API Reference

### AGStreamSQL

```python
class AGStreamSQL:
    def __init__(
        self,
        atype: Type[BaseModel],
        topic: str,
        kafka_server: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
        auto_create_topic: bool = True,
        num_partitions: int = 1,
    )

    def produce(self, states: List[BaseModel]) -> List[str]

    def consume(
        self,
        limit: Optional[int] = None,
        timeout_ms: int = 5000,
        from_beginning: bool = True
    ) -> List[BaseModel]
```

## See Also

- [Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Schema Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)

---

Made with Bob
