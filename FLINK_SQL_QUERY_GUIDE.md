# Flink SQL Query Guide for AGStreamSQL

This guide shows how to query AGStreamSQL topics using Flink SQL.

## Prerequisites

1. **Clean Topics**: Run the cleanup script first to remove old non-Avro messages:
   ```bash
   python tools/agstream_manager/scripts/clean_sql_topics.py
   ```

2. **Produce Data**: Run the example to create fresh Avro messages:
   ```bash
   python examples/agstream_sql_example.py
   ```

3. **Start Flink SQL Client**:
   ```bash
   cd tools/agstream_manager/scripts && ./flink_sql.sh
   ```

## Create Tables

### Questions Table
```sql
CREATE TABLE questions_sql (
  text STRING,
  `timestamp` BIGINT,
  category STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'questions_sql',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);
```

### Answers Table
```sql
CREATE TABLE answers_sql (
  text STRING,
  question_id STRING,
  `timestamp` BIGINT,
  confidence DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'answers_sql',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);
```

**Note**: Use `kafka:29092` (internal Docker port) and `karapace-schema-registry:8081` (Schema Registry hostname).

## Query Examples

### 1. Get All Questions
```sql
SELECT * FROM questions_sql;
```

### 2. Filter by Category
```sql
SELECT text, `timestamp`
FROM questions_sql
WHERE category = 'AI';
```

### 3. Get High-Confidence Answers
```sql
SELECT text, confidence
FROM answers_sql
WHERE confidence > 0.9;
```

### 4. Join Questions and Answers
```sql
SELECT
  q.text as question,
  a.text as answer,
  a.confidence
FROM questions_sql q
JOIN answers_sql a ON q.`timestamp` < a.`timestamp`;
```

### 5. Count by Category
```sql
SELECT category, COUNT(*) as count
FROM questions_sql
GROUP BY category;
```

## Troubleshooting

### Empty Results

If `SELECT * FROM questions_sql LIMIT 10;` returns empty results:

1. **Check if data exists**:
   ```bash
   python tools/agstream_manager/scripts/verify_avro_messages.py questions_sql
   ```

2. **Clean and recreate**:
   ```bash
   # Clean old topics
   python tools/agstream_manager/scripts/clean_sql_topics.py

   # Produce fresh data
   python examples/agstream_sql_example.py
   ```

3. **Verify in Flink SQL**:
   ```sql
   -- Drop and recreate table
   DROP TABLE IF EXISTS questions_sql;

   -- Recreate with earliest-offset
   CREATE TABLE questions_sql (...) WITH (
     ...
     'scan.startup.mode' = 'earliest-offset',
     ...
   );

   -- Query again
   SELECT * FROM questions_sql;
   ```

### Deserialization Errors

If you see "message does not start with magic byte":
- Old non-Avro messages exist in the topic
- Solution: Run `python tools/agstream_manager/scripts/clean_sql_topics.py`

### Connection Refused

If you see connection errors:
- Kafka/Schema Registry not running
- Solution: Start services with `./manage_services.sh start`

## Key Differences: AGStream vs AGStreamSQL

| Feature | AGStream | AGStreamSQL |
|---------|----------|-------------|
| Format | JSON Schema | Avro |
| Message Structure | Envelope + State | State only |
| SQL Queries | Complex (nested) | Direct field access |
| Use Case | General streaming | Flink SQL analytics |

## Notes

- **Avro Format**: AGStreamSQL uses Avro for better Flink SQL compatibility
- **No Envelope**: Messages contain only state data for direct SQL access
- **Schema Registry**: Schemas are automatically registered
- **Clean Topics**: Always start with clean topics for best results
