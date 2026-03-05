# AGStreamSQL Quick Start Guide

## First Time Setup

### 1. Download Flink Avro Connector (One Time Only)
```bash
./tools/agstream_manager/scripts/download_flink_avro.sh
```

### 2. Restart Flink to Load JARs
```bash
./tools/agstream_manager/scripts/restart_flink.sh
```

## Regular Usage

### 1. Clean Topics (First Time or After Errors)
```bash
python tools/agstream_manager/scripts/clean_sql_topics.py
```

### 2. Produce Test Data
```bash
python examples/agstream_sql_example.py
```

### 3. Start Flink SQL Client
```bash
cd tools/agstream_manager/scripts && ./flink_sql.sh
```

### 4. Create Table (Copy & Paste into Flink SQL)
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

**⚠️ IMPORTANT**:
- Use `kafka:29092` (internal Docker network port)
- Use `http://karapace-schema-registry:8081` (Schema Registry hostname)

**⚠️ IMPORTANT**: Use backticks around `timestamp` - it's a reserved keyword!

### 5. Query Data
```sql
SELECT * FROM questions_sql;
```

**Expected Output:**
```
+----------------------------------+------------+----------+
| text                             | timestamp  | category |
+----------------------------------+------------+----------+
| What is artificial intelligence? | 1234567890 | AI       |
| How does machine learning work?  | 1234567891 | ML       |
| What is deep learning?           | 1234567892 | DL       |
+----------------------------------+------------+----------+
3 rows in set
```

## More Queries

### Filter by Category
```sql
SELECT text, `timestamp` FROM questions_sql WHERE category = 'AI';
```

### Count by Category
```sql
SELECT category, COUNT(*) as count FROM questions_sql GROUP BY category;
```

### Create Answers Table
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

### Join Questions and Answers
```sql
SELECT
  q.text as question,
  a.text as answer,
  a.confidence
FROM questions_sql q
JOIN answers_sql a ON q.`timestamp` < a.`timestamp`;
```

## Troubleshooting

### "Encountered 'timestamp' at line X" Error
**Problem**: `timestamp` is a reserved keyword in Flink SQL.
**Solution**: Use backticks: `` `timestamp` ``

### Empty Query Results
**Problem**: Old non-Avro messages in topic.
**Solution**:
```bash
python tools/agstream_manager/scripts/clean_sql_topics.py
python examples/agstream_sql_example.py
```

### "message does not start with magic byte"
**Problem**: Mixed Avro and non-Avro messages.
**Solution**: Clean and recreate topics (see above).

## Key Points

✅ Always use backticks for reserved keywords: `` `timestamp` ``
✅ Clean topics before first use
✅ Use `'scan.startup.mode' = 'earliest-offset'` to read all messages
✅ Avro format required for Flink SQL compatibility

## Files Reference

- **Example**: `examples/agstream_sql_example.py`
- **Cleanup**: `tools/agstream_manager/scripts/clean_sql_topics.py`
- **Verify**: `tools/agstream_manager/scripts/verify_avro_messages.py`
- **Full Guide**: `FLINK_SQL_QUERY_GUIDE.md`
