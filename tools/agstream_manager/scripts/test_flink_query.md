# Test Flink SQL Query

The topic `questions_sql` now has 3 clean Avro messages:

```
Partition: 2 | Offset: 0 - "What is artificial intelligence?" (AI)
Partition: 2 | Offset: 1 - "How does machine learning work?" (ML)
Partition: 0 | Offset: 0 - "What is deep learning?" (DL)
```

## To Query with Flink SQL:

### 1. Start Flink SQL Client
```bash
cd tools/agstream_manager/scripts && ./flink_sql.sh
```

### 2. Create Table
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

**Important Notes**:
- Use backticks around `timestamp` (reserved keyword in Flink SQL)
- Use `kafka:29092` (internal Docker network port, not 9092)
- Use `karapace-schema-registry:8081` (Schema Registry hostname)

### 3. Query Data
```sql
SELECT * FROM questions_sql;
```

**Expected Result:**
```
+----------------------------------+------------+----------+
| text                             | timestamp  | category |
+----------------------------------+------------+----------+
| What is artificial intelligence? | 1234567890 | AI       |
| How does machine learning work?  | 1234567891 | ML       |
| What is deep learning?           | 1234567892 | DL       |
+----------------------------------+------------+----------+
```

### 4. Filter by Category
```sql
SELECT text, category FROM questions_sql WHERE category = 'AI';
```

**Expected Result:**
```
+----------------------------------+----------+
| text                             | category |
+----------------------------------+----------+
| What is artificial intelligence? | AI       |
+----------------------------------+----------+
```

## If Query Returns Empty

1. **Check table exists**: `SHOW TABLES;`
2. **Drop and recreate**: `DROP TABLE questions_sql;` then recreate
3. **Verify scan mode**: Ensure `'scan.startup.mode' = 'earliest-offset'`
4. **Check Kafka connectivity**: Ensure `kafka:9092` is accessible from Flink container

## Success Indicators

✅ Topic has 3 messages (verified above)
✅ All messages are Avro-formatted
✅ Schema registered in Schema Registry (ID: 5)
✅ No old non-Avro messages

You should now see results when querying!
