# Flink SQL Client Usage Guide

## Quick Start

### Option 1: Auto-Load Tables (Recommended) ⭐
The easiest way - tables are automatically loaded when you start:

```bash
cd tools/agstream_manager/scripts
./flink_sql_with_tables.sh
```

This script will:
1. Check if Flink is running
2. Generate CREATE TABLE statements for all AGStream channels
3. Auto-load them using Flink's `-i` initialization flag
4. Open the SQL client with tables ready to query

**Just run and start querying immediately!**

### Option 2: Manual Copy/Paste
If you want to see the SQL first:

```bash
cd tools/agstream_manager/scripts

# Show SQL to copy
./show_table_sql.sh

# Open SQL client
./flink_sql.sh

# Paste the SQL, then query
```

### Option 3: Basic SQL Client
Open without pre-loaded tables:

```bash
cd tools/agstream_manager/scripts
./flink_sql.sh
```

Then manually create tables as needed.

## Common SQL Commands

### Show Available Tables
```sql
SHOW TABLES;
```

### Describe Table Schema
```sql
DESCRIBE Questions;
```

### Query a Table
```sql
-- Get latest 10 records
SELECT * FROM Questions LIMIT 10;

-- Filter by field
SELECT * FROM Questions WHERE category = 'science' LIMIT 10;

-- Count records
SELECT COUNT(*) FROM Questions;

-- Group by field
SELECT category, COUNT(*) as count
FROM Questions
GROUP BY category;
```

### Exit the Client
```sql
QUIT;
```
or
```sql
EXIT;
```

## Troubleshooting

### Tables Not Found
If you get "Table not found" errors, you need to register the tables first:

1. Exit the SQL client (type `QUIT;`)
2. Run `./flink_sql_auto.sh` and choose 'y' to auto-register
3. Or manually run the CREATE TABLE statements from `./init_flink_tables.py`

### Flink Not Running
If you get "Flink JobManager container is not running":

```bash
cd /Users/gliozzo/Code/agentics911/agentics
docker compose -f docker-compose-karapace-flink.yml up -d
```

### Avro Format Errors
The containerized Flink SQL client has all required JARs pre-loaded, so Avro format should work out of the box. If you still get errors, check that:

1. Schema Registry is running: `curl http://localhost:8081/subjects`
2. Topics have Avro schemas registered
3. The topic name in CREATE TABLE matches the actual Kafka topic name

## Example Session

```sql
-- List all tables
Flink SQL> SHOW TABLES;
+---------------+
|    table name |
+---------------+
|   answers_sql |
|     Questions |
| questions_sql |
+---------------+

-- Check schema
Flink SQL> DESCRIBE Questions;
+----------+--------+------+-----+--------+-----------+
|     name |   type | null | key | extras | watermark |
+----------+--------+------+-----+--------+-----------+
|     text | STRING | TRUE |     |        |           |
| category | STRING | TRUE |     |        |           |
+----------+--------+------+-----+--------+-----------+

-- Query data
Flink SQL> SELECT * FROM Questions LIMIT 5;
+--------------------------------+----------+
|                           text | category |
+--------------------------------+----------+
| What is the capital of France? |   travel |
| How does photosynthesis work?  |  science |
+--------------------------------+----------+

-- Exit
Flink SQL> QUIT;
```

## Tips

1. **Case Sensitivity**: Table names are case-sensitive. Use the exact name shown in `SHOW TABLES;`

2. **Streaming Mode**: Queries run in streaming mode by default. Use `LIMIT` to avoid infinite results.

3. **Performance**: For large topics, queries may take time to process. Be patient or use more specific filters.

4. **Real-time Updates**: The SQL client shows data as it arrives. New messages will appear in query results.

5. **Multiple Sessions**: You can open multiple SQL client sessions in different terminals.

## Advanced: Manual Table Creation

If you need to manually create a table:

```sql
CREATE TABLE my_topic (
  field1 STRING,
  field2 BIGINT,
  field3 DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'my_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);
```

## Why Use Containerized Flink SQL Client?

The containerized Flink SQL client (accessed via `flink_sql_auto.sh` or `flink_sql.sh`) is more reliable than PyFlink because:

1. ✅ **Pre-configured**: All JARs (Kafka connector, Avro format) are already loaded
2. ✅ **Version Compatibility**: No Python/Java version conflicts
3. ✅ **Official Tool**: Uses Apache Flink's official SQL client
4. ✅ **Better Performance**: Runs directly in the Flink cluster
5. ✅ **Stable**: No PyFlink compatibility issues

The PyFlink approach (`flink_sql_shell.py`) can have Java class loading issues due to version mismatches between PyFlink and Flink JARs.
