# AGStream Utilities UDFs Guide

Utility functions for interacting with the AGStream environment and Schema Registry.

## Overview

The `agstream_utilities.py` module provides UDFs for:
- Listing all registered types in the Schema Registry
- Retrieving schema definitions for specific types
- Extracting field information from schemas
- Checking if types exist
- Getting Schema Registry connection info

## Installation

```bash
cd tools/agstream_manager
./scripts/install_udfs.sh
```

## Available UDFs

### 1. `list_registered_types()`

Lists all registered types (subjects) in the Schema Registry.

**Returns:** JSON array of type names

**Example:**
```sql
-- Register the function
CREATE TEMPORARY SYSTEM FUNCTION list_registered_types
AS 'agstream_utilities.list_registered_types'
LANGUAGE PYTHON;

-- Use it
SELECT list_registered_types();
```

**Output:**
```json
["Question", "Answer", "SentimentOutput", "UserProfile"]
```

### 2. `get_type_schema(type_name)`

Retrieves the complete schema definition for a specific type.

**Parameters:**
- `type_name` (STRING): Name of the type to retrieve

**Returns:** JSON object with schema details

**Example:**
```sql
-- Register the function
CREATE TEMPORARY SYSTEM FUNCTION get_type_schema
AS 'agstream_utilities.get_type_schema'
LANGUAGE PYTHON;

-- Get schema for a specific type
SELECT get_type_schema('Question');
```

**Output:**
```json
{
  "subject": "Question-value",
  "version": 1,
  "id": 123,
  "schema": {
    "type": "record",
    "name": "Question",
    "fields": [
      {"name": "text", "type": "string"},
      {"name": "timestamp", "type": "long"}
    ]
  }
}
```

### 3. `get_type_fields(type_name)`

Extracts just the field names and types from a schema.

**Parameters:**
- `type_name` (STRING): Name of the type

**Returns:** JSON object mapping field names to types

**Example:**
```sql
-- Register the function
CREATE TEMPORARY SYSTEM FUNCTION get_type_fields
AS 'agstream_utilities.get_type_fields'
LANGUAGE PYTHON;

-- Get fields for a type
SELECT get_type_fields('Question');
```

**Output:**
```json
{
  "text": "string",
  "timestamp": "long"
}
```

### 4. `type_exists(type_name)`

Checks if a type is registered in the Schema Registry.

**Parameters:**
- `type_name` (STRING): Name of the type to check

**Returns:** BOOLEAN (true/false)

**Example:**
```sql
-- Register the function
CREATE TEMPORARY SYSTEM FUNCTION type_exists
AS 'agstream_utilities.type_exists'
LANGUAGE PYTHON;

-- Check if type exists
SELECT type_exists('Question');  -- Returns: true
SELECT type_exists('NonExistent');  -- Returns: false
```

### 5. `schema_registry_info()`

Gets information about the Schema Registry connection.

**Returns:** JSON object with connection details

**Example:**
```sql
-- Register the function
CREATE TEMPORARY SYSTEM FUNCTION schema_registry_info
AS 'agstream_utilities.schema_registry_info'
LANGUAGE PYTHON;

-- Get registry info
SELECT schema_registry_info();
```

**Output:**
```json
{
  "url": "http://localhost:8081",
  "status": "connected",
  "subjects_count": 5,
  "subjects": ["Question-value", "Answer-value", ...]
}
```

## Complete Usage Example

### Register All Functions

```sql
-- Schema Registry utilities
CREATE TEMPORARY SYSTEM FUNCTION list_registered_types
AS 'agstream_utilities.list_registered_types' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION get_type_schema
AS 'agstream_utilities.get_type_schema' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION get_type_fields
AS 'agstream_utilities.get_type_fields' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION type_exists
AS 'agstream_utilities.type_exists' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION schema_registry_info
AS 'agstream_utilities.schema_registry_info' LANGUAGE PYTHON;
```

### Explore Available Types

```sql
-- 1. Check Schema Registry connection
SELECT schema_registry_info();

-- 2. List all registered types
SELECT list_registered_types();

-- 3. Get schema for a specific type
SELECT get_type_schema('Question');

-- 4. Get just the fields
SELECT get_type_fields('Question');

-- 5. Check if a type exists before using it
SELECT
    CASE
        WHEN type_exists('Question') THEN 'Type exists'
        ELSE 'Type not found'
    END as status;
```

### Dynamic Type Validation

```sql
-- Validate types before processing
SELECT
    topic_name,
    type_name,
    type_exists(type_name) as is_valid
FROM (
    VALUES
        ('questions', 'Question'),
        ('answers', 'Answer'),
        ('invalid', 'NonExistent')
) AS t(topic_name, type_name);
```

**Output:**
```
topic_name  | type_name    | is_valid
------------|--------------|----------
questions   | Question     | true
answers     | Answer       | true
invalid     | NonExistent  | false
```

## Use Cases

### 1. Schema Discovery

Discover what types are available in your AGStream environment:

```sql
-- List all types
SELECT list_registered_types();

-- Get details for each type
SELECT
    type_name,
    get_type_fields(type_name) as fields
FROM (
    SELECT UNNEST(CAST(list_registered_types() AS ARRAY<STRING>)) as type_name
);
```

### 2. Type Validation

Validate that required types exist before starting a job:

```sql
-- Check if all required types exist
SELECT
    'Question' as type_name,
    type_exists('Question') as exists
UNION ALL
SELECT
    'Answer' as type_name,
    type_exists('Answer') as exists;
```

### 3. Schema Inspection

Inspect schema details for debugging:

```sql
-- Get full schema with metadata
SELECT get_type_schema('Question');

-- Get just field definitions
SELECT get_type_fields('Question');
```

### 4. Dynamic Schema-Based Processing

Use schema information to drive processing logic:

```sql
-- Example: Process only if type has required fields
SELECT
    type_name,
    get_type_fields(type_name) as fields,
    CASE
        WHEN get_type_fields(type_name) LIKE '%text%'
        THEN 'Has text field'
        ELSE 'No text field'
    END as validation
FROM (VALUES ('Question'), ('Answer')) AS t(type_name);
```

## Configuration

The UDFs use the `SCHEMA_REGISTRY_URL` environment variable:

```bash
# In .env file
SCHEMA_REGISTRY_URL=http://localhost:8081
```

Default: `http://localhost:8081`

## Testing

### Standalone Test

```bash
cd tools/agstream_manager/udfs
python agstream_utilities.py
```

This will run built-in tests that:
1. Check Schema Registry connection
2. List all registered types
3. Get schema for first available type
4. Extract fields from schema
5. Test type existence checks

### Flink SQL Test

```sql
-- 1. Register functions (see above)

-- 2. Test each function
SELECT schema_registry_info();
SELECT list_registered_types();
SELECT get_type_schema('Question');
SELECT get_type_fields('Question');
SELECT type_exists('Question');
```

## Error Handling

All UDFs return JSON with error information if something goes wrong:

```json
{
  "error": "Failed to connect to Schema Registry: Connection refused"
}
```

**Common errors:**
- Schema Registry not running
- Type not found
- Network connectivity issues
- Invalid JSON in schema

## Troubleshooting

### Schema Registry Not Accessible

```bash
# Check if Schema Registry is running
curl http://localhost:8081/subjects

# Check environment variable
docker exec flink-taskmanager cat /opt/flink/.env | grep SCHEMA_REGISTRY_URL
```

### UDF Not Found

```bash
# Reinstall UDFs
cd tools/agstream_manager
./scripts/install_udfs.sh
```

### Connection Timeout

Increase timeout in the code or check network connectivity:

```bash
# Test connectivity from Flink container
docker exec flink-taskmanager curl http://localhost:8081/subjects
```

## Integration with Other UDFs

Combine with semantic operators for powerful workflows:

```sql
-- Example: Analyze sentiment only for types with 'text' field
SELECT
    type_name,
    CASE
        WHEN get_type_fields(type_name) LIKE '%text%'
        THEN 'Can analyze sentiment'
        ELSE 'No text field'
    END as sentiment_capable
FROM (
    SELECT UNNEST(CAST(list_registered_types() AS ARRAY<STRING>)) as type_name
);
```

## Related Documentation

- [UDF Guide](UDF_GUIDE.md) - Complete UDF documentation
- [AGStream Manager Guide](../docs/AGSTREAM_MANAGER_GUIDE.md) - Full system guide
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)

## Quick Reference

```sql
-- List all types
SELECT list_registered_types();

-- Get schema
SELECT get_type_schema('TypeName');

-- Get fields only
SELECT get_type_fields('TypeName');

-- Check existence
SELECT type_exists('TypeName');

-- Registry info
SELECT schema_registry_info();
