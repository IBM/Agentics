# Complete UDF Guide for AGStream

This comprehensive guide covers all User-Defined Functions (UDFs) available in AGStream, including both core functions and auto-generated registry functions.

## Table of Contents

1. [Overview](#overview)
2. [Core UDFs](#core-udfs)
   - [agmap - Dynamic Mapping](#agmap---dynamic-mapping)
   - [agreduce - Aggregation](#agreduce---aggregation)
3. [Registry UDFs](#registry-udfs)
   - [agmap_registry - Auto-generated UDTFs](#agmap_registry---auto-generated-udtfs)
   - [agreduce_registry - Auto-generated UDAFs](#agreduce_registry---auto-generated-udafs)
4. [Quick Reference](#quick-reference)
5. [Troubleshooting](#troubleshooting)
6. [Best Practices](#best-practices)

---

## Overview

AGStream provides two types of UDFs:

1. **Core UDFs**: Manually defined functions with dynamic type support
   - `agmap`: Row-by-row transformation (UDTF)
   - `agreduce`: Aggregation across all rows (UDAF)

2. **Registry UDFs**: Auto-generated functions from Schema Registry
   - `agmap_<schema>`: Schema-specific row transformations
   - `agreduce_<schema>`: Schema-specific aggregations

---

## Core UDFs

### agmap - Dynamic Mapping

**Type:** UDTF (User-Defined Table Function)
**Purpose:** Transform each input row into output rows with dynamic target types

#### Registration

```sql
CREATE TEMPORARY SYSTEM FUNCTION agmap
AS 'ag_operators.agmap' LANGUAGE PYTHON;
```

#### Syntax

```sql
SELECT T.field1, T.field2, ...
FROM source_table,
LATERAL TABLE(agmap(
    input_column,
    'TargetTypeName',
    'field1:type1,field2:type2,...'
)) AS T(field1, field2, ...);
```

#### Parameters

- `input_column`: Source data column
- `target_type_name`: Name of the target Pydantic model
- `field_definitions`: Comma-separated field definitions (name:type)

#### Supported Types

- `str` - String
- `int` - Integer
- `float` - Float/Double
- `bool` - Boolean

#### Examples

**Example 1: Sentiment Analysis**
```sql
SELECT
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'Sentiment',
    'sentiment_label:str,sentiment_score:float'
)) AS T(sentiment_label, sentiment_score);
```

**Example 2: Product Classification**
```sql
SELECT
    T.category,
    T.subcategory,
    T.confidence
FROM products,
LATERAL TABLE(agmap(
    product_description,
    'ProductCategory',
    'category:str,subcategory:str,confidence:float'
)) AS T(category, subcategory, confidence);
```

**Example 3: Entity Extraction**
```sql
SELECT
    T.person_name,
    T.organization,
    T.location
FROM documents,
LATERAL TABLE(agmap(
    text_content,
    'Entities',
    'person_name:str,organization:str,location:str'
)) AS T(person_name, organization, location);
```

---

### agreduce - Aggregation

**Type:** UDAF (User-Defined Aggregate Function)
**Purpose:** Aggregate multiple rows into a single result with dynamic target types

#### Registration

```sql
CREATE TEMPORARY SYSTEM FUNCTION agreduce
AS 'agreduce.agreduce' LANGUAGE PYTHON;
```

#### Syntax

```sql
WITH aggregated AS (
    SELECT agreduce(
        input_column,
        'TargetTypeName',
        'field1:type1,field2:type2,...'
    ) as agg_result
    FROM source_table
)
SELECT
    JSON_VALUE(agg_result, '$.field1') as field1,
    JSON_VALUE(agg_result, '$.field2') as field2
FROM aggregated;
```

#### Parameters

- `input_column`: Source data column
- `target_type_name`: Name of the target Pydantic model
- `field_definitions`: Comma-separated field definitions (name:type)

#### Output Format

Returns a JSON string containing all fields. Use `JSON_VALUE()` to extract specific fields.

#### Examples

**Example 1: Overall Sentiment**
```sql
WITH sentiment_agg AS (
    SELECT agreduce(
        customer_review,
        'OverallSentiment',
        'sentiment_label:str,sentiment_score:float,review_count:int'
    ) as agg_result
    FROM pr
)
SELECT
    JSON_VALUE(agg_result, '$.sentiment_label') as overall_sentiment,
    JSON_VALUE(agg_result, '$.sentiment_score') as overall_score,
    JSON_VALUE(agg_result, '$.review_count') as total_reviews
FROM sentiment_agg;
```

**Example 2: Product Summary**
```sql
WITH product_summary AS (
    SELECT agreduce(
        customer_review,
        'ProductSummary',
        'summary:str,key_features:str,rating:float'
    ) as agg_result
    FROM pr
    WHERE Product_name = 'Laptop'
)
SELECT
    JSON_VALUE(agg_result, '$.summary') as product_summary,
    JSON_VALUE(agg_result, '$.key_features') as features,
    JSON_VALUE(agg_result, '$.rating') as avg_rating
FROM product_summary;
```

**Example 3: Time-Windowed Aggregation**
```sql
WITH hourly_summary AS (
    SELECT
        TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
        agreduce(
            customer_review,
            'HourlySummary',
            'summary:str,sentiment:str'
        ) as agg_result
    FROM pr
    GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
)
SELECT
    window_start,
    JSON_VALUE(agg_result, '$.summary') as hourly_summary,
    JSON_VALUE(agg_result, '$.sentiment') as hourly_sentiment
FROM hourly_summary;
```

---

## Registry UDFs

Registry UDFs are automatically generated from schemas registered in the Schema Registry. They provide type-safe, schema-aware transformations without manual type definitions.

### Generating Registry UDFs

```bash
cd tools/agstream_manager
python scripts/generate_registry_udfs.py
```

This generates:
- `udfs/agmap_registry.py` - UDTFs for row-by-row mapping
- `udfs/agreduce_registry.py` - UDAFs for aggregation

### Deploying Registry UDFs

After generation, rebuild and restart:

```bash
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart_services
```

### agmap_registry - Auto-generated UDTFs

**Purpose:** Schema-specific row-by-row transformations

#### Registration

```sql
-- Register all at once
\i sql/register_registry_udfs.sql

-- Or register individually
CREATE TEMPORARY SYSTEM FUNCTION agmap_sentiment
AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;
```

#### Available Functions

Based on Schema Registry schemas:
- `agmap_sentiment` - Sentiment analysis
- `agmap_productreview` - Product review transformation
- `agmap_pr` - PR record transformation
- `agmap_s` - S record transformation
- `agmap_test_generation` - Test data transformation
- `agmap_electronics_reviews` - Electronics review transformation
- `agmap_product_reviews` - Product review transformation

#### Usage

```sql
SELECT T.sentiment_label, T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review))
AS T(sentiment_label, sentiment_score);
```

#### Advantages

- ✅ No manual type definitions needed
- ✅ Schema validation built-in
- ✅ Automatically updated when schemas change
- ✅ Type-safe transformations

---

### agreduce_registry - Auto-generated UDAFs

**Purpose:** Schema-specific aggregation across all rows

#### Registration

```sql
-- Register all at once
\i sql/register_registry_udfs.sql

-- Or register individually
CREATE TEMPORARY SYSTEM FUNCTION agreduce_sentiment
AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;
```

#### Available Functions

Based on Schema Registry schemas:
- `agreduce_sentiment` - Aggregate sentiment analysis
- `agreduce_productreview` - Aggregate product reviews
- `agreduce_pr` - Aggregate PR records
- `agreduce_s` - Aggregate S records
- `agreduce_test_generation` - Aggregate test data
- `agreduce_electronics_reviews` - Aggregate electronics reviews
- `agreduce_product_reviews` - Aggregate product reviews

#### Usage

```sql
WITH sentiment_agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT
    JSON_VALUE(agg_result, '$.sentiment_label') as overall_sentiment,
    JSON_VALUE(agg_result, '$.sentiment_score') as overall_score
FROM sentiment_agg;
```

#### Output Format

All agreduce registry functions return JSON strings. Use `JSON_VALUE()` to extract fields:

```sql
JSON_VALUE(agg_result, '$.field_name')
```

#### Advantages

- ✅ Semantic aggregation using areduce
- ✅ Schema-aware output
- ✅ Parallel processing support
- ✅ No manual type definitions

---

## Quick Reference

### Function Comparison

| Feature | agmap | agreduce | agmap_registry | agreduce_registry |
|---------|-------|----------|----------------|-------------------|
| **Type** | UDTF | UDAF | UDTF | UDAF |
| **Input** | One row | All rows | One row | All rows |
| **Output** | Multiple rows | Single JSON | Multiple rows | Single JSON |
| **Type Definition** | Manual | Manual | Auto (Schema) | Auto (Schema) |
| **Use Case** | Row transformation | Aggregation | Schema-based transform | Schema-based aggregation |

### Registration Commands

```sql
-- Core UDFs
CREATE TEMPORARY SYSTEM FUNCTION agmap AS 'ag_operators.agmap' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION agreduce AS 'agreduce.agreduce' LANGUAGE PYTHON;

-- Registry UDFs (example)
CREATE TEMPORARY SYSTEM FUNCTION agmap_sentiment AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION agreduce_sentiment AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;
```

### Common Patterns

**Pattern 1: Transform then Aggregate**
```sql
-- First transform each row
WITH transformed AS (
    SELECT T.sentiment_label, T.sentiment_score
    FROM pr,
    LATERAL TABLE(agmap_sentiment(customer_review))
    AS T(sentiment_label, sentiment_score)
)
-- Then aggregate
SELECT
    sentiment_label,
    AVG(sentiment_score) as avg_score,
    COUNT(*) as count
FROM transformed
GROUP BY sentiment_label;
```

**Pattern 2: Direct Aggregation**
```sql
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT
    JSON_VALUE(agg_result, '$.sentiment_label') as overall_sentiment
FROM agg;
```

**Pattern 3: Windowed Aggregation**
```sql
WITH windowed AS (
    SELECT
        TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
        agreduce_sentiment(customer_review) as agg_result
    FROM pr
    GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
)
SELECT
    window_start,
    JSON_VALUE(agg_result, '$.sentiment_label') as hourly_sentiment
FROM windowed;
```

---

## Troubleshooting

### Common Issues

#### 1. ModuleNotFoundError

**Error:** `ModuleNotFoundError: No module named 'agreduce_registry'`

**Solution:**
```bash
# Rebuild Flink image
./scripts/rebuild_flink_image.sh

# Restart services
./manage_services_full.sh restart_services
```

#### 2. ImportError: libgomp.so.1

**Error:** `ImportError: libgomp.so.1: cannot open shared object file`

**Solution:**
```bash
# Run the fix script
./scripts/fix_agreduce.sh
```

#### 3. Function Already Exists

**Error:** `A function named 'agreduce_sentiment' does already exist`

**Solution:** Function is already registered, just use it. Or drop and recreate:
```sql
DROP TEMPORARY SYSTEM FUNCTION IF EXISTS agreduce_sentiment;
CREATE TEMPORARY SYSTEM FUNCTION agreduce_sentiment
AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;
```

#### 4. ParseException with 'result'

**Error:** `Encountered "result" at line X`

**Solution:** `result` is a reserved keyword. Use a different alias:
```sql
-- ❌ Wrong
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as result
    FROM pr
)
SELECT JSON_VALUE(result, '$.sentiment_label') FROM agg;

-- ✅ Correct
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT JSON_VALUE(agg_result, '$.sentiment_label') FROM agg;
```

#### 5. Empty or NULL Results

**Causes:**
- No data in source table
- Input column contains NULLs
- Function not properly registered

**Solutions:**
```sql
-- Check data exists
SELECT COUNT(*) FROM pr;

-- Check for NULLs
SELECT COUNT(*) FROM pr WHERE customer_review IS NOT NULL;

-- Verify function registration
SHOW FUNCTIONS;
```

#### 6. JSON_VALUE Returns NULL

**Cause:** Field name doesn't match schema or JSON structure

**Solution:**
```sql
-- Check the actual JSON structure first
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT agg_result FROM agg;

-- Then extract with correct field names
SELECT JSON_VALUE(agg_result, '$.sentiment_label') FROM agg;
```

---

## Best Practices

### 1. Use WITH Clauses (CTEs)

✅ **Recommended:**
```sql
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT JSON_VALUE(agg_result, '$.sentiment_label') FROM agg;
```

❌ **Avoid:** Subqueries in FROM clause (not supported in Flink SQL)

### 2. Avoid Reserved Keywords

❌ **Don't use:** `result`, `value`, `data` as column aliases
✅ **Use instead:** `agg_result`, `output_value`, `transformed_data`

### 3. Filter Before Aggregation

✅ **Efficient:**
```sql
WITH filtered AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
    WHERE customer_review IS NOT NULL
      AND LENGTH(customer_review) > 10
)
SELECT JSON_VALUE(agg_result, '$.sentiment_label') FROM filtered;
```

### 4. Use Registry UDFs When Possible

✅ **Preferred:** Schema-based functions
```sql
SELECT T.sentiment_label FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label);
```

⚠️ **When needed:** Dynamic types for ad-hoc analysis
```sql
SELECT T.custom_field FROM pr,
LATERAL TABLE(agmap(customer_review, 'Custom', 'custom_field:str'))
AS T(custom_field);
```

### 5. Handle NULLs Explicitly

```sql
-- Filter out NULLs before processing
WITH clean_data AS (
    SELECT customer_review
    FROM pr
    WHERE customer_review IS NOT NULL
)
SELECT T.sentiment_label
FROM clean_data,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label);
```

### 6. Use Appropriate Parallelism

```sql
-- Set parallelism for better performance
SET 'parallelism.default' = '4';

-- Then run your query
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT JSON_VALUE(agg_result, '$.sentiment_label') FROM agg;
```

### 7. Monitor Performance

```sql
-- Check job status in Flink Web UI
-- http://localhost:8081

-- Use EXPLAIN to understand query plan
EXPLAIN SELECT T.sentiment_label
FROM pr, LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label);
```

### 8. Regenerate Registry UDFs When Schemas Change

```bash
# After schema changes in Registry
cd tools/agstream_manager
python scripts/generate_registry_udfs.py
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart_services
```

---

## Additional Resources

- **SQL Examples:** `sql/agreduce_registry_examples.sql`
- **Registration Script:** `sql/register_registry_udfs.sql`
- **Generation Script:** `scripts/generate_registry_udfs.py`
- **Flink Web UI:** http://localhost:8081
- **Schema Registry:** http://localhost:8081

---

## Summary

This guide covers all UDFs in AGStream:

1. **Core UDFs** (`agmap`, `agreduce`) - Dynamic, flexible transformations
2. **Registry UDFs** (`agmap_*`, `agreduce_*`) - Schema-based, type-safe operations

Choose the right tool for your use case:
- Use **registry UDFs** for production workloads with stable schemas
- Use **core UDFs** for ad-hoc analysis and dynamic type exploration

All functions support semantic transformations powered by LLMs, enabling intelligent data processing at scale.
