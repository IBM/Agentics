# AGStream UDFs - Complete Documentation

This comprehensive guide covers all User-Defined Functions (UDFs) available in AGStream, including core functions, registry functions, vector search, and build optimization.

## Table of Contents

1. [Overview](#overview)
2. [Core UDFs](#core-udfs)
   - [agmap - Dynamic Mapping](#agmap---dynamic-mapping)
   - [agreduce - Aggregation](#agreduce---aggregation)
3. [Registry UDFs](#registry-udfs)
   - [agmap_registry - Auto-generated UDTFs](#agmap_registry---auto-generated-udtfs)
   - [agreduce_registry - Auto-generated UDAFs](#agreduce_registry---auto-generated-udafs)
4. [Vector Search UDFs](#vector-search-udfs)
   - [agsearch - Non-Persistent Search](#agsearch---non-persistent-search)
   - [agpersist_search - Persistent Search](#agpersist_search---persistent-search)
5. [Build Optimization](#build-optimization)
6. [Quick Reference](#quick-reference)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## Overview

AGStream provides several types of UDFs:

1. **Core UDFs**: Manually defined functions with dynamic type support
   - `agmap`: Row-by-row transformation (UDTF)
   - `agreduce`: Aggregation across all rows (UDAF)

2. **Registry UDFs**: Auto-generated functions from Schema Registry
   - `agmap_<schema>`: Schema-specific row transformations
   - `agreduce_<schema>`: Schema-specific aggregations

3. **Vector Search UDFs**: Semantic search capabilities
   - `agsearch`: Non-persistent vector search (UDAF)
   - `build_search_index`: Build persistent search indexes (UDAF)
   - `search_persisted_index`: Search persistent indexes (UDTF)
   - `explode_search_results`: Expand search results into rows (UDTF)

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

**Sentiment Analysis:**
```sql
SELECT T.sentiment_label, T.sentiment_score
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'Sentiment',
    'sentiment_label:str,sentiment_score:float'
)) AS T(sentiment_label, sentiment_score);
```

**Product Classification:**
```sql
SELECT T.category, T.subcategory, T.confidence
FROM products,
LATERAL TABLE(agmap(
    product_description,
    'ProductCategory',
    'category:str,subcategory:str,confidence:float'
)) AS T(category, subcategory, confidence);
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

#### Output Format

Returns a JSON string containing all fields. Use `JSON_VALUE()` to extract specific fields.

#### Examples

**Overall Sentiment:**
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

---

## Vector Search UDFs

### agsearch - Non-Persistent Search

**Type:** UDAF (User-Defined Aggregate Function)
**Purpose:** Perform semantic vector search on accumulated table rows

#### Features

- Vector-based semantic search using sentence transformers
- Configurable result count (max_k parameter)
- JSON output format
- Thread-safe with thread-local AG instances
- Automatic index building for each query

#### Syntax

```sql
agsearch(input_data, query, max_k)
```

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `input_data` | STRING | Yes | - | The text content to search through |
| `query` | STRING | Yes | - | The search query |
| `max_k` | INT | No | 10 | Maximum number of results to return |

#### Returns

JSON array of search results:
```json
[
  {"text": "The actual text content", "index": 0},
  {"text": "Another matching text", "index": 5}
]
```

#### Registration

```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS agsearch
AS 'agsearch.agsearch' LANGUAGE PYTHON;
```

#### Examples

**Basic Search:**
```sql
SELECT agsearch(review_text, 'great service', 10) as search_results
FROM pr;
```

**With Time Windows:**
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
    agsearch(log_message, 'warning', 10) as warnings
FROM system_logs
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
```

#### Exploding Results

Use `explode_search_results` UDTF to convert JSON array into multiple rows:

```sql
-- Register the function
CREATE TEMPORARY FUNCTION IF NOT EXISTS explode_search_results
AS 'explode_search_results.explode_search_results' LANGUAGE PYTHON;

-- Use it
SELECT text, idx
FROM (
    SELECT agsearch(review_text, 'great service', 10) as results FROM pr
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);
```

---

### agpersist_search - Persistent Search

**Purpose:** Build and search persistent vector indexes that survive Flink sessions

#### Features

- **Persistent Storage**: Indexes saved to disk and reusable across sessions
- **Thread-Safe**: File locking ensures safe concurrent access
- **Versioning**: Metadata tracking for index versions
- **HNSW Algorithm**: Fast approximate nearest neighbor search
- **Sentence Transformers**: Uses `all-MiniLM-L6-v2` for embeddings (384 dimensions)
- **Flexible Storage**: Configurable storage location via environment variable

#### Functions

##### 1. build_search_index() - UDAF

Builds and persists a vector search index from table rows.

**Syntax:**
```sql
build_search_index(text_column, 'index_name')
```

**Parameters:**
- `row_data` (STRING): Text data or "text|metadata_json" format
- `index_name` (STRING): Name for the persistent index

**Returns:** JSON string with status
```json
{
  "status": "success",
  "index_name": "my_index",
  "num_items": 1000,
  "dimension": 384
}
```

**Examples:**
```sql
-- Basic index building
SELECT build_search_index(customer_review, 'reviews_index') as status
FROM pr;

-- Filtered index
SELECT build_search_index(customer_review, 'positive_reviews_index') as status
FROM pr
WHERE rating >= 4;
```

##### 2. search_persisted_index() - UDTF

Searches a persisted vector index.

**Syntax:**
```sql
LATERAL TABLE(search_persisted_index('index_name', 'query', k))
```

**Parameters:**
- `index_name` (STRING): Name of the persisted index
- `query` (STRING): Search query
- `k` (INT, optional): Number of results (default: 10)

**Returns:** Multiple rows with (text, index, score)
- `text` (STRING): The text content
- `index` (INT): Original row index
- `score` (DOUBLE): Similarity score (0.0 to 1.0)

**Examples:**
```sql
-- Basic search
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'great service', 10))
AS T(text, index, score);

-- With filtering
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'quality product', 20))
AS T(text, index, score)
WHERE T.score > 0.7
ORDER BY T.score DESC;
```

#### Installation

**Step 1: Install UDFs to Flink Containers**
```bash
cd tools/agstream_manager
./scripts/install_udfs.sh
```

**Step 2: Register Functions**
```bash
./scripts/flink_sql.sh -f sql/register_agpersist_search.sql
```

Or manually:
```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS build_search_index
AS 'agpersist_search.build_search_index' LANGUAGE PYTHON;

CREATE TEMPORARY FUNCTION IF NOT EXISTS search_persisted_index
AS 'agpersist_search.search_persisted_index' LANGUAGE PYTHON;
```

#### Configuration

Set storage path via environment variable:
```bash
export AGSTREAM_INDEX_PATH=/path/to/indexes
```

Default: `/tmp/agstream_indexes`

#### Comparison: agsearch vs agpersist_search

| Feature | agsearch | agpersist_search |
|---------|----------|------------------|
| **Persistence** | No (rebuilds each time) | Yes (saved to disk) |
| **Use Case** | One-time searches | Repeated searches |
| **Performance** | Slower (rebuilds index) | Faster (loads index) |
| **Storage** | Memory only | Disk + memory |
| **Sharing** | No | Yes (across sessions) |
| **Best For** | Dynamic data | Static/semi-static data |

**When to use agpersist_search:**
- ✅ Running same/similar queries multiple times
- ✅ Production workloads
- ✅ Large datasets that don't change frequently
- ✅ Need to share indexes across jobs

**When to use agsearch:**
- ✅ One-time queries
- ✅ Rapidly changing data
- ✅ Prototyping
- ✅ Small datasets

---

## Build Optimization

### Quick Reference

| Scenario | Command | Speed | Use When |
|----------|---------|-------|----------|
| **UDF changes only** | `./scripts/update_udf.sh` | ⚡ Instant | Modified UDF files |
| **Incremental build** | `./scripts/rebuild_flink_image.sh` | 🚀 Fast | Added dependencies |
| **Full rebuild** | `./scripts/rebuild_flink_image.sh --force` | 🐌 Slow | Major changes |

### Detailed Workflows

#### 1. UDF-Only Updates (Fastest) ⚡

When you only modify UDF files:

```bash
cd tools/agstream_manager
./scripts/update_udf.sh
```

**What it does:**
- Copies updated UDF files to the running Flink container
- No container restart needed
- Changes take effect immediately on next query

**Time:** ~1 second

#### 2. Incremental Build (Default) 🚀

When you need to rebuild but want to use Docker cache:

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh
```

**Time:** ~30 seconds - 2 minutes

**Use when:**
- Added new Python dependencies
- Modified Dockerfile
- Updated Agentics package

#### 3. Full Rebuild (Slowest) 🐌

When you need a complete rebuild from scratch:

```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh --force
```

**Time:** ~5-10 minutes

**Use when:**
- Troubleshooting cache issues
- Major version upgrades
- Suspected corrupted layers

### Workflow Examples

**Adding a New UDF Function:**
```bash
# 1. Edit the UDF file
vim tools/agstream_manager/udfs/ag_operators.py

# 2. Quick update (no rebuild)
cd tools/agstream_manager
./scripts/update_udf.sh

# 3. Test immediately
./scripts/flink_sql.sh
```

**Adding a Python Dependency:**
```bash
# 1. Edit Dockerfile
vim tools/agstream_manager/Dockerfile.flink-python

# 2. Incremental rebuild
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh

# 3. Restart services
./manage_services_full.sh restart
```

---

## Quick Reference

### Function Comparison

| Feature | agmap | agreduce | agmap_registry | agreduce_registry | agsearch | agpersist_search |
|---------|-------|----------|----------------|-------------------|----------|------------------|
| **Type** | UDTF | UDAF | UDTF | UDAF | UDAF | UDAF + UDTF |
| **Input** | One row | All rows | One row | All rows | All rows | All rows |
| **Output** | Multiple rows | Single JSON | Multiple rows | Single JSON | JSON array | Multiple rows |
| **Type Definition** | Manual | Manual | Auto (Schema) | Auto (Schema) | N/A | N/A |
| **Persistence** | N/A | N/A | N/A | N/A | No | Yes |

### Registration Commands

```sql
-- Core UDFs
CREATE TEMPORARY SYSTEM FUNCTION agmap AS 'ag_operators.agmap' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION agreduce AS 'agreduce.agreduce' LANGUAGE PYTHON;

-- Registry UDFs (example)
CREATE TEMPORARY SYSTEM FUNCTION agmap_sentiment AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION agreduce_sentiment AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;

-- Vector Search UDFs
CREATE TEMPORARY FUNCTION agsearch AS 'agsearch.agsearch' LANGUAGE PYTHON;
CREATE TEMPORARY FUNCTION build_search_index AS 'agpersist_search.build_search_index' LANGUAGE PYTHON;
CREATE TEMPORARY FUNCTION search_persisted_index AS 'agpersist_search.search_persisted_index' LANGUAGE PYTHON;
CREATE TEMPORARY FUNCTION explode_search_results AS 'explode_search_results.explode_search_results' LANGUAGE PYTHON;
```

### Common Patterns

**Pattern 1: Transform then Aggregate**
```sql
WITH transformed AS (
    SELECT T.sentiment_label, T.sentiment_score
    FROM pr,
    LATERAL TABLE(agmap_sentiment(customer_review))
    AS T(sentiment_label, sentiment_score)
)
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

**Pattern 3: Persistent Search Workflow**
```sql
-- Build index once
SELECT build_search_index(customer_review, 'reviews_index') as status
FROM pr;

-- Search many times
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'fast delivery', 10))
AS T(text, index, score)
WHERE T.score > 0.5;
```

---

## Troubleshooting

### Common Issues

#### 1. ModuleNotFoundError

**Error:** `ModuleNotFoundError: No module named 'agreduce_registry'`

**Solution:**
```bash
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart_services
```

#### 2. ImportError: libgomp.so.1

**Error:** `ImportError: libgomp.so.1: cannot open shared object file`

**Solution:**
```bash
./scripts/fix_agreduce.sh
```

#### 3. Function Already Exists

**Error:** `A function named 'agreduce_sentiment' does already exist`

**Solution:** Function is already registered. Or drop and recreate:
```sql
DROP TEMPORARY SYSTEM FUNCTION IF EXISTS agreduce_sentiment;
CREATE TEMPORARY SYSTEM FUNCTION agreduce_sentiment
AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;
```

#### 4. ParseException with 'result'

**Error:** `Encountered "result" at line X`

**Solution:** `result` is a reserved keyword. Use a different alias:
```sql
-- ✅ Correct
WITH agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT JSON_VALUE(agg_result, '$.sentiment_label') FROM agg;
```

#### 5. Index Not Found (agpersist_search)

**Solution:**
```sql
-- Build the index first
SELECT build_search_index(customer_review, 'test_index') as status
FROM pr;
```

#### 6. Model Download Issues

**Solution:**
```bash
# Pre-download the model in the Flink container
docker exec flink-taskmanager python3 -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"
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

### 2. Avoid Reserved Keywords

❌ **Don't use:** `result`, `value`, `data` as column aliases
✅ **Use instead:** `agg_result`, `output_value`, `transformed_data`

### 3. Filter Before Aggregation

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

### 5. Choose the Right Search Function

- Use **agpersist_search** for production workloads with repeated searches
- Use **agsearch** for ad-hoc analysis and dynamic data

### 6. Optimize Build Process

- Use `update_udf.sh` for UDF changes (instant)
- Use incremental builds for dependency changes (fast)
- Use full rebuilds only when necessary (slow)

### 7. Monitor Performance

```sql
-- Check job status in Flink Web UI: http://localhost:8081

-- Use EXPLAIN to understand query plan
EXPLAIN SELECT T.sentiment_label
FROM pr, LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label);
```

### 8. Regenerate Registry UDFs When Schemas Change

```bash
cd tools/agstream_manager
python scripts/generate_registry_udfs.py
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart_services
```

---

## Additional Resources

- **SQL Examples:** `sql/` directory
- **Registration Scripts:** `sql/register_*.sql`
- **Generation Script:** `scripts/generate_registry_udfs.py`
- **Flink Web UI:** http://localhost:8081
- **Schema Registry:** http://localhost:8081

---

## Summary

This guide covers all UDFs in AGStream:

1. **Core UDFs** (`agmap`, `agreduce`) - Dynamic, flexible transformations
2. **Registry UDFs** (`agmap_*`, `agreduce_*`) - Schema-based, type-safe operations
3. **Vector Search UDFs** (`agsearch`, `agpersist_search`) - Semantic search capabilities
4. **Build Optimization** - Efficient development workflows

Choose the right tool for your use case:
- Use **registry UDFs** for production workloads with stable schemas
- Use **core UDFs** for ad-hoc analysis and dynamic type exploration
- Use **agpersist_search** for repeated searches on static data
- Use **agsearch** for one-time searches on dynamic data
- Use **update_udf.sh** for rapid UDF development

All functions support semantic transformations powered by LLMs, enabling intelligent data processing at scale.

---

**Made with Bob** 🤖
