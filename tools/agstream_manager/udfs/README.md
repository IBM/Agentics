# AGStream UDFs - Complete Documentation

This comprehensive guide covers all User-Defined Functions (UDFs) available in AGStream, including core functions, registry functions, vector search, and build optimization.

## Table of Contents

1. [Overview](#overview)
2. [Core UDFs](#core-udfs)
   - [agmap - Dynamic Mapping](#agmap---dynamic-mapping)
   - [agreduce - Aggregation](#agreduce---aggregation)
   - [aggenerate - Instance Generation](#aggenerate---instance-generation)
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
   - `aggenerate`: Generate prototypical instances (UDTF)

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

---

### aggenerate - Instance Generation

**Type:** UDTF (User-Defined Table Function)
**Purpose:** Generate prototypical instances of a given type using LLM-powered generation

#### Features

- **Continuous Generation**: Generates instances continuously until stopped by LIMIT
- **Dynamic Mode**: Define single-field types on-the-fly
- **Registry Mode**: Use pre-registered schemas for multi-field generation
- **Batch Processing**: Configurable batch size for efficient LLM calls
- **Custom Instructions**: Guide generation with specific requirements

#### Registration

```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS aggenerate
AS 'aggenerate.aggenerate' LANGUAGE PYTHON;
```

#### Syntax

```sql
-- Dynamic mode (single field)
SELECT T.field_name
FROM LATERAL TABLE(aggenerate(
    'field_name',
    'field_type',
    'optional_instructions',
    batch_size
)) AS T(field_name)
LIMIT n;

-- Registry mode (multiple fields)
SELECT T.field1, T.field2, ...
FROM LATERAL TABLE(aggenerate(
    'SchemaName',
    'registry'
)) AS T(field1, field2, ...)
LIMIT n;
```

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `type_name` | STRING | Yes | - | Field name (dynamic) or schema name (registry) |
| `field_type` | STRING | No | 'str' | Type: 'str', 'int', 'float', 'bool', or 'registry' |
| `instructions` | STRING | No | None | Instructions to guide generation |
| `batch_size` | INT | No | 10 | Number of instances per LLM call |

#### Supported Types (Dynamic Mode)

- `str`, `string` - String values
- `int`, `integer` - Integer values
- `float`, `double` - Float values
- `bool`, `boolean` - Boolean values
- `registry` - Use Schema Registry (multi-field)

#### Examples

**Generate Product Names:**
```sql
SELECT T.product_name
FROM LATERAL TABLE(aggenerate(
    'product_name',
    'str',
    'Creative tech product names'
))
AS T(product_name)
LIMIT 10;
```

**Generate Ratings:**
```sql
SELECT T.rating
FROM LATERAL TABLE(aggenerate(
    'rating',
    'float',
    'Product ratings between 1.0 and 5.0'
))
AS T(rating)
LIMIT 20;
```

**Generate from Registry Schema:**
```sql
SELECT
    JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
FROM LATERAL TABLE(aggenerate('Sentiment', 'registry'))
AS T(result)
LIMIT 5;
```

**Generate with Custom Batch Size:**
```sql
SELECT T.company_name
FROM LATERAL TABLE(aggenerate(
    'company_name',
    'str',
    'Tech startup company names',
    20  -- Generate 20 at a time
))
AS T(company_name)
LIMIT 100;
```

**Create Test Data Table:**
```sql
CREATE TEMPORARY VIEW test_products AS
SELECT T.product_name
FROM LATERAL TABLE(aggenerate(
    'product_name',
    'str',
    'Consumer electronics product names'
))
AS T(product_name)
LIMIT 1000;
```

#### Use Cases

1. **Test Data Generation**: Create synthetic data for testing pipelines
2. **Data Augmentation**: Generate additional training examples
3. **Prototyping**: Quickly populate tables with realistic data
4. **Simulation**: Generate continuous streams of test events
5. **Benchmarking**: Create large datasets for performance testing

#### Important Notes

- **LIMIT Required**: Always use LIMIT to control output; without it, generation continues indefinitely
- **LLM Costs**: Each batch makes an LLM API call, which incurs costs
- **Performance**: Generation is slower than reading from tables due to LLM calls
- **Batch Size**: Larger batches are more efficient but take longer per call
- **Instructions**: More specific instructions yield better results

#### Comparison with Other UDFs

| Feature | agmap | aggenerate |
|---------|-------|------------|
| **Input** | Existing data | None (generates new) |
| **Output** | Transformed data | Generated instances |
| **Use Case** | Transform existing rows | Create new data |
| **LIMIT** | Optional | Required |

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
build_search_index(text_column, 'index_name', override)
```

**Parameters:**
- `row_data` (STRING): Text data or "text|metadata_json" format
- `index_name` (STRING): Name for the persistent index
- `override` (BOOLEAN, optional): If true, rebuild existing index; if false, use existing (default: false)

**Returns:** JSON string with status

Success (new index):
```json
{
  "status": "success",
  "index_name": "my_index",
  "num_items": 1000,
  "dimension": 384
}
```

Index already exists (override=false):
```json
{
  "status": "exists",
  "message": "Index 'my_index' already exists. Using existing index.",
  "index_name": "my_index"
}
```

**Examples:**
```sql
-- Build index (use existing if present)
WITH build_data AS (
    SELECT build_search_index(customer_review, 'reviews_index', false) as json_result
    FROM pr
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.message') as message
FROM build_data;

-- Force rebuild existing index
WITH build_data AS (
    SELECT build_search_index(customer_review, 'reviews_index', true) as json_result
    FROM pr
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.num_items') as num_items
FROM build_data;

-- Filtered index
SELECT build_search_index(customer_review, 'positive_reviews_index', false) as json_result
FROM pr
WHERE rating >= 4;
```

##### 2. search_index_json() - UDAF (Recommended)

Searches a persisted vector index and returns JSON results.

**Syntax:**
```sql
search_index_json('index_name', 'query', k)
```

**Parameters:**
- `index_name` (STRING): Name of the persisted index
- `query` (STRING): Search query
- `k` (INT, optional): Number of results (default: 10)

**Returns:** JSON array of results

**Examples:**
```sql
-- Search and explode results (RECOMMENDED PATTERN)
SELECT text, idx
FROM (
    SELECT search_index_json('reviews_index', 'great service', 10) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- Get raw JSON results
SELECT search_index_json('reviews_index', 'quality product', 5) as results
FROM (VALUES (1));
```

##### 3. list_indexes() - UDAF

Lists all persisted indexes as a comma-separated string.

**Syntax:**
```sql
list_indexes()
```

**Returns:** Comma-separated string of index names

**Examples:**
```sql
-- List all indexes
SELECT list_indexes() as index_list FROM (VALUES (1));
```

##### 4. remove_search_index() - UDAF

Removes a persisted vector search index and all its files.

**Syntax:**
```sql
remove_search_index('index_name')
```

**Parameters:**
- `index_name` (STRING): Name of the index to remove

**Returns:** JSON string with status

**Examples:**
```sql
-- Remove an index
SELECT remove_search_index('old_reviews_index') as status FROM (VALUES (1));

-- Check result
WITH removal AS (
    SELECT remove_search_index('reviews_index') as json_result FROM (VALUES (1))
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.message') as message
FROM removal;
```

##### 5. search_persisted_index() - UDTF (Legacy)

⚠️ **Note:** This function may have issues. Use `search_index_json()` + `explode_search_results()` instead.

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

| Feature | agmap | agreduce | aggenerate | agmap_registry | agreduce_registry | agsearch | agpersist_search |
|---------|-------|----------|------------|----------------|-------------------|----------|------------------|
| **Type** | UDTF | UDAF | UDTF | UDTF | UDAF | UDAF | UDAF + UDTF |
| **Input** | One row | All rows | None | One row | All rows | All rows | All rows |
| **Output** | Multiple rows | Single JSON | Multiple rows | Multiple rows | Single JSON | JSON array | Multiple rows |
| **Type Definition** | Manual | Manual | Manual/Registry | Auto (Schema) | Auto (Schema) | N/A | N/A |
| **Persistence** | N/A | N/A | N/A | N/A | N/A | No | Yes |
| **Use Case** | Transform | Aggregate | Generate | Transform | Aggregate | Search | Search |

### Registration Commands

```sql
-- Core UDFs
CREATE TEMPORARY SYSTEM FUNCTION agmap AS 'ag_operators.agmap' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION agreduce AS 'agreduce.agreduce' LANGUAGE PYTHON;
CREATE TEMPORARY FUNCTION aggenerate AS 'aggenerate.aggenerate' LANGUAGE PYTHON;

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

**Pattern 4: Generate Test Data**
```sql
-- Generate synthetic test data
CREATE TEMPORARY VIEW test_reviews AS
SELECT T.review_text
FROM LATERAL TABLE(aggenerate(
    'review_text',
    'str',
    'Product reviews for electronics, mix of positive and negative'
))
AS T(review_text)
LIMIT 100;

-- Use generated data in pipeline
SELECT T.sentiment_label, COUNT(*) as count
FROM test_reviews,
LATERAL TABLE(agmap_sentiment(review_text))
AS T(sentiment_label)
GROUP BY T.sentiment_label;
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

1. **Core UDFs** (`agmap`, `agreduce`, `aggenerate`) - Dynamic, flexible transformations and generation
2. **Registry UDFs** (`agmap_*`, `agreduce_*`) - Schema-based, type-safe operations
3. **Vector Search UDFs** (`agsearch`, `agpersist_search`) - Semantic search capabilities
4. **Build Optimization** - Efficient development workflows

Choose the right tool for your use case:
- Use **registry UDFs** for production workloads with stable schemas
- Use **core UDFs** for ad-hoc analysis and dynamic type exploration
- Use **aggenerate** for creating synthetic test data and prototyping
- Use **agpersist_search** for repeated searches on static data
- Use **agsearch** for one-time searches on dynamic data
- Use **update_udf.sh** for rapid UDF development

All functions support semantic transformations powered by LLMs, enabling intelligent data processing at scale.

---

**Made with Bob** 🤖
