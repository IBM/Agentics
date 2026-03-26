# AGsearch UDF Implementation Summary

## Overview

I've created a new aggregate UDF called `agsearch` that performs vector-based semantic search on accumulated table rows using AG's built-in vector store. This is the streaming SQL equivalent of the `AG.search()` method.

## What Was Created

### 1. Main UDF Implementation
**File**: `tools/agstream_manager/udfs/agsearch.py`

- **Type**: User-Defined Aggregate Function (UDAF)
- **Purpose**: Accumulates all input rows, builds a vector index, and performs semantic search
- **Technology**: Uses AG's vector store with sentence transformers for embeddings

**Key Features**:
- Vector-based semantic search using `sentence-transformers/all-MiniLM-L6-v2`
- Configurable result count (max_k parameter, default: 10)
- JSON output format for easy parsing
- Thread-safe with thread-local AG instances
- Automatic index building for each query

### 2. SQL Registration Script
**File**: `tools/agstream_manager/sql/register_agsearch.sql`

Simple SQL script to register the UDF in Flink:
```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS agsearch AS 'agsearch.agsearch' LANGUAGE PYTHON;
```

### 3. SQL Examples
**File**: `tools/agstream_manager/sql/agsearch_examples.sql`

Contains 10 comprehensive examples demonstrating:
- Basic search operations
- Custom result counts
- Filtering and windowing
- Multiple simultaneous searches
- Integration with other SQL operations

### 4. Documentation
**File**: `tools/agstream_manager/udfs/AGSEARCH_README.md`

Complete documentation including:
- Syntax and parameters
- Usage examples
- Technical details about vector store
- Performance considerations
- Troubleshooting guide
- Best practices

### 5. Docker Dependencies
**File**: `tools/agstream_manager/Dockerfile.flink-python` (updated)

Added `sentence-transformers` to the Docker image dependencies to support the embedding model.

## How It Works

### Architecture

```
Input Rows → Accumulator → AG Instance → Vector Index → Search → JSON Results
```

1. **Accumulation Phase**: Collects all input rows during aggregation
2. **Index Building**: Creates AG instance and builds vector index using sentence transformers
3. **Search Execution**: Performs vector search with the user's query
4. **Result Formatting**: Returns top-k results as JSON array

### Data Flow

```python
# Input: Table rows
SELECT agsearch(review_text, 'great service', 10) FROM reviews;

# Processing:
# 1. Accumulate: ["Review 1", "Review 2", "Review 3", ...]
# 2. Build AG: AG(atype=TextItem, states=[...])
# 3. Build Index: Vector embeddings for all texts
# 4. Search: Find top 10 most similar to "great service"
# 5. Return: [{"text": "...", "index": 0}, ...]
```

## Usage Examples

### Basic Search
```sql
SELECT agsearch(review_text, 'great service', 10) as search_results
FROM pr;
```

### With Filtering
```sql
SELECT agsearch(review_text, 'quality', 10) as quality_reviews
FROM pr
WHERE rating >= 4;
```

### With Time Windows
```sql
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
    agsearch(log_message, 'warning', 10) as warnings
FROM system_logs
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);

### Exploding Results into Multiple Rows (NEW!)
```sql
-- Use explode_search_results UDTF to get each result as a separate row
SELECT
    text,
    idx
FROM (
    SELECT agsearch(review_text, 'great service', 10) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx);
```

### Explode with Filtering
```sql
-- Get only top 3 results
SELECT
    text,
    idx,
    LENGTH(text) as text_length
FROM (
    SELECT agsearch(review_text, 'excellent product', 10) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx)
WHERE idx < 3
ORDER BY idx;
```

```

## Technical Details

### Vector Store Implementation

- **Embedding Model**: `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions)
- **Index Type**: HNSW (Hierarchical Navigable Small World)
- **Similarity Metric**: Cosine similarity
- **Search Algorithm**: Approximate nearest neighbor

### Performance Characteristics

**First Execution**:
- Downloads embedding model (~90MB)
- Model initialization
- May take 30-60 seconds

**Subsequent Executions**:
- Model cached in memory
- Only index building and search
- Much faster (seconds)

**Memory Usage**:
- Embedding model: ~400MB per task
- Vector index: Depends on data size
- Each parallel Flink task loads its own copy

### Thread Safety

- Uses thread-local storage for AG instances
- Each Flink task maintains its own AG instance
- Safe for parallel processing across multiple tasks

## Comparison with AG.search()

| Aspect | AG.search() | agsearch UDF |
|--------|-------------|--------------|
| **Environment** | Python scripts | Flink SQL |
| **Input** | AG instance | Table rows |
| **Output** | Filtered AG | JSON array |
| **Use Case** | Batch processing | Stream processing |
| **Index Building** | Manual (`build_index()`) | Automatic |
| **Deployment** | Local/notebook | Distributed Flink cluster |

## Dependencies

### Python Packages (Already in Docker)
- `agentics` - Core AG functionality
- `sentence-transformers` - Embedding model (NEWLY ADDED)
- `hnswlib` - Vector index
- `scikit-learn` - Clustering utilities
- `pydantic` - Data models
- `pyflink` - Flink Python API

### System Requirements
- Internet access (first run only, for model download)
- Sufficient memory (~500MB per task)
- Python 3.10+

## Installation & Setup

### 1. Rebuild Docker Image
```bash
cd tools/agstream_manager
docker-compose build flink-jobmanager flink-taskmanager
```

### 2. Start Services
```bash
./manage_services.sh start
```

### 3. Register UDF
```bash
./scripts/flink_sql.sh -f sql/register_agsearch.sql
```

### 4. Verify
```sql
SHOW FUNCTIONS;
-- Should see 'agsearch' in the list
```

## Best Practices

### 1. Filter Before Searching
Reduce input data size to improve performance:
```sql
SELECT agsearch(text, 'query', 10)
FROM table
WHERE category = 'relevant' AND date > CURRENT_DATE - 7
```

### 2. Use Appropriate max_k
Don't request more results than needed:
```sql
-- Good: Only need top 5
agsearch(text, 'query', 5)

-- Bad: Requesting 100 when only using 5
agsearch(text, 'query', 100)
```

### 3. Window Large Streams
Use time windows for continuous streams:
```sql
SELECT agsearch(text, 'query', 10)
FROM table
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
```

### 4. Pre-download Model (Optional)
For production, pre-download the model in the Docker image:
```dockerfile
RUN python3 -c "from sentence_transformers import SentenceTransformer; \
    SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"
```

## Limitations

1. **Model Download**: First execution requires internet access
2. **Memory**: Each task loads the full embedding model (~400MB)
3. **Latency**: Index building adds latency to aggregation
4. **Accuracy**: Uses approximate nearest neighbor (not exact)
5. **Scalability**: Not suitable for very large datasets (millions of rows)

## Exploding Results (NEW Feature!)

### The explode_search_results UDTF

Since `agsearch` is an aggregate function that returns a single JSON array, we've created a companion **Table Function (UDTF)** called `explode_search_results` to convert the JSON array into multiple rows.

**Function Signature:**
```python
@udtf(result_types=[DataTypes.STRING(), DataTypes.INT()])
def explode_search_results(json_array_string)
```

**What it does:**
- Takes the JSON array string from `agsearch`
- Parses it and yields one row per search result
- Returns `(text, index)` for each result

**Registration:**
```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS explode_search_results
AS 'agsearch.explode_search_results'
LANGUAGE PYTHON;
```

**Usage Pattern:**
```sql
SELECT text, idx
FROM (
    SELECT agsearch(column, 'query', k) as results FROM table
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);
```

**Benefits:**
- ✅ Get each search result as a separate row
- ✅ Easy to filter, sort, and aggregate individual results
- ✅ Integrate seamlessly with other SQL operations
- ✅ No need to manually parse JSON with `JSON_VALUE`

**Example Use Cases:**

1. **Get top 3 results only:**
```sql
SELECT text, idx
FROM (SELECT agsearch(review, 'query', 10) as r FROM pr),
LATERAL TABLE(explode_search_results(r)) AS T(text, idx)
WHERE idx < 3;
```

2. **Count results by keyword:**
```sql
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN text LIKE '%excellent%' THEN 1 ELSE 0 END) as has_excellent
FROM (SELECT agsearch(review, 'quality', 20) as r FROM pr),
LATERAL TABLE(explode_search_results(r)) AS T(text, idx);
```

3. **Combine multiple searches:**
```sql
SELECT 'positive' as type, text FROM
(SELECT agsearch(comment, 'positive', 5) as r FROM comments),
LATERAL TABLE(explode_search_results(r)) AS T(text, idx)
UNION ALL
SELECT 'negative' as type, text FROM
(SELECT agsearch(comment, 'negative', 5) as r FROM comments),
LATERAL TABLE(explode_search_results(r)) AS T(text, idx);
```


## Troubleshooting

### Model Download Fails
```bash
# Pre-download in Flink container
docker exec -it flink-jobmanager bash
python3 -c "from sentence_transformers import SentenceTransformer; \
    SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"
```

### Out of Memory
- Reduce input data with filtering
- Decrease max_k value
- Increase Flink task memory allocation

### Slow Performance
- Use smaller time windows
- Filter data before aggregation
- Consider caching results for repeated queries

## Future Enhancements

Potential improvements for future versions:

1. **Custom Embedding Models**: Support for different embedding models
2. **Persistent Index**: Cache vector index across queries
3. **Batch Processing**: Process multiple queries in one pass
4. **Score Threshold**: Filter results by minimum similarity score
5. **Metadata Support**: Include additional metadata in results
6. **External Vector DB**: Integration with external vector databases (Pinecone, Weaviate, etc.)

## Related Files

- Main implementation: `tools/agstream_manager/udfs/agsearch.py`
- Registration: `tools/agstream_manager/sql/register_agsearch.sql`
- Examples: `tools/agstream_manager/sql/agsearch_examples.sql`
- Documentation: `tools/agstream_manager/udfs/AGSEARCH_README.md`
- Docker config: `tools/agstream_manager/Dockerfile.flink-python`

## Testing

To test the UDF:

```sql
-- 1. Create test data
CREATE TABLE test_reviews (
    id INT,
    review_text STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

-- 2. Run search
SELECT agsearch(review_text, 'excellent product', 5) as results
FROM test_reviews;

-- 3. Check output format
-- Should return JSON array: [{"text": "...", "index": 0}, ...]
```

## Summary

The `agsearch` UDF successfully brings AG's powerful vector search capabilities to Flink SQL, enabling semantic search operations on streaming data. It uses the same underlying technology as `AG.search()` but is optimized for the Flink distributed processing environment.

Key benefits:
- ✅ Semantic search in SQL
- ✅ No code required (pure SQL)
- ✅ Distributed processing
- ✅ Real-time streaming support
- ✅ Easy integration with existing Flink pipelines
