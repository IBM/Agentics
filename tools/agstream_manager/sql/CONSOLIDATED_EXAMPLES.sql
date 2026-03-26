-- ============================================================================
-- CONSOLIDATED AGMap & AGReduce Examples
-- ============================================================================
-- This file provides complete examples for all AGMap and AGReduce modes:
-- 1. AGMap with Schema Registry (registry mode)
-- 2. AGMap Dynamic with Description (dynamic mode)
-- 3. AGReduce with Schema Registry (registry mode)
-- 4. AGReduce Dynamic with Description (dynamic mode)
--
-- Topic: pr (Product Reviews)
-- Schema: ProductReview with fields: Product_name, customer_review
-- ============================================================================
-- ============================================================================
-- VIEWING RESULTS: Tips for Full Table Display
-- ============================================================================
-- If table output is truncated in Flink SQL terminal:
--
-- Option 1: Set wider display width
--   SET 'sql-client.display.max-column-width' = '100';
--
-- Option 2: Use fewer columns in SELECT
--   SELECT customer_review, JSON_VALUE(T.json_result, '$.sentiment_label')
--   (instead of selecting all fields)
--
-- Option 3: Increase terminal window width
--   Resize your terminal to be wider before running queries
--
-- Option 4: Export results to file
--   INSERT INTO output_table SELECT ... FROM ...;
--
-- Option 5: Use DESCRIBE to see schema first
--   DESCRIBE pr;
-- ============================================================================


-- ============================================================================
-- SETUP: Create Source Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS pr (
    Product_name STRING,
    customer_review STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'pr',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);

-- ============================================================================
-- SECTION 1: AGMap with Schema Registry (Registry Mode)
-- ============================================================================
-- Uses pre-registered schemas from Schema Registry
-- Returns multiple fields defined in the schema
-- ============================================================================

-- Example 1.1: Extract Sentiment using Registry Schema
-- First, register the UDF (if not already registered):
-- CREATE TEMPORARY SYSTEM FUNCTION agmap_sentiment AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;
--
-- Registry UDFs return TYPED COLUMNS directly (not JSON!)
-- The 'Sentiment' schema has fields: sentiment_label (STRING), sentiment_score (DOUBLE)

SELECT
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
LIMIT 5;

-- Example 1.2: Using agmap() with Registry Mode
-- The generic agmap() function with 'registry' mode returns JSON
-- This is different from the specialized registry UDFs above

SELECT
    customer_review,
    JSON_VALUE(T.json_result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(T.json_result, '$.sentiment_score') AS DOUBLE) as sentiment_score
FROM pr,
LATERAL TABLE(agmap(customer_review, 'Sentiment', 'registry')) AS T(json_result)
LIMIT 5;

-- Example 1.3: Filter by Registry-based Sentiment (using typed columns)
SELECT
    Product_name,
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
WHERE T.sentiment_label = 'positive'
  AND T.sentiment_score > 0.8
LIMIT 10;

-- Example 1.4: Aggregate by Registry-based Sentiment (using typed columns)
SELECT
    T.sentiment_label,
    COUNT(*) as review_count,
    AVG(T.sentiment_score) as avg_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY T.sentiment_label;

-- ============================================================================
-- SECTION 2: AGMap Dynamic with Description (Dynamic Mode)
-- ============================================================================
-- Generates types on-the-fly without Schema Registry
-- Single field extraction with custom descriptions
-- ============================================================================

-- Example 2.1: Extract Sentiment (String) with Description
SELECT
    Product_name,
    customer_review,
    JSON_VALUE(T.json_result, '$.sentiment') as sentiment
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'sentiment',
    'str',
    'The sentiment: positive, negative, or neutral'
)) AS T(json_result)
LIMIT 5;

-- Example 2.2: Extract Rating (Integer) with Description
SELECT
    Product_name,
    customer_review,
    CAST(JSON_VALUE(T.json_result, '$.rating') AS INT) as rating
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'rating',
    'int',
    'Extract star rating from 1 to 5 based on review sentiment'
)) AS T(json_result)
LIMIT 5;

-- Example 2.3: Extract Emotion (String) with Detailed Description
SELECT
    customer_review,
    JSON_VALUE(T.json_result, '$.emotion') as emotion
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'emotion',
    'str',
    'Primary emotion expressed: joy, anger, sadness, fear, surprise, or neutral'
)) AS T(json_result)
LIMIT 5;

-- Example 2.4: Extract Urgency Level (Integer) with Scale Description
SELECT
    Product_name,
    customer_review,
    CAST(JSON_VALUE(T.json_result, '$.urgency') AS INT) as urgency
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'urgency',
    'int',
    'Urgency level from 1 (low) to 5 (critical)'
)) AS T(json_result)
WHERE CAST(JSON_VALUE(T.json_result, '$.urgency') AS INT) >= 4
LIMIT 5;

-- Example 2.5: Extract Boolean Flag with Description
SELECT
    Product_name,
    customer_review,
    CAST(JSON_VALUE(T.json_result, '$.would_recommend') AS BOOLEAN) as would_recommend
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'would_recommend',
    'bool',
    'Would the reviewer recommend this product to others?'
)) AS T(json_result)
WHERE CAST(JSON_VALUE(T.json_result, '$.would_recommend') AS BOOLEAN) = true
LIMIT 5;

-- Example 2.6: Extract Confidence Score (Float) with Range Description
SELECT
    Product_name,
    customer_review,
    CAST(JSON_VALUE(T.json_result, '$.confidence') AS DOUBLE) as confidence
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'confidence',
    'float',
    'Confidence score between 0.0 (uncertain) and 1.0 (very confident)'
)) AS T(json_result)
LIMIT 5;

-- Example 2.7: Multiple Dynamic Extractions in One Query
SELECT
    Product_name,
    customer_review,
    JSON_VALUE(s.json_result, '$.sentiment') as sentiment,
    CAST(JSON_VALUE(r.json_result, '$.rating') AS INT) as rating,
    JSON_VALUE(e.json_result, '$.emotion') as emotion
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment', 'str', 'positive, negative, or neutral')) AS s(json_result),
LATERAL TABLE(agmap(customer_review, 'rating', 'int', 'star rating 1-5')) AS r(json_result),
LATERAL TABLE(agmap(customer_review, 'emotion', 'str', 'primary emotion')) AS e(json_result)
LIMIT 5;

-- ============================================================================
-- SECTION 3: AGReduce with Schema Registry (Registry Mode)
-- ============================================================================
-- Aggregates all rows using pre-registered schema
-- Returns structured result matching the schema
-- ============================================================================

-- Example 3.1: Aggregate to ProductReview Schema
-- Assumes 'ProductReview' schema is registered
-- Returns aggregated Product_name and customer_review

SELECT
    JSON_VALUE(agreduce_productreview(customer_review), '$.Product_name') as aggregated_product,
    JSON_VALUE(agreduce_productreview(customer_review), '$.customer_review') as aggregated_review
FROM pr;

-- Example 3.2: Aggregate to Sentiment Schema
-- Assumes 'Sentiment' schema is registered
-- Returns overall sentiment_label and sentiment_score

SELECT
    JSON_VALUE(agreduce_sentiment(customer_review), '$.sentiment_label') as overall_sentiment,
    JSON_VALUE(agreduce_sentiment(customer_review), '$.sentiment_score') as overall_score
FROM pr;

-- Example 3.3: Aggregate with Filtering (Positive Reviews Only)
SELECT
    JSON_VALUE(agreduce_productreview(customer_review), '$.Product_name') as product,
    JSON_VALUE(agreduce_productreview(customer_review), '$.customer_review') as positive_summary
FROM pr
WHERE customer_review LIKE '%good%' OR customer_review LIKE '%great%' OR customer_review LIKE '%excellent%';

-- Example 3.4: Aggregate by Product (Grouped)
SELECT
    Product_name,
    JSON_VALUE(agreduce_productreview(customer_review), '$.customer_review') as product_summary
FROM pr
GROUP BY Product_name;

-- ============================================================================
-- SECTION 4: AGReduce Dynamic with Description (Dynamic Mode)
-- ============================================================================
-- Aggregates all rows with custom field and description
-- Single field aggregation with LLM guidance
-- ============================================================================

-- Example 4.1: Create Executive Summary (String)
SELECT
    JSON_VALUE(
        agreduce(
            customer_review,
            'summary',
            'str',
            'Create a 2-3 sentence executive summary of all customer feedback'
        ),
        '$.summary'
    ) as executive_summary
FROM pr;

-- Example 4.2: Calculate Average Sentiment Score (Float)
SELECT
    CAST(JSON_VALUE(
        agreduce(
            customer_review,
            'avg_sentiment',
            'float',
            'Average sentiment score from 0.0 (very negative) to 1.0 (very positive)'
        ),
        '$.avg_sentiment'
    ) AS DOUBLE) as avg_sentiment_score
FROM pr;

-- Example 4.3: Count Total Complaints (Integer)
SELECT
    CAST(JSON_VALUE(
        agreduce(
            customer_review,
            'complaint_count',
            'int',
            'Total number of complaints or issues mentioned across all reviews'
        ),
        '$.complaint_count'
    ) AS INT) as total_complaints
FROM pr;

-- Example 4.4: Check for Any Negative Reviews (Boolean)
SELECT
    CAST(JSON_VALUE(
        agreduce(
            customer_review,
            'has_negative',
            'bool',
            'Are there any negative reviews in the dataset?'
        ),
        '$.has_negative'
    ) AS BOOLEAN) as has_negative_reviews
FROM pr;

-- Example 4.5: Extract Top Themes (String with Detailed Description)
SELECT
    JSON_VALUE(
        agreduce(
            customer_review,
            'top_themes',
            'str',
            'List the top 5 most common themes or topics mentioned, in order of frequency'
        ),
        '$.top_themes'
    ) as top_themes
FROM pr;

-- Example 4.6: Generate Action Items (String)
SELECT
    JSON_VALUE(
        agreduce(
            customer_review,
            'action_items',
            'str',
            'Generate 3-5 specific, actionable recommendations based on customer feedback'
        ),
        '$.action_items'
    ) as recommended_actions
FROM pr;

-- Example 4.7: Calculate Net Promoter Score (Float)
SELECT
    CAST(JSON_VALUE(
        agreduce(
            customer_review,
            'nps_score',
            'float',
            'Calculate Net Promoter Score from 0-10 based on customer sentiment'
        ),
        '$.nps_score'
    ) AS DOUBLE) as nps_score
FROM pr;

-- Example 4.8: Aggregate with LIMIT (Testing)
SELECT
    JSON_VALUE(
        agreduce(
            customer_review,
            'sample_summary',
            'str',
            'Summarize the overall sentiment from this sample of reviews'
        ),
        '$.sample_summary'
    ) as sample_summary
FROM pr
LIMIT 10;

-- ============================================================================
-- COMBINED EXAMPLES: Using Both AGMap and AGReduce
-- ============================================================================

-- Example 5.1: Extract Sentiments, Then Aggregate
WITH extracted_sentiments AS (
    SELECT
        customer_review,
        JSON_VALUE(T.json_result, '$.sentiment') as sentiment
    FROM pr,
    LATERAL TABLE(agmap(customer_review, 'sentiment', 'str', 'positive, negative, or neutral')) AS T(json_result)
)
SELECT
    JSON_VALUE(
        agreduce(
            sentiment,
            'sentiment_distribution',
            'str',
            'Provide percentage breakdown: X% positive, Y% neutral, Z% negative'
        ),
        '$.sentiment_distribution'
    ) as distribution
FROM extracted_sentiments;

-- Example 5.2: Filter by Dynamic AGMap, Aggregate with Registry
SELECT
    JSON_VALUE(agreduce_productreview(customer_review), '$.customer_review') as negative_summary
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment', 'str', 'positive, negative, or neutral')) AS T(json_result)
WHERE JSON_VALUE(T.json_result, '$.sentiment') = 'negative';

-- Example 5.3: Registry AGMap with Dynamic AGReduce
WITH sentiment_extracted AS (
    SELECT
        customer_review,
        JSON_VALUE(T.json_result, '$.sentiment_label') as sentiment_label,
        CAST(JSON_VALUE(T.json_result, '$.sentiment_score') AS DOUBLE) as sentiment_score
    FROM pr,
    LATERAL TABLE(agmap_sentiment(customer_review)) AS T(json_result)
    WHERE CAST(JSON_VALUE(T.json_result, '$.sentiment_score') AS DOUBLE) > 0.7
)
SELECT
    JSON_VALUE(
        agreduce(
            customer_review,
            'high_confidence_summary',
            'str',
            'Summarize reviews with high confidence sentiment scores'
        ),
        '$.high_confidence_summary'
    ) as summary
FROM sentiment_extracted;

-- ============================================================================
-- QUICK REFERENCE GUIDE
-- ============================================================================

/*
AGMap Modes:
1. Registry Mode: agmap(input, 'SchemaName', 'registry')
   - Returns JSON string with multiple fields from registered schema
   - Example: agmap(text, 'Sentiment', 'registry') AS T(json_result)
   - Extract: JSON_VALUE(T.json_result, '$.sentiment_label'), JSON_VALUE(T.json_result, '$.sentiment_score')

2. Dynamic Mode: agmap(input, 'field_name', 'type', 'description')
   - Returns JSON string with single field
   - Types: 'str', 'int', 'float', 'bool'
   - Example: agmap(text, 'rating', 'int', 'star rating 1-5') AS T(json_result)
   - Extract: JSON_VALUE(T.json_result, '$.rating')

AGReduce Modes:
1. Registry Mode: agreduce_schemaname(input)
   - Aggregates to registered schema structure, returns JSON
   - Example: agreduce_sentiment(text) → JSON with sentiment_label, sentiment_score
   - Extract: JSON_VALUE(T.json_result, '$.sentiment_label')

2. Dynamic Mode: agreduce(input, 'field_name', 'type', 'description')
   - Aggregates to single field, returns JSON
   - Types: 'str', 'int', 'float', 'bool'
   - Example: agreduce(text, 'summary', 'str', 'overall summary') → JSON with summary
   - Extract: JSON_VALUE(T.json_result, '$.summary')

Extracting Results:
- ALL functions return JSON strings
- Use JSON_VALUE() to extract fields: JSON_VALUE(T.json_result, '$.field_name')
- Cast as needed: CAST(JSON_VALUE(...) AS INT/DOUBLE/BOOLEAN)
- AGMap returns 1 column, AGReduce returns JSON directly
*/

-- ============================================================================
-- SECTION 5: AGSearch - Semantic Vector Search
-- ============================================================================
-- Performs vector-based semantic search on accumulated rows
-- Uses sentence transformers for embeddings and HNSW for indexing
-- Returns top-k most similar results to a query
-- ============================================================================

-- Example 5.1: Basic Semantic Search
-- Search for reviews mentioning "great service"
SELECT
    JSON_VALUE(agsearch(customer_review, 'great service', 10), '$[0].text') as top_result,
    JSON_VALUE(agsearch(customer_review, 'great service', 10), '$[0].index') as result_index
FROM pr;

-- Example 5.2: Search with Exploded Results (Multiple Rows)
-- Use explode_search_results to get each result as a separate row
-- First register the explode function:
-- CREATE TEMPORARY FUNCTION IF NOT EXISTS explode_search_results
-- AS 'agsearch.explode_search_results' LANGUAGE PYTHON;

SELECT
    text as matching_review,
    idx as relevance_rank
FROM (
    SELECT agsearch(customer_review, 'excellent product quality', 10) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx)
LIMIT 5;

-- Example 5.3: Search with Filtering
-- Find reviews about "fast shipping" from positive reviews only
SELECT
    text,
    idx
FROM (
    SELECT agsearch(customer_review, 'fast shipping', 5) as results
    FROM pr
    WHERE customer_review LIKE '%good%' OR customer_review LIKE '%great%'
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
ORDER BY idx;

-- Example 5.4: Multiple Searches in One Query
-- Search for different topics and compare results
SELECT
    'quality' as search_topic,
    text,
    idx
FROM (
    SELECT agsearch(customer_review, 'product quality', 3) as results FROM pr
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
UNION ALL
SELECT
    'service' as search_topic,
    text,
    idx
FROM (
    SELECT agsearch(customer_review, 'customer service', 3) as results FROM pr
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
ORDER BY search_topic, idx;

-- Example 5.5: Search by Product
-- Find most relevant reviews for each product
SELECT
    Product_name,
    text as relevant_review,
    idx as rank
FROM pr,
LATERAL TABLE(explode_search_results(
    (SELECT agsearch(customer_review, 'value for money', 3)
     FROM pr p2
     WHERE p2.Product_name = pr.Product_name)
)) AS T(text, idx)
GROUP BY Product_name, text, idx
ORDER BY Product_name, idx;

-- Example 5.6: Search with Top Results Only
-- Get only the top 3 most relevant results
SELECT
    text,
    idx,
    LENGTH(text) as review_length
FROM (
    SELECT agsearch(customer_review, 'highly recommend', 10) as results
    FROM pr
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
WHERE idx < 3
ORDER BY idx;

-- Example 5.7: Search with Time Windows (for streaming data)
-- Find relevant reviews in 5-minute windows
-- Note: Requires event_time column in your table
-- SELECT
--     TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
--     text,
--     idx
-- FROM pr,
-- LATERAL TABLE(explode_search_results(
--     agsearch(customer_review, 'urgent issue', 5)
-- )) AS T(text, idx)
-- GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), text, idx;

-- Example 5.8: Combine Search with AGMap
-- Extract sentiment from search results
WITH search_results AS (
    SELECT text
    FROM (
        SELECT agsearch(customer_review, 'disappointed', 5) as results
        FROM pr
    ),
    LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
)
SELECT
    text,
    JSON_VALUE(s.json_result, '$.sentiment') as sentiment
FROM search_results,
LATERAL TABLE(agmap(text, 'sentiment', 'str', 'positive, negative, or neutral')) AS s(json_result);

-- Example 5.9: Search and Aggregate
-- Find reviews about "durability" and create summary
SELECT
    JSON_VALUE(
        agreduce(
            text,
            'durability_summary',
            'str',
            'Summarize what customers say about product durability'
        ),
        '$.durability_summary'
    ) as summary
FROM (
    SELECT text
    FROM (
        SELECT agsearch(customer_review, 'durability long-lasting', 10) as results
        FROM pr
    ),
    LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
);

-- Example 5.10: Count Results by Keyword
-- Analyze search results for specific keywords
SELECT
    COUNT(*) as total_results,
    SUM(CASE WHEN text LIKE '%excellent%' THEN 1 ELSE 0 END) as has_excellent,
    SUM(CASE WHEN text LIKE '%recommend%' THEN 1 ELSE 0 END) as has_recommend,
    AVG(LENGTH(text)) as avg_review_length
FROM (
    SELECT agsearch(customer_review, 'best purchase', 20) as results
    FROM pr
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- ============================================================================
-- AGSearch Quick Reference
-- ============================================================================

/*
AGSearch Function:
- Syntax: agsearch(text_column, 'search_query', max_results)
- Returns: JSON array of results: [{"text": "...", "index": 0}, ...]
- Uses: Sentence transformers for embeddings, HNSW for vector search
- Best for: Finding semantically similar content, not exact matches

Explode Search Results:
- Syntax: LATERAL TABLE(explode_search_results(json_array)) AS T(text, idx)
- Converts JSON array into multiple rows
- Returns: (text STRING, index INT) for each result
- Use with: WHERE idx < N to limit results, ORDER BY idx for ranking

Performance Tips:
1. Filter data before searching to reduce input size
2. Use appropriate max_results (don't request more than needed)
3. First execution downloads embedding model (~90MB, takes 30-60s)
4. Subsequent executions are much faster (model cached)
5. Each Flink task loads its own model copy (~400MB memory)

Common Patterns:
- Basic search: agsearch(column, 'query', 10)
- With filtering: WHERE condition before search
- Multiple rows: Use explode_search_results UDTF
- Top N only: WHERE idx < N after explode
- Combine with AGMap: Extract features from search results
- Aggregate results: Use AGReduce on search output
-- ============================================================================
-- SECTION 6: AGPersist Search - Persistent Vector Search Indexes
-- ============================================================================
-- Build persistent vector indexes that survive Flink restarts
-- Enables fast repeated searches without rebuilding indexes
-- Indexes stored on disk and reused across queries
-- ============================================================================

-- Example 6.1: Build a Persistent Index
-- Creates a vector index and saves it to disk
-- First register the functions:
-- CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS build_search_index
-- AS 'agpersist_search.build_search_index' LANGUAGE PYTHON;

SELECT build_search_index(customer_review, 'my_index') as status
FROM pr
LIMIT 10;

-- Expected output: {"status": "success", "index_name": "my_index", "num_items": 10, "dimension": 384}

-- Example 6.2: Search a Persistent Index
-- Search the index built in Example 6.1
-- Register the search functions:
-- CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS search_index_json
-- AS 'agpersist_search.search_index_json' LANGUAGE PYTHON;
-- CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS explode_search_results
-- AS 'agsearch.explode_search_results' LANGUAGE PYTHON;

SELECT text, idx
FROM (
    SELECT search_index_json('my_index', 'great service', 10) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- Example 6.3: Build Index from Filtered Data
-- Create index only from positive reviews
SELECT build_search_index(customer_review, 'positive_reviews_index') as status
FROM pr
WHERE customer_review LIKE '%good%' OR customer_review LIKE '%great%' OR customer_review LIKE '%excellent%'
LIMIT 50;

-- Example 6.4: Build Category-Specific Indexes
-- Create separate indexes for different products
SELECT build_search_index(customer_review, CONCAT(Product_name, '_index')) as status
FROM pr
WHERE Product_name = 'Laptop'
LIMIT 30;

-- Example 6.5: Search Multiple Indexes
-- Compare results across different indexes
SELECT 'positive' as index_type, text, idx
FROM (
    SELECT search_index_json('positive_reviews_index', 'quality', 5) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
UNION ALL
SELECT 'all_reviews' as index_type, text, idx
FROM (
    SELECT search_index_json('my_index', 'quality', 5) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
ORDER BY index_type, idx;

-- Example 6.6: Build Larger Index
-- Create comprehensive index from more data
SELECT build_search_index(customer_review, 'reviews_full') as status
FROM pr
LIMIT 1000;

-- Example 6.7: Search with Different Queries
-- Run multiple searches on same index (very fast!)
SELECT 'shipping' as topic, text
FROM (
    SELECT search_index_json('reviews_full', 'fast delivery shipping', 3) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
UNION ALL
SELECT 'quality' as topic, text
FROM (
    SELECT search_index_json('reviews_full', 'excellent quality durable', 3) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
UNION ALL
SELECT 'service' as topic, text
FROM (
    SELECT search_index_json('reviews_full', 'customer support helpful', 3) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- Example 6.8: Combine Persistent Search with AGMap
-- Extract sentiment from persistent search results
WITH search_results AS (
    SELECT text
    FROM (
        SELECT search_index_json('reviews_full', 'disappointed unhappy', 5) as results
        FROM (VALUES (1))
    ),
    LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
)
SELECT
    text,
    JSON_VALUE(s.json_result, '$.sentiment') as sentiment
FROM search_results,
LATERAL TABLE(agmap(text, 'sentiment', 'str', 'positive, negative, or neutral')) AS s(json_result);

-- Example 6.9: Aggregate Persistent Search Results
-- Summarize what customers say about a topic
SELECT
    JSON_VALUE(
        agreduce(
            text,
            'price_summary',
            'str',
            'Summarize what customers say about pricing and value'
        ),
        '$.price_summary'
    ) as summary
FROM (
    SELECT text
    FROM (
        SELECT search_index_json('reviews_full', 'price value cost expensive cheap', 10) as results
        FROM (VALUES (1))
    ),
    LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
);

-- Example 6.10: Time-Based Index (for temporal analysis)
-- Build daily indexes for tracking changes over time
-- Note: Requires date column in your table
-- SELECT build_search_index(
--     customer_review,
--     CONCAT('reviews_', CAST(CURRENT_DATE AS STRING))
-- ) as status
-- FROM pr
-- WHERE DATE(review_date) = CURRENT_DATE
-- LIMIT 100;

-- ============================================================================
-- AGPersist Search Quick Reference
-- ============================================================================

/*
Build Persistent Index:
- Syntax: build_search_index(text_column, 'index_name')
- Returns: JSON with status, index_name, num_items, dimension
- Storage: Default /tmp/agstream_indexes/ (set AGSTREAM_INDEX_PATH to customize)
- First run: Downloads model (~90MB), takes 30-60s
- Subsequent: Uses cached model, much faster

Search Persistent Index:
- Syntax: search_index_json('index_name', 'query', max_results)
- Returns: JSON array compatible with explode_search_results
- Usage: Same pattern as agsearch with explode_search_results
- Performance: 50-100ms (vs seconds to rebuild index each time)
- Speedup: 20-60x faster than non-persistent search

Key Advantages:
1. Build once, search many times
2. Indexes survive Flink restarts (if using persistent storage)
3. Share indexes across multiple queries/jobs
4. Ideal for static or semi-static data
5. Perfect for production workloads with repeated searches

When to Use:
- ✅ Running same/similar queries multiple times
- ✅ Production workloads requiring fast response
- ✅ Static or slowly-changing datasets
- ✅ Need to share indexes across jobs

When to Use Regular agsearch:
- ✅ One-time ad-hoc queries
- ✅ Rapidly changing data
- ✅ Prototyping and exploration
- ✅ Don't need to persist results

Performance Comparison:
- First search (persistent): ~2-3s (loads index from disk)
- Subsequent searches: ~50-100ms ⚡
- Non-persistent: Rebuilds index every time (~2-3s each)
- Speedup: 20-60x for repeated queries

Storage:
- Default: /tmp/agstream_indexes/ (cleared on container restart)
- Persistent: Mount volume and set AGSTREAM_INDEX_PATH
- Index size: ~1-5MB per 1000 documents
- Model cache: ~400MB per Flink task

Common Patterns:
1. Build filtered indexes: WHERE clause before build_search_index
2. Category-specific: Use CONCAT(category, '_index') for index names
3. Time-based: Include date in index name for temporal analysis
4. Multiple searches: Reuse same index for different queries
5. Combine with AGMap: Extract features from search results
6. Aggregate results: Use AGReduce on search output
*/

*/

-- ============================================================================
-- SECTION 7: List Persistent Indexes
-- ============================================================================
-- View all available persistent search indexes
-- Useful for discovering what indexes exist before searching
-- ============================================================================

-- Example 7.1: List All Indexes (Simple Syntax)
-- Returns comma-separated string of all index names
-- First register the function:
-- CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS list_indexes
-- AS 'agpersist_search.list_indexes' LANGUAGE PYTHON;

SELECT list_indexes() as indexes
FROM (VALUES (1));

-- Expected output: "my_index,positive_reviews_index,reviews_full" or "" if no indexes

-- Example 7.2: Check if Indexes Exist
-- Verify if any indexes are available before searching
SELECT
    CASE
        WHEN list_indexes() = '' THEN 'No indexes found - build one first!'
        ELSE 'Indexes available: ' || list_indexes()
    END as status
FROM (VALUES (1));

-- Example 7.3: Count Available Indexes
-- Calculate how many indexes exist
SELECT
    CASE
        WHEN list_indexes() = '' THEN 0
        ELSE CARDINALITY(SPLIT(list_indexes(), ','))
    END as index_count
FROM (VALUES (1));

-- Example 7.4: List Indexes as Separate Rows (Advanced)
-- Use UDTF version for one row per index
-- First register the UDTF:
-- CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS list_search_indexes
-- AS 'agpersist_search.list_search_indexes' LANGUAGE PYTHON;

SELECT T.indexes
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes)
ORDER BY T.indexes;

-- Example 7.5: Filter Indexes by Pattern
-- Find indexes matching a specific pattern
SELECT T.indexes
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes)
WHERE T.indexes LIKE '%review%';

-- Example 7.6: Check for Specific Index
-- Verify if a particular index exists
SELECT
    CASE
        WHEN COUNT(*) > 0 THEN 'Index "my_index" exists'
        ELSE 'Index "my_index" not found'
    END as status
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes)
WHERE T.indexes = 'my_index';

-- Example 7.7: List Indexes Before Building
-- Good practice: check what exists before creating new index
SELECT
    'Existing indexes: ' || list_indexes() as before_build
FROM (VALUES (1));

-- Then build your new index:
-- SELECT build_search_index(customer_review, 'new_index') as status FROM pr LIMIT 100;

-- Then verify it was created:
SELECT
    'After build: ' || list_indexes() as after_build
FROM (VALUES (1));

-- ============================================================================
-- List Indexes Quick Reference
-- ============================================================================

/*
List Indexes Functions:

1. Simple Syntax (UDAF):
   - Function: list_indexes()
   - Returns: Comma-separated string of index names
   - Usage: SELECT list_indexes() FROM (VALUES (1));
   - Best for: Quick checks, simple displays
   - Example output: "index1,index2,index3"

2. Advanced Syntax (UDTF):
   - Function: list_search_indexes()
   - Returns: One row per index
   - Usage: LATERAL TABLE(list_search_indexes()) AS T(indexes)
   - Best for: Filtering, joining, complex queries
   - Requires: Dummy source table with VALUES (1)

Common Use Cases:
- ✅ Check what indexes exist before searching
- ✅ Verify index was created successfully
- ✅ Discover available indexes in production
- ✅ Filter indexes by naming pattern
- ✅ Count total number of indexes
- ✅ Validate index names before operations

Workflow:
1. List indexes: SELECT list_indexes() FROM (VALUES (1));
2. Build if needed: SELECT build_search_index(...) FROM table;
3. Verify creation: SELECT list_indexes() FROM (VALUES (1));
4. Search index: SELECT search_index_json('index_name', 'query', 10) ...

Storage Location:
- Default: /tmp/agstream_indexes/
- Custom: Set AGSTREAM_INDEX_PATH environment variable
- Persistent: Mount volume in docker-compose.yml
*/

-- ============================================================================
-- END OF CONSOLIDATED EXAMPLES
-- ============================================================================

-- Made with Bob
