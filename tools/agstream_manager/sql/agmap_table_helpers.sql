-- ============================================================================
-- AGMap Table Helper Functions
-- ============================================================================
-- SQL helper functions that wrap agmap_table JSON parsing for cleaner syntax
-- ============================================================================

-- ============================================================================
-- APPROACH 1: Create Helper Scalar Functions (UDFs)
-- ============================================================================
-- These extract specific fields from the JSON result

-- Helper to extract sentiment_label
CREATE TEMPORARY FUNCTION IF NOT EXISTS get_sentiment_label AS
'org.apache.flink.table.functions.ScalarFunction' LANGUAGE JAVA
USING JAR 'file:///path/to/udf.jar';  -- Would need Java implementation

-- ============================================================================
-- APPROACH 2: Use SQL Views (RECOMMENDED - No Java needed!)
-- ============================================================================
-- Create views that wrap the JSON parsing logic

-- View for sentiment analysis with clean column names
CREATE TEMPORARY VIEW sentiment_analysis AS
SELECT
    customer_review,
    JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
FROM pr,
LATERAL TABLE(agmap_table(customer_review, 'Sentiment', 'registry')) AS T(result);

-- Now you can use it simply:
-- SELECT * FROM sentiment_analysis WHERE sentiment_label = 'positive';

-- ============================================================================
-- APPROACH 3: Create Parameterized View Function (Flink 1.18+)
-- ============================================================================
-- This allows you to pass the input column as a parameter

-- Note: This requires Flink 1.18+ with SQL function support
CREATE TEMPORARY FUNCTION agmap_sentiment_view AS
$$
  SELECT
    JSON_VALUE(result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(result, '$.sentiment_score') AS DOUBLE) as sentiment_score
  FROM LATERAL TABLE(agmap_table($1, 'Sentiment', 'registry')) AS T(result)
$$;

-- Usage would be:
-- SELECT * FROM agmap_sentiment_view(customer_review);

-- ============================================================================
-- APPROACH 4: Use Common Table Expressions (CTEs) - MOST PRACTICAL
-- ============================================================================
-- Create reusable CTE patterns

-- Pattern 1: Simple sentiment extraction
WITH sentiment AS (
    SELECT
        customer_review,
        JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
        CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
    FROM pr,
    LATERAL TABLE(agmap_table(customer_review, 'Sentiment', 'registry')) AS T(result)
)
SELECT * FROM sentiment WHERE sentiment_label = 'positive';

-- Pattern 2: Multiple schema extractions
WITH
    sentiment AS (
        SELECT
            customer_review,
            JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
            CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
        FROM pr,
        LATERAL TABLE(agmap_table(customer_review, 'Sentiment', 'registry')) AS T(result)
    ),
    product_info AS (
        SELECT
            customer_review,
            JSON_VALUE(T.result, '$.Product_name') as product_name,
            JSON_VALUE(T.result, '$.category') as category
        FROM pr,
        LATERAL TABLE(agmap_table(customer_review, 'Product', 'registry')) AS T(result)
    )
SELECT
    s.sentiment_label,
    p.product_name,
    COUNT(*) as count
FROM sentiment s
JOIN product_info p ON s.customer_review = p.customer_review
GROUP BY s.sentiment_label, p.product_name;

-- ============================================================================
-- APPROACH 5: Create SQL Macros (Best for Reusability)
-- ============================================================================
-- Store commonly used patterns as SQL files that can be sourced

-- File: sentiment_macro.sql
-- Content:
/*
SELECT
    $INPUT_COLUMN as original_text,
    JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
FROM $SOURCE_TABLE,
LATERAL TABLE(agmap_table($INPUT_COLUMN, 'Sentiment', 'registry')) AS T(result)
*/

-- Then use with variable substitution in your SQL client

-- ============================================================================
-- RECOMMENDED SOLUTION: Hybrid Approach
-- ============================================================================
-- Combine schema-specific UDTFs with views for best of both worlds

-- Step 1: Register schema-specific UDTF (returns typed columns)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table_sentiment
AS 'ag_operators.agmap_table_sentiment' LANGUAGE PYTHON;

-- Step 2: Create view for common use case
CREATE TEMPORARY VIEW pr_with_sentiment AS
SELECT
    Product_name,
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

-- Step 3: Use the view (super clean!)
SELECT * FROM pr_with_sentiment WHERE sentiment_label = 'positive';

SELECT sentiment_label, COUNT(*) as count
FROM pr_with_sentiment
GROUP BY sentiment_label;

-- ============================================================================
-- PRACTICAL EXAMPLES
-- ============================================================================

-- Example 1: Create a reusable sentiment view
CREATE TEMPORARY VIEW sentiment_reviews AS
SELECT
    Product_name,
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

-- Now use it anywhere:
SELECT * FROM sentiment_reviews WHERE sentiment_score > 0.8;

-- Example 2: Create aggregated view
CREATE TEMPORARY VIEW product_sentiment_summary AS
SELECT
    Product_name,
    T.sentiment_label,
    COUNT(*) as review_count,
    AVG(T.sentiment_score) as avg_score
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY Product_name, T.sentiment_label;

-- Use it:
SELECT * FROM product_sentiment_summary ORDER BY avg_score DESC;

-- ============================================================================
-- SUMMARY: Best Practices
-- ============================================================================
/*
1. For typed columns: Use schema-specific UDTFs (agmap_table_sentiment)
2. For reusability: Wrap in TEMPORARY VIEWs
3. For one-off queries: Use CTEs (WITH clauses)
4. For complex logic: Combine views with CTEs

RECOMMENDED PATTERN:
1. Register: CREATE TEMPORARY SYSTEM FUNCTION agmap_table_sentiment...
2. Create View: CREATE TEMPORARY VIEW sentiment_reviews AS SELECT...
3. Query: SELECT * FROM sentiment_reviews WHERE...

This gives you:
✅ Typed columns (no JSON parsing)
✅ Clean, reusable syntax
✅ Easy to understand and maintain
*/

-- Made with Bob
