-- ============================================================================
-- AGMap Table Example - Complete SQL Demonstration
-- ============================================================================
-- This file shows the limitation and workaround for typed column output
-- ============================================================================

-- ============================================================================
-- IMPORTANT: Flink UDTF Limitation
-- ============================================================================
-- Flink requires UDTFs to have STATIC return types defined at registration time.
-- This means we CANNOT create a single agmap_table(input, schema, mode) that
-- dynamically returns different typed columns based on the schema argument.
--
-- WHAT YOU WANT (but is technically impossible):
--   agmap_table(input, 'Sentiment', 'registry') → (sentiment_label, sentiment_score)
--   agmap_table(input, 'Product', 'registry') → (product_name, price, category)
--
-- WHY IT'S IMPOSSIBLE:
--   Flink needs to know the return types (STRING, DOUBLE, etc.) when you
--   CREATE the function, not when you call it. The @udtf decorator requires
--   result_types=[...] to be fixed at registration time.
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Source Table
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
-- STEP 2: Register Generic agmap_table (returns JSON like agmap)
-- ============================================================================

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table
AS 'ag_operators.agmap_table' LANGUAGE PYTHON;

-- ============================================================================
-- STEP 3: Use agmap_table - Still Returns JSON (same as agmap)
-- ============================================================================

-- This works but still requires JSON parsing (no improvement over agmap)
SELECT
    customer_review,
    JSON_VALUE(T.result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(T.result, '$.sentiment_score') AS DOUBLE) as sentiment_score
FROM pr,
LATERAL TABLE(agmap_table(customer_review, 'Sentiment', 'registry')) AS T(result)
LIMIT 5;

-- ============================================================================
-- WORKAROUND: Use Schema-Specific Functions for Typed Columns
-- ============================================================================

-- Register schema-specific function
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table_sentiment
AS 'ag_operators.agmap_table_sentiment' LANGUAGE PYTHON;

-- NOW you get typed columns (no JSON parsing!)
SELECT
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
LIMIT 5;

-- ============================================================================
-- ADVANCED EXAMPLES
-- ============================================================================

-- Example 1: Filter by sentiment (no CAST needed!)
SELECT
    Product_name,
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
WHERE T.sentiment_label = 'positive'
  AND T.sentiment_score > 0.8
LIMIT 10;

-- Example 2: Aggregations work directly (no CAST needed!)
SELECT
    T.sentiment_label,
    COUNT(*) as review_count,
    AVG(T.sentiment_score) as avg_confidence
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY T.sentiment_label;

-- ============================================================================
-- KEY BENEFITS
-- ============================================================================
-- ✅ No JSON_VALUE() needed
-- ✅ No CAST operations required
-- ✅ Cleaner, more readable SQL
-- ✅ Better performance (no JSON parsing)
-- ✅ Type-safe operations in WHERE, GROUP BY, aggregations

-- Made with Bob
