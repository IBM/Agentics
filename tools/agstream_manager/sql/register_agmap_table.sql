-- ============================================================================
-- AGMap Table UDF Registration
-- ============================================================================
-- Register UDFs that return typed table columns instead of JSON
--
-- These UDFs work like agmap() but return typed columns directly,
-- eliminating the need for JSON_VALUE() parsing.
-- ============================================================================

-- ============================================================================
-- Register agmap_table UDFs
-- ============================================================================

-- Sentiment Analysis (returns sentiment_label STRING, sentiment_score DOUBLE)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table_sentiment
AS 'ag_operators.agmap_table_sentiment' LANGUAGE PYTHON;

-- Product Review (returns Product_name STRING, customer_review STRING)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table_productreview
AS 'ag_operators.agmap_table_productreview' LANGUAGE PYTHON;

-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Example 1: OLD WAY vs NEW WAY
--
-- OLD WAY (agmap with registry mode - returns JSON):
-- SELECT
--     customer_review,
--     JSON_VALUE(T.json_result, '$.sentiment_label') as sentiment_label,
--     CAST(JSON_VALUE(T.json_result, '$.sentiment_score') AS DOUBLE) as sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap(customer_review, 'Sentiment', 'registry')) AS T(json_result)
-- LIMIT 5;
--
-- NEW WAY (agmap_table - returns typed columns):
-- SELECT
--     customer_review,
--     T.sentiment_label,
--     T.sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
-- LIMIT 5;

-- Example 2: Filter with typed columns (no CAST needed!)
-- SELECT
--     Product_name,
--     customer_review,
--     T.sentiment_label,
--     T.sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
-- WHERE T.sentiment_label = 'positive'
--   AND T.sentiment_score > 0.8
-- LIMIT 10;

-- Example 3: Aggregations work seamlessly
-- SELECT
--     T.sentiment_label,
--     COUNT(*) as review_count,
--     AVG(T.sentiment_score) as avg_confidence
-- FROM pr,
-- LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
-- GROUP BY T.sentiment_label;

-- ============================================================================
-- Key Benefits
-- ============================================================================
-- ✅ No JSON parsing needed - get typed columns directly
-- ✅ No CAST required - types are correct from the start
-- ✅ Works in WHERE clauses without type conversion
-- ✅ Works in aggregations (AVG, SUM, etc.) without casting
-- ✅ Same arguments as agmap() but better output
-- ✅ Dynamically created in memory from registry schema
-- ✅ No code generation or file creation needed

-- Made with Bob
