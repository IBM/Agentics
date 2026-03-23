-- ============================================================================
-- Dynamic Registry UDF Registration
-- ============================================================================
-- This file registers UDFs that dynamically fetch schemas from the registry
-- and return typed columns directly (not JSON).
--
-- These UDFs provide the same convenience as auto-generated registry UDFs
-- but work dynamically without code generation.
-- ============================================================================

-- ============================================================================
-- Dynamic Registry UDTFs - Fetch schema at runtime
-- ============================================================================

-- Sentiment Analysis (dynamically fetches Sentiment schema)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_sentiment_dynamic
AS 'ag_operators.create_agmap_sentiment' LANGUAGE PYTHON;

-- Product Review (dynamically fetches ProductReview schema)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_productreview_dynamic
AS 'ag_operators.create_agmap_productreview' LANGUAGE PYTHON;

-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Example 1: Using agmap_sentiment_dynamic (returns typed columns directly)
-- This is equivalent to agmap_sentiment but fetches schema dynamically
--
-- SELECT
--     customer_review,
--     T.sentiment_label,
--     T.sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap_sentiment_dynamic(customer_review)) AS T(sentiment_label, sentiment_score)
-- LIMIT 5;

-- Example 2: Compare with old agmap registry mode (returns JSON)
-- OLD WAY (requires JSON parsing):
-- SELECT
--     customer_review,
--     JSON_VALUE(T.json_result, '$.sentiment_label') as sentiment_label,
--     CAST(JSON_VALUE(T.json_result, '$.sentiment_score') AS DOUBLE) as sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap(customer_review, 'Sentiment', 'registry')) AS T(json_result)
-- LIMIT 5;
--
-- NEW WAY (typed columns directly):
-- SELECT
--     customer_review,
--     T.sentiment_label,
--     T.sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap_sentiment_dynamic(customer_review)) AS T(sentiment_label, sentiment_score)
-- LIMIT 5;

-- Example 3: Filter by sentiment (works with typed columns)
-- SELECT
--     Product_name,
--     customer_review,
--     T.sentiment_label,
--     T.sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap_sentiment_dynamic(customer_review)) AS T(sentiment_label, sentiment_score)
-- WHERE T.sentiment_label = 'positive'
--   AND T.sentiment_score > 0.8
-- LIMIT 10;

-- Example 4: Aggregate by sentiment
-- SELECT
--     T.sentiment_label,
--     COUNT(*) as review_count,
--     AVG(T.sentiment_score) as avg_score
-- FROM pr,
-- LATERAL TABLE(agmap_sentiment_dynamic(customer_review)) AS T(sentiment_label, sentiment_score)
-- GROUP BY T.sentiment_label;

-- ============================================================================
-- Benefits of Dynamic Registry UDFs
-- ============================================================================
-- 1. No JSON parsing needed - get typed columns directly
-- 2. No code generation required - works with any schema in registry
-- 3. Same convenience as auto-generated UDFs
-- 4. Schemas are fetched at runtime from Schema Registry
-- 5. Easy to use in WHERE clauses and aggregations

-- Made with Bob
