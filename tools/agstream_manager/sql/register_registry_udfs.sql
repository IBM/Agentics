-- ============================================================================
-- Auto-Generated Registry UDF Registration
-- ============================================================================
-- This file registers all auto-generated UDFs from the Schema Registry
--
-- Generated UDTFs (row-by-row mapping) from agmap_registry.py
-- Generated UDAFs (aggregation) from agreduce_registry.py
--
-- Usage:
--   Run this file in Flink SQL to register all registry-based functions
-- ============================================================================

-- ============================================================================
-- UDTF Functions (agmap_registry.py) - Row-by-row mapping
-- ============================================================================

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_productreview
AS 'agmap_registry.agmap_productreview' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_pr
AS 'agmap_registry.agmap_pr' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_sentiment
AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_s
AS 'agmap_registry.agmap_s' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_test_generation
AS 'agmap_registry.agmap_test_generation' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_electronics_reviews
AS 'agmap_registry.agmap_electronics_reviews' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_product_reviews
AS 'agmap_registry.agmap_product_reviews' LANGUAGE PYTHON;

-- ============================================================================
-- UDAF Functions (agreduce_registry.py) - Aggregation
-- ============================================================================

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_productreview
AS 'agreduce_registry.agreduce_productreview' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_pr
AS 'agreduce_registry.agreduce_pr' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_sentiment
AS 'agreduce_registry.agreduce_sentiment' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_s
AS 'agreduce_registry.agreduce_s' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_test_generation
AS 'agreduce_registry.agreduce_test_generation' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_electronics_reviews
AS 'agreduce_registry.agreduce_electronics_reviews' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce_product_reviews
AS 'agreduce_registry.agreduce_product_reviews' LANGUAGE PYTHON;

-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Example 1: Using agmap_sentiment (UDTF - row-by-row)
-- SELECT T.sentiment_label, T.sentiment_score
-- FROM pr, LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

-- Example 2: Using agreduce_sentiment (UDAF - aggregation)
-- WITH sentiment_agg AS (
--     SELECT agreduce_sentiment(customer_review) as result
--     FROM pr
-- )
-- SELECT
--     JSON_VALUE(result, '$.sentiment_label') as overall_sentiment,
--     JSON_VALUE(result, '$.sentiment_score') as overall_score
-- FROM sentiment_agg;

-- Example 3: Using agreduce_pr to aggregate all product reviews
-- WITH product_agg AS (
--     SELECT agreduce_pr(customer_review) as result
--     FROM pr
-- )
-- SELECT
--     JSON_VALUE(result, '$.Product_name') as aggregated_product,
--     JSON_VALUE(result, '$.customer_review') as aggregated_review
-- FROM product_agg;

-- Made with Bob
