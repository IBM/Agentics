-- ============================================================================
-- Registry-Based agreduce Examples
-- ============================================================================
-- This file demonstrates usage of auto-generated agreduce functions
-- from agreduce_registry.py
--
-- These functions perform semantic aggregation (areduce) on all rows
-- to produce a single result with the target schema type.
-- ============================================================================

-- ============================================================================
-- Example 1: Aggregate Product Reviews to Single Summary
-- ============================================================================
-- Aggregate all product reviews into a single ProductReview summary

-- Method 1: Using WITH clause (CTE)
WITH aggregated AS (
    SELECT agreduce_productreview(customer_review) as agg_result
    FROM pr
)
SELECT
    JSON_VALUE(agg_result, '$.Product_name') as aggregated_product,
    JSON_VALUE(agg_result, '$.customer_review') as aggregated_review
FROM aggregated;

-- Method 2: Direct usage (simpler)
SELECT
    JSON_VALUE(agreduce_productreview(customer_review), '$.Product_name') as aggregated_product,
    JSON_VALUE(agreduce_productreview(customer_review), '$.customer_review') as aggregated_review
FROM pr;

-- ============================================================================
-- Example 2: Aggregate Sentiment Analysis
-- ============================================================================
-- Aggregate all sentiment analyses into overall sentiment

-- Using WITH clause
WITH sentiment_agg AS (
    SELECT agreduce_sentiment(customer_review) as agg_result
    FROM pr
)
SELECT
    JSON_VALUE(agg_result, '$.sentiment_label') as overall_sentiment,
    JSON_VALUE(agg_result, '$.sentiment_score') as overall_score
FROM sentiment_agg;

-- ============================================================================
-- Example 3: Aggregate with Filtering
-- ============================================================================
-- Aggregate only positive reviews

WITH positive_reviews AS (
    SELECT agreduce_pr(customer_review) as agg_result
    FROM pr
    WHERE customer_review LIKE '%good%' OR customer_review LIKE '%great%'
)
SELECT
    JSON_VALUE(agg_result, '$.Product_name') as product,
    JSON_VALUE(agg_result, '$.customer_review') as positive_summary
FROM positive_reviews;

-- ============================================================================
-- Example 4: Aggregate by Time Window (Tumbling Window)
-- ============================================================================
-- Aggregate reviews in 1-hour windows

WITH windowed_agg AS (
    SELECT
        TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
        TUMBLE_END(event_time, INTERVAL '1' HOUR) as window_end,
        agreduce_pr(customer_review) as agg_result
    FROM pr
    GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
)
SELECT
    window_start,
    window_end,
    JSON_VALUE(agg_result, '$.Product_name') as product,
    JSON_VALUE(agg_result, '$.customer_review') as hourly_summary
FROM windowed_agg;

-- ============================================================================
-- Example 5: Aggregate Electronics Reviews
-- ============================================================================
-- Aggregate all electronics reviews

WITH electronics_agg AS (
    SELECT agreduce_electronics_reviews(customer_review) as agg_result
    FROM pr
    WHERE Product_name LIKE '%electronics%'
)
SELECT
    JSON_VALUE(agg_result, '$.Product_name') as electronics_product,
    JSON_VALUE(agg_result, '$.customer_review') as electronics_summary
FROM electronics_agg;

-- ============================================================================
-- Example 6: Compare Multiple Aggregations
-- ============================================================================
-- Compare aggregations from different sources

WITH pr_agg AS (
    SELECT agreduce_productreview(customer_review) as agg_result
    FROM pr
),
elec_agg AS (
    SELECT agreduce_electronics_reviews(customer_review) as agg_result
    FROM pr
    WHERE Product_name LIKE '%electronics%'
)
SELECT
    'ProductReview' as source,
    JSON_VALUE(agg_result, '$.Product_name') as product,
    JSON_VALUE(agg_result, '$.customer_review') as summary
FROM pr_agg
UNION ALL
SELECT
    'Electronics' as source,
    JSON_VALUE(agg_result, '$.Product_name') as product,
    JSON_VALUE(agg_result, '$.customer_review') as summary
FROM elec_agg;

-- ============================================================================
-- Example 7: Aggregate with COUNT
-- ============================================================================
-- Show aggregated result with row count

WITH agg_with_count AS (
    SELECT
        COUNT(*) as total_reviews,
        agreduce_pr(customer_review) as agg_result
    FROM pr
)
SELECT
    total_reviews,
    JSON_VALUE(agg_result, '$.Product_name') as product,
    JSON_VALUE(agg_result, '$.customer_review') as aggregated_review
FROM agg_with_count;

-- ============================================================================
-- Example 8: Nested Aggregation with Sentiment
-- ============================================================================
-- First aggregate reviews, then analyze sentiment of the aggregation

WITH review_agg AS (
    SELECT agreduce_pr(customer_review) as agg_result
    FROM pr
),
sentiment_agg AS (
    SELECT agreduce_sentiment(
        JSON_VALUE(agg_result, '$.customer_review')
    ) as agg_result
    FROM review_agg
)
SELECT
    JSON_VALUE(agg_result, '$.sentiment_label') as aggregated_sentiment,
    JSON_VALUE(agg_result, '$.sentiment_score') as aggregated_score
FROM sentiment_agg;

-- ============================================================================
-- Example 9: Aggregate Test Generation Data
-- ============================================================================
-- Aggregate test generation reviews

WITH test_agg AS (
    SELECT agreduce_test_generation(customer_review) as agg_result
    FROM pr
    LIMIT 10
)
SELECT
    JSON_VALUE(agg_result, '$.Product_name') as test_product,
    JSON_VALUE(agg_result, '$.customer_review') as test_summary
FROM test_agg;

-- ============================================================================
-- Example 10: Aggregate with GROUP BY Product
-- ============================================================================
-- Aggregate reviews grouped by product name

WITH grouped_agg AS (
    SELECT
        Product_name,
        agreduce_pr(customer_review) as agg_result
    FROM pr
    GROUP BY Product_name
)
SELECT
    Product_name,
    JSON_VALUE(agg_result, '$.customer_review') as product_summary
FROM grouped_agg;

-- ============================================================================
-- Notes:
-- ============================================================================
-- 1. All agreduce functions return JSON strings
-- 2. Use JSON_VALUE() to extract specific fields from the result
-- 3. agreduce performs semantic aggregation using areduce transduction
-- 4. The aggregation considers all input rows to produce a single output
-- 5. Results are schema-aware based on the registered schema type
-- 6. Functions are auto-generated from Schema Registry schemas
-- ============================================================================

-- Made with Bob
