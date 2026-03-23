-- ============================================================================
-- AG Sentiment - Clean Signature Example
-- ============================================================================
-- This demonstrates the BEST approach: redesigned UDTF signature
-- No JSON, no complex parameters - just clean, simple function calls
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
-- STEP 2: Register Clean Signature UDF
-- ============================================================================

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS ag_sentiment
AS 'ag_operators.ag_sentiment' LANGUAGE PYTHON;

-- ============================================================================
-- STEP 3: Use with Clean, Simple Syntax
-- ============================================================================

-- Basic usage - just pass the text!
SELECT
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
LIMIT 5;

-- ============================================================================
-- COMPARISON: Evolution of Syntax
-- ============================================================================

-- APPROACH 1: Original agmap with registry (most verbose)
SELECT
    customer_review,
    JSON_VALUE(T.json_result, '$.sentiment_label') as sentiment_label,
    CAST(JSON_VALUE(T.json_result, '$.sentiment_score') AS DOUBLE) as sentiment_score
FROM pr,
LATERAL TABLE(agmap(customer_review, 'Sentiment', 'registry')) AS T(json_result)
LIMIT 5;

-- APPROACH 2: agmap_table_sentiment (better, but still verbose function name)
SELECT
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(agmap_table_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
LIMIT 5;

-- APPROACH 3: ag_sentiment (BEST - clean, simple, intuitive!)
SELECT
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
LIMIT 5;

-- ============================================================================
-- PRACTICAL EXAMPLES
-- ============================================================================

-- Example 1: Filter by sentiment
SELECT
    Product_name,
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
WHERE T.sentiment_label = 'positive'
  AND T.sentiment_score > 0.8
LIMIT 10;

-- Example 2: Aggregations
SELECT
    T.sentiment_label,
    COUNT(*) as review_count,
    AVG(T.sentiment_score) as avg_confidence,
    MIN(T.sentiment_score) as min_confidence,
    MAX(T.sentiment_score) as max_confidence
FROM pr,
LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY T.sentiment_label
ORDER BY review_count DESC;

-- Example 3: Product-level analysis
SELECT
    Product_name,
    T.sentiment_label,
    COUNT(*) as review_count,
    AVG(T.sentiment_score) as avg_sentiment_score
FROM pr,
LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY Product_name, T.sentiment_label
ORDER BY Product_name, review_count DESC;

-- Example 4: Create reusable view
CREATE TEMPORARY VIEW pr_with_sentiment AS
SELECT
    Product_name,
    customer_review,
    T.sentiment_label,
    T.sentiment_score
FROM pr,
LATERAL TABLE(ag_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

-- Now use the view with super clean syntax
SELECT * FROM pr_with_sentiment WHERE sentiment_label = 'positive';

SELECT sentiment_label, COUNT(*)
FROM pr_with_sentiment
GROUP BY sentiment_label;

-- ============================================================================
-- WHY THIS IS THE BEST APPROACH
-- ============================================================================
/*
1. ✅ Clean function name: ag_sentiment (not agmap_table_sentiment)
2. ✅ Simple signature: just pass the text
3. ✅ No JSON parsing needed
4. ✅ No configuration parameters
5. ✅ Typed columns from the start
6. ✅ Intuitive and easy to remember
7. ✅ Follows Flink best practices
8. ✅ Works with Flink's named parameters (1.19+)

DESIGN PRINCIPLE:
Instead of a generic JSON-driven operator with complex parameters,
create specialized but reusable functions with clean signatures.

This is the "redesign the UDTF signature" approach - the RIGHT way
to solve the problem in Flink SQL.
*/

-- ============================================================================
-- FUTURE: Named Parameters (Flink 1.19+)
-- ============================================================================
/*
With Flink 1.19+, you could even use named parameters:

SELECT T.sentiment_label, T.sentiment_score
FROM pr,
LATERAL TABLE(ag_sentiment(text => customer_review)) AS T(sentiment_label, sentiment_score);

This makes it even clearer what the parameter represents!
*/

-- Made with Bob
