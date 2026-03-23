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
    Product_name,
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
-- END OF CONSOLIDATED EXAMPLES
-- ============================================================================

-- Made with Bob
