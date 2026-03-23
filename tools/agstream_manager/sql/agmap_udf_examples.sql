-- ============================================================================
-- AGMap UDF Sample Queries
-- ============================================================================
-- This file contains example queries demonstrating how to use AGMap UDFs
-- after registering them with the "Register UDFs" button in the UI.
--
-- Prerequisites:
-- 1. Start Flink terminal using "▶️ Start Terminal" button
-- 2. Click "📝 Register UDFs" to register all UDF functions
-- 3. Create source tables (see examples below)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DYNAMIC MODE: agmap() - Single Field Extraction
-- ----------------------------------------------------------------------------
-- The agmap() function dynamically creates a type with one field on-the-fly.
-- Syntax: agmap(input_data, column_name, [field_type], [description])
-- Field types: 'str' (default), 'int', 'float', 'bool'

-- Example 1: Extract sentiment from product reviews (string output)
CREATE TABLE product_reviews (
    review_id STRING,
    review_text STRING,
    product_name STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'product-reviews',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'review-processor',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- Query: Extract sentiment as a string
SELECT
    review_id,
    product_name,
    review_text,
    sentiment
FROM product_reviews,
LATERAL TABLE(agmap(review_text, 'sentiment', 'str', 'The sentiment of the review: positive, negative, or neutral')) AS T(sentiment);

-- Example 2: Extract rating from text (integer output)
SELECT
    review_id,
    product_name,
    rating
FROM product_reviews,
LATERAL TABLE(agmap(review_text, 'rating', 'int', 'Extract the star rating from 1 to 5')) AS T(rating);

-- Example 3: Extract price from description (float output)
CREATE TABLE product_descriptions (
    product_id STRING,
    description STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'product-descriptions',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'price-extractor',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

SELECT
    product_id,
    description,
    price
FROM product_descriptions,
LATERAL TABLE(agmap(description, 'price', 'float', 'Extract the price in dollars')) AS T(price);

-- Example 4: Extract boolean flag
SELECT
    review_id,
    is_verified
FROM product_reviews,
LATERAL TABLE(agmap(review_text, 'is_verified', 'bool', 'Is this a verified purchase?')) AS T(is_verified);

-- ----------------------------------------------------------------------------
-- REGISTRY MODE: Pre-defined Multi-Field UDTFs
-- ----------------------------------------------------------------------------
-- These functions extract multiple fields based on schemas in Schema Registry.
-- Each function returns all fields defined in its corresponding schema.

-- Example 5: agmap_sentiment - Extract sentiment analysis fields
-- Schema fields: sentiment (str), confidence (float)
SELECT
    review_id,
    product_name,
    sentiment,
    confidence
FROM product_reviews,
LATERAL TABLE(agmap_sentiment(review_text)) AS T(sentiment, confidence);

-- Example 6: agmap_productreview - Full product review analysis
-- Schema fields: rating (int), sentiment (str), summary (str), pros (str), cons (str)
SELECT
    review_id,
    product_name,
    rating,
    sentiment,
    summary,
    pros,
    cons
FROM product_reviews,
LATERAL TABLE(agmap_productreview(review_text)) AS T(rating, sentiment, summary, pros, cons);

-- Example 7: agmap_electronics_reviews - Electronics-specific review analysis
-- Schema fields: rating (int), sentiment (str), build_quality (str), value_for_money (str), would_recommend (bool)
SELECT
    review_id,
    product_name,
    rating,
    sentiment,
    build_quality,
    value_for_money,
    would_recommend
FROM product_reviews,
LATERAL TABLE(agmap_electronics_reviews(review_text)) AS T(rating, sentiment, build_quality, value_for_money, would_recommend);

-- ----------------------------------------------------------------------------
-- ADVANCED QUERIES: Combining Multiple UDFs
-- ----------------------------------------------------------------------------

-- Example 8: Extract multiple insights from the same text
SELECT
    review_id,
    product_name,
    review_text,
    s.sentiment AS sentiment_label,
    s.confidence AS sentiment_confidence,
    r.rating AS extracted_rating,
    k.key_phrase AS key_phrase
FROM product_reviews,
LATERAL TABLE(agmap_sentiment(review_text)) AS s(sentiment, confidence),
LATERAL TABLE(agmap(review_text, 'rating', 'int', 'Star rating 1-5')) AS r(rating),
LATERAL TABLE(agmap(review_text, 'key_phrase', 'str', 'Most important phrase')) AS k(key_phrase);

-- Example 9: Filter based on extracted fields
SELECT
    review_id,
    product_name,
    sentiment,
    confidence
FROM product_reviews,
LATERAL TABLE(agmap_sentiment(review_text)) AS T(sentiment, confidence)
WHERE sentiment = 'positive' AND confidence > 0.8;

-- Example 10: Aggregate extracted data
SELECT
    product_name,
    COUNT(*) as review_count,
    AVG(CAST(rating AS DOUBLE)) as avg_rating,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count
FROM product_reviews,
LATERAL TABLE(agmap_productreview(review_text)) AS T(rating, sentiment, summary, pros, cons)
GROUP BY product_name;

-- ----------------------------------------------------------------------------
-- STREAMING QUERIES: Real-time Processing
-- ----------------------------------------------------------------------------

-- Example 11: Real-time sentiment monitoring with tumbling window
SELECT
    TUMBLE_START(rowtime, INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(rowtime, INTERVAL '1' MINUTE) as window_end,
    product_name,
    COUNT(*) as review_count,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
    AVG(confidence) as avg_confidence
FROM product_reviews,
LATERAL TABLE(agmap_sentiment(review_text)) AS T(sentiment, confidence)
GROUP BY
    TUMBLE(rowtime, INTERVAL '1' MINUTE),
    product_name;

-- Example 12: Alert on negative reviews with high confidence
CREATE TABLE negative_review_alerts (
    review_id STRING,
    product_name STRING,
    review_text STRING,
    sentiment STRING,
    confidence DOUBLE,
    alert_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'negative-review-alerts',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO negative_review_alerts
SELECT
    review_id,
    product_name,
    review_text,
    sentiment,
    confidence,
    CURRENT_TIMESTAMP as alert_time
FROM product_reviews,
LATERAL TABLE(agmap_sentiment(review_text)) AS T(sentiment, confidence)
WHERE sentiment = 'negative' AND confidence > 0.85;

-- ----------------------------------------------------------------------------
-- MIXED MODE: Combining Dynamic and Registry UDFs
-- ----------------------------------------------------------------------------

-- Example 13: Use registry UDF for structured data, dynamic for custom fields
SELECT
    review_id,
    product_name,
    pr.rating,
    pr.sentiment,
    pr.summary,
    c.category,
    t.topic
FROM product_reviews,
LATERAL TABLE(agmap_productreview(review_text)) AS pr(rating, sentiment, summary, pros, cons),
LATERAL TABLE(agmap(review_text, 'category', 'str', 'Product category: electronics, clothing, books, etc.')) AS c(category),
LATERAL TABLE(agmap(review_text, 'topic', 'str', 'Main topic discussed')) AS t(topic);

-- ----------------------------------------------------------------------------
-- CREATING OUTPUT TABLES
-- ----------------------------------------------------------------------------

-- Example 14: Create enriched review table with extracted insights
CREATE TABLE enriched_reviews (
    review_id STRING,
    product_name STRING,
    original_text STRING,
    rating INT,
    sentiment STRING,
    summary STRING,
    pros STRING,
    cons STRING,
    confidence DOUBLE,
    processing_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched-reviews',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO enriched_reviews
SELECT
    review_id,
    product_name,
    review_text as original_text,
    pr.rating,
    pr.sentiment,
    pr.summary,
    pr.pros,
    pr.cons,
    s.confidence,
    CURRENT_TIMESTAMP as processing_time
FROM product_reviews,
LATERAL TABLE(agmap_productreview(review_text)) AS pr(rating, sentiment, summary, pros, cons),
LATERAL TABLE(agmap_sentiment(review_text)) AS s(sentiment, confidence);

-- ----------------------------------------------------------------------------
-- TESTING QUERIES
-- ----------------------------------------------------------------------------

-- Example 15: Test with sample data
-- First, produce a test message to the product-reviews topic:
-- {"review_id": "R001", "product_name": "Wireless Headphones", "review_text": "These headphones are amazing! Great sound quality and comfortable to wear. Battery lasts all day. Highly recommend!"}

-- Then run this query to see the extraction:
SELECT
    review_id,
    product_name,
    rating,
    sentiment,
    summary
FROM product_reviews,
LATERAL TABLE(agmap_productreview(review_text)) AS T(rating, sentiment, summary, pros, cons)
LIMIT 5;

-- ----------------------------------------------------------------------------
-- NOTES
-- ----------------------------------------------------------------------------
-- 1. All UDFs use LLM-powered extraction, so results may vary slightly
-- 2. Processing time depends on input text length and LLM response time
-- 3. For production use, consider adding error handling and retry logic
-- 4. Monitor LLM API costs when processing high-volume streams
-- 5. Use appropriate field types in dynamic mode for better type safety
-- 6. Registry mode UDFs are faster as they don't need runtime type generation
-- ============================================================================

-- Made with Bob
