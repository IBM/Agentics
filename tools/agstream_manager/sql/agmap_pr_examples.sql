-- ============================================================================
-- AGMap UDF Examples for 'pr' (Product Reviews) Stream
-- ============================================================================
-- These examples work with the existing 'pr' Kafka topic that contains
-- product reviews with fields: Product_name, customer_review
--
-- ⚠️ IMPORTANT - FOLLOW THESE STEPS IN ORDER:
-- 1. Click "▶️ Start Terminal" button in the UI
-- 2. Click "📝 Register UDFs" button in the UI (REQUIRED!)
-- 3. Wait for success message: "Successfully registered X UDF functions"
-- 4. Copy and paste queries from this file into the Flink SQL terminal
--
-- If you get "No match found for function signature agmap" error:
-- → You forgot step 2! Click "📝 Register UDFs" button first.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: Create Source Table for 'pr' Stream
-- ----------------------------------------------------------------------------

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

-- ----------------------------------------------------------------------------
-- Example 1: Extract Sentiment from Customer Reviews (Dynamic Mode)
-- ----------------------------------------------------------------------------
-- Use agmap() to extract a single field with custom description

SELECT
    Product_name,
    customer_review,
    sentiment
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment', 'str', 'The sentiment: positive, negative, or neutral')) AS T(sentiment);

-- ----------------------------------------------------------------------------
-- Example 2: Extract Rating from Review Text (Dynamic Mode)
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    rating
FROM pr,
LATERAL TABLE(agmap(customer_review, 'rating', 'int', 'Extract star rating from 1 to 5 based on review sentiment')) AS T(rating);

-- ----------------------------------------------------------------------------
-- Example 3: Extract Sentiment with Confidence (Registry Mode)
-- ----------------------------------------------------------------------------
-- Use agmap_sentiment or agmap_s for structured sentiment analysis

SELECT
    Product_name,
    customer_review,
    sentiment_label,
    sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score);

-- Or use the short alias:
SELECT
    Product_name,
    sentiment_label,
    sentiment_score
FROM pr,
LATERAL TABLE(agmap_s(customer_review)) AS T(sentiment_label, sentiment_score);

-- ----------------------------------------------------------------------------
-- Example 4: Extract Multiple Custom Fields (Dynamic Mode)
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    s.sentiment,
    r.rating,
    k.key_phrase
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment', 'str', 'positive, negative, or neutral')) AS s(sentiment),
LATERAL TABLE(agmap(customer_review, 'rating', 'int', 'star rating 1-5')) AS r(rating),
LATERAL TABLE(agmap(customer_review, 'key_phrase', 'str', 'most important phrase from review')) AS k(key_phrase);

-- ----------------------------------------------------------------------------
-- Example 5: Filter Reviews by Sentiment
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    sentiment_label,
    sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
WHERE sentiment_label = 'positive' AND sentiment_score > 0.8;

-- ----------------------------------------------------------------------------
-- Example 6: Aggregate Sentiment by Product
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    COUNT(*) as total_reviews,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
    AVG(sentiment_score) as avg_sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY Product_name;

-- ----------------------------------------------------------------------------
-- Example 7: Extract Boolean Flags
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    would_recommend,
    mentions_price
FROM pr,
LATERAL TABLE(agmap(customer_review, 'would_recommend', 'bool', 'Would the reviewer recommend this product?')) AS r(would_recommend),
LATERAL TABLE(agmap(customer_review, 'mentions_price', 'bool', 'Does the review mention price or value?')) AS p(mentions_price);

-- ----------------------------------------------------------------------------
-- Example 8: Extract Numeric Scores
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    quality_score,
    value_score
FROM pr,
LATERAL TABLE(agmap(customer_review, 'quality_score', 'float', 'Quality rating from 0.0 to 10.0')) AS q(quality_score),
LATERAL TABLE(agmap(customer_review, 'value_score', 'float', 'Value for money from 0.0 to 10.0')) AS v(value_score);

-- ----------------------------------------------------------------------------
-- Example 9: Create Enriched Output Stream
-- ----------------------------------------------------------------------------

-- First, create the output table
CREATE TABLE IF NOT EXISTS pr_enriched (
    Product_name STRING,
    customer_review STRING,
    sentiment_label STRING,
    sentiment_score DOUBLE,
    rating INT,
    key_phrase STRING,
    would_recommend BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = 'pr-enriched',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);

-- Then, insert enriched data
INSERT INTO pr_enriched
SELECT
    Product_name,
    customer_review,
    s.sentiment_label,
    s.sentiment_score,
    r.rating,
    k.key_phrase,
    w.would_recommend
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS s(sentiment_label, sentiment_score),
LATERAL TABLE(agmap(customer_review, 'rating', 'int', 'star rating 1-5')) AS r(rating),
LATERAL TABLE(agmap(customer_review, 'key_phrase', 'str', 'key phrase')) AS k(key_phrase),
LATERAL TABLE(agmap(customer_review, 'would_recommend', 'bool', 'would recommend?')) AS w(would_recommend);

-- ----------------------------------------------------------------------------
-- Example 10: Real-time Sentiment Monitoring with Windows
-- ----------------------------------------------------------------------------

SELECT
    TUMBLE_START(PROCTIME(), INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(PROCTIME(), INTERVAL '1' MINUTE) as window_end,
    Product_name,
    COUNT(*) as review_count,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count,
    AVG(sentiment_score) as avg_sentiment
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
GROUP BY
    TUMBLE(PROCTIME(), INTERVAL '1' MINUTE),
    Product_name;

-- ----------------------------------------------------------------------------
-- Example 11: Alert on Negative Reviews
-- ----------------------------------------------------------------------------

-- Create alerts table
CREATE TABLE IF NOT EXISTS pr_negative_alerts (
    Product_name STRING,
    customer_review STRING,
    sentiment_label STRING,
    sentiment_score DOUBLE,
    alert_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pr-negative-alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- Insert negative reviews into alerts
INSERT INTO pr_negative_alerts
SELECT
    Product_name,
    customer_review,
    sentiment_label,
    sentiment_score,
    CURRENT_TIMESTAMP as alert_time
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
WHERE sentiment_label = 'negative' AND sentiment_score < 0.3;

-- ----------------------------------------------------------------------------
-- Example 12: Product Comparison Dashboard
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    COUNT(*) as total_reviews,
    AVG(CAST(rating AS DOUBLE)) as avg_rating,
    AVG(sentiment_score) as avg_sentiment,
    SUM(CASE WHEN would_recommend THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as recommend_percentage
FROM pr,
LATERAL TABLE(agmap(customer_review, 'rating', 'int', 'rating 1-5')) AS r(rating),
LATERAL TABLE(agmap_sentiment(customer_review)) AS s(sentiment_label, sentiment_score),
LATERAL TABLE(agmap(customer_review, 'would_recommend', 'bool', 'would recommend')) AS w(would_recommend)
GROUP BY Product_name
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC;

-- ----------------------------------------------------------------------------
-- Example 13: Extract Product Categories
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    category,
    COUNT(*) OVER (PARTITION BY category) as category_count
FROM pr,
LATERAL TABLE(agmap(customer_review, 'category', 'str', 'Product category: electronics, clothing, books, home, sports, etc.')) AS c(category);

-- ----------------------------------------------------------------------------
-- Example 14: Identify Review Topics
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    main_topic,
    mentions_quality,
    mentions_price,
    mentions_shipping
FROM pr,
LATERAL TABLE(agmap(customer_review, 'main_topic', 'str', 'Main topic: quality, price, shipping, customer service, etc.')) AS t(main_topic),
LATERAL TABLE(agmap(customer_review, 'mentions_quality', 'bool', 'Mentions product quality')) AS q(mentions_quality),
LATERAL TABLE(agmap(customer_review, 'mentions_price', 'bool', 'Mentions price')) AS p(mentions_price),
LATERAL TABLE(agmap(customer_review, 'mentions_shipping', 'bool', 'Mentions shipping')) AS s(mentions_shipping);

-- ----------------------------------------------------------------------------
-- Example 15: Simple View of Latest Reviews
-- ----------------------------------------------------------------------------

SELECT
    Product_name,
    customer_review,
    sentiment_label,
    sentiment_score
FROM pr,
LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
LIMIT 10;

-- ============================================================================
-- TESTING
-- ============================================================================

-- To test these queries, you can produce sample messages to the 'pr' topic:
--
-- Using kafka-avro-console-producer:
-- kafka-avro-console-producer \
--   --broker-list kafka:9092 \
--   --topic pr \
--   --property value.schema='{"type":"record","name":"ProductReview","fields":[{"name":"Product_name","type":"string"},{"name":"customer_review","type":"string"}]}' \
--   --property schema.registry.url=http://karapace-schema-registry:8081
--
-- Then enter JSON messages like:
-- {"Product_name":"Wireless Headphones","customer_review":"Amazing sound quality! Very comfortable and great battery life. Highly recommend!"}
-- {"Product_name":"Wireless Headphones","customer_review":"Terrible product. Broke after one week. Do not buy!"}
-- {"Product_name":"Smart Watch","customer_review":"Good value for money. Works well but battery could be better."}

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. All UDFs use LLM-powered extraction - results may vary slightly
-- 2. Processing time depends on review length and LLM API response time
-- 3. Use INSERT INTO for production pipelines (better parallelism)
-- 4. Use SELECT with LIMIT for testing and exploration
-- 5. Combine dynamic mode (agmap) and registry mode (agmap_sentiment) as needed
-- 6. Monitor LLM API costs for high-volume streams
-- ============================================================================

-- Made with Bob
