-- ============================================================================
-- Test SQL for Row-Level Sentiment Analysis UDF
-- ============================================================================

-- Step 1: Create a test table with sample data
CREATE TABLE test_messages (
    id BIGINT,
    title STRING,
    content STRING,
    author STRING,
    rating INT,
    timestamp_val TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10',
    'fields.title.length' = '20',
    'fields.content.length' = '100',
    'fields.author.length' = '15',
    'fields.rating.min' = '1',
    'fields.rating.max' = '5'
);

-- Step 2: Load the UDF (make sure it's installed first)
-- Run: ./scripts/install_udfs.sh

-- Step 3: Test with manual JSON construction (original method)
SELECT
    id,
    title,
    analyze_row_sentiment(
        JSON_OBJECT(
            'id', id,
            'title', title,
            'content', content,
            'author', author,
            'rating', rating
        )
    ) as sentiment_json
FROM test_messages
LIMIT 5;

-- Step 4: Test with CAST(ROW(*) AS STRING) - simplified method
SELECT
    id,
    title,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment_json
FROM test_messages
LIMIT 5;

-- Step 5: Extract sentiment label from result
SELECT
    id,
    title,
    extract_sentiment_label(
        analyze_row_sentiment(CAST(ROW(*) AS STRING))
    ) as sentiment_label,
    extract_confidence(
        analyze_row_sentiment(CAST(ROW(*) AS STRING))
    ) as confidence
FROM test_messages
LIMIT 5;

-- Step 6: Create a view for easier querying
CREATE VIEW messages_with_sentiment AS
SELECT
    *,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment_json,
    extract_sentiment_label(
        analyze_row_sentiment(CAST(ROW(*) AS STRING))
    ) as sentiment_label
FROM test_messages;

-- Step 7: Query the view
SELECT * FROM messages_with_sentiment LIMIT 5;

-- Step 8: Filter by sentiment
SELECT *
FROM messages_with_sentiment
WHERE sentiment_label = 'POSITIVE'
LIMIT 5;

-- Step 9: Test with real Kafka data (if you have a Kafka topic)
/*
CREATE TABLE kafka_messages (
    id BIGINT,
    title STRING,
    content STRING,
    author STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'messages',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'test-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Analyze sentiment on Kafka stream
SELECT
    id,
    title,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment_json
FROM kafka_messages;
*/

-- Step 10: Test with parallelism
SET 'parallelism.default' = '4';

SELECT
    id,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment
FROM test_messages
LIMIT 20;

-- Step 11: Aggregate sentiment statistics
SELECT
    extract_sentiment_label(
        analyze_row_sentiment(CAST(ROW(*) AS STRING))
    ) as sentiment,
    COUNT(*) as count
FROM test_messages
GROUP BY extract_sentiment_label(
    analyze_row_sentiment(CAST(ROW(*) AS STRING))
);

-- Step 12: Test with specific columns only (more efficient)
SELECT
    id,
    analyze_row_sentiment(
        CAST(ROW(title, content) AS STRING)
    ) as sentiment
FROM test_messages
LIMIT 5;

-- Cleanup
-- DROP VIEW IF EXISTS messages_with_sentiment;
-- DROP TABLE IF EXISTS test_messages;

-- Made with Bob
