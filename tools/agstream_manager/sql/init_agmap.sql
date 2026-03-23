-- ============================================
-- AGmap UDF Initialization for Flink SQL
-- ============================================
-- This file registers the agmap UDF and creates necessary tables
-- for joke generation from questions.
--
-- Usage in Flink SQL Client:
--   Copy and paste this entire file into the Flink SQL UI
--
-- ============================================

-- Set parallelism for better performance
SET 'parallelism.default' = '20';

-- Register the agmap UDF for semantic transformations
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap
AS 'agmap.agmap' LANGUAGE PYTHON;

-- ============================================
-- Table Definitions
-- ============================================

-- Q (Questions) table
CREATE TABLE IF NOT EXISTS Q (
  text STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'Q',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);

-- J (Jokes) table
CREATE TABLE IF NOT EXISTS J (
  joke_text STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'J',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);

-- ============================================
-- Ready to Use!
-- ============================================
-- The agmap function is now registered and ready to use.
--
-- IMPORTANT: For maximum parallelism, use INSERT INTO instead of SELECT with LIMIT
-- SELECT queries with LIMIT force parallelism to 1 for the Python UDF operator
--
-- Example queries:
--
-- 1. Generate jokes from all questions (BEST PERFORMANCE - uses parallelism 20):
--    INSERT INTO J
--    SELECT JSON_VALUE(agmap('Joke', text), '$.joke_text') as joke_text
--    FROM Q;
--
-- 2. View generated jokes:
--    SELECT * FROM J LIMIT 10;
--
-- 3. Test with a single question (slower - parallelism 1):
--    SELECT agmap('Joke', text) FROM Q LIMIT 1;
--
-- 4. See question and joke together (slower - parallelism 1):
--    SELECT
--      text as question,
--      JSON_VALUE(agmap('Joke', text), '$.joke_text') as joke
--    FROM Q LIMIT 5;
--
-- ============================================

-- Made with Bob
