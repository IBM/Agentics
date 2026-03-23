-- ============================================================================
-- Register AGMap and AGReduce UDFs in Flink SQL
-- ============================================================================
-- ⚠️ IMPORTANT: Flink SQL only accepts ONE statement at a time!
--
-- INSTRUCTIONS:
-- 1. Click "▶️ Start Terminal" to launch Flink SQL
-- 2. Copy ONE statement at a time from below
-- 3. Paste into Flink SQL terminal and press Enter
-- 4. Wait for "Function has been created" message
-- 5. Repeat for each statement (9 total)
--
-- OR use the "📝 Register UDFs" button (if it works in your environment)
-- ============================================================================

-- Dynamic Mode UDF (single field extraction with custom type)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap
AS 'ag_operators.agmap' LANGUAGE PYTHON;

-- Registry Mode UDTFs (multi-field extraction from Schema Registry)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_sentiment
AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_s
AS 'agmap_registry.agmap_s' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_productreview
AS 'agmap_registry.agmap_productreview' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_pr
AS 'agmap_registry.agmap_pr' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_product_reviews
AS 'agmap_registry.agmap_product_reviews' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_electronics_reviews
AS 'agmap_registry.agmap_electronics_reviews' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_test_generation
AS 'agmap_registry.agmap_test_generation' LANGUAGE PYTHON;

-- Aggregate Reduce UDF (aggregate all rows into single result)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agreduce
AS 'agreduce.agreduce' LANGUAGE PYTHON;

-- ============================================================================
-- Verify Registration
-- ============================================================================
-- After pasting the above, run this to see all registered functions:
-- SHOW FUNCTIONS;
--
-- Look for these functions in the output:
-- - agmap (dynamic mode)
-- - agreduce (aggregate reduce)
-- - agmap_sentiment (or agmap_s)
-- - agmap_productreview (or agmap_pr)
-- - agmap_product_reviews
-- - agmap_electronics_reviews
-- - agmap_test_generation
-- ============================================================================

-- ============================================================================
-- Quick Test
-- ============================================================================
-- After registration, test with simple queries:
--
-- Test agmap (row-by-row):
-- SELECT
--     Product_name,
--     sentiment_label,
--     sentiment_score
-- FROM pr,
-- LATERAL TABLE(agmap_sentiment(customer_review)) AS T(sentiment_label, sentiment_score)
-- LIMIT 5;
--
-- Test agreduce (aggregate all rows):
-- SELECT agreduce(customer_review, 'summary', 'str', 'Summarize all reviews') as summary
-- FROM pr;
-- ============================================================================

-- Made with Bob
