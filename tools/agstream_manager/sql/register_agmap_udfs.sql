-- Register AGMap UDTFs in Flink SQL
-- Run this after rebuilding UDFs and restarting Flink

-- ============================================
-- Dynamic Mode UDF (Single Field Extraction)
-- ============================================

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap
AS 'ag_operators.agmap' LANGUAGE PYTHON;

-- Usage:
-- SELECT agmap(text, 'field_name', 'str', 'description') FROM table;


-- ============================================
-- Registry Mode UDTFs (Multi-Field Extraction)
-- ============================================

-- Sentiment Analysis (2 fields)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_sentiment
AS 'agmap_registry.agmap_sentiment' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_s
AS 'agmap_registry.agmap_s' LANGUAGE PYTHON;

-- Product Reviews (2 fields)
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


-- ============================================
-- Usage Examples
-- ============================================

-- Example 1: Dynamic mode - extract single field
-- SELECT
--     review_text,
--     agmap(review_text, 'sentiment', 'str', 'positive, negative, or neutral') as sentiment
-- FROM reviews;

-- Example 2: Registry mode - extract multiple fields (sentiment)
-- SELECT
--     review_text,
--     s.sentiment_label,
--     s.sentiment_score
-- FROM reviews,
-- LATERAL TABLE(agmap_sentiment(review_text)) AS s(sentiment_label, sentiment_score);

-- Example 3: Registry mode - extract product info
-- SELECT
--     raw_data,
--     p.Product_name,
--     p.customer_review
-- FROM raw_table,
-- LATERAL TABLE(agmap_productreview(raw_data)) AS p(Product_name, customer_review);

-- Example 4: Create structured output table
-- INSERT INTO structured_reviews
-- SELECT
--     id,
--     s.sentiment_label,
--     s.sentiment_score
-- FROM raw_reviews,
-- LATERAL TABLE(agmap_sentiment(review_text)) AS s(sentiment_label, sentiment_score);

-- Made with Bob
