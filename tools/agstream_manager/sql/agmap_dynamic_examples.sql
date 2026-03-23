-- ============================================================================
-- AGMAP DYNAMIC UDF - SQL Examples
-- ============================================================================
-- This file contains examples for using the agmap() UDF with dynamic type generation.
-- The agmap() function creates types on-the-fly without needing Schema Registry.
--
-- Function Signature:
--   agmap(input_data, column_name, field_type='str', description=None)
--
-- Parameters:
--   1. input_data (required): The text/data to process
--   2. column_name (required): Name for the generated column
--   3. field_type (optional): Type of the field - 'str', 'int', 'float', 'bool' (default: 'str')
--   4. description (optional): Description to guide the LLM extraction (default: None)
--
-- ============================================================================

-- First, register the UDF (only needed once per session)
CREATE TEMPORARY SYSTEM FUNCTION agmap AS 'ag_operators.agmap' LANGUAGE PYTHON;

-- ============================================================================
-- EXAMPLE 1: Basic sentiment extraction (default string type)
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.sentiment
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment')) AS T(sentiment)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 2: Extract rating as float
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.rating
FROM pr,
LATERAL TABLE(agmap(customer_review, 'rating', 'float')) AS T(rating)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 3: Extract rating as integer
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.rating
FROM pr,
LATERAL TABLE(agmap(customer_review, 'rating', 'int')) AS T(rating)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 4: Extract boolean flag (is_positive)
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.is_positive
FROM pr,
LATERAL TABLE(agmap(customer_review, 'is_positive', 'bool')) AS T(is_positive)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 5: Extract with description to guide LLM
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.emotion
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'emotion',
    'str',
    'Primary emotion expressed: joy, anger, sadness, fear, surprise, or neutral'
)) AS T(emotion)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 6: Extract product category with description
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.category
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'category',
    'str',
    'Product category: electronics, clothing, food, books, or other'
)) AS T(category)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 7: Extract urgency level as integer (1-5 scale)
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.urgency
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'urgency',
    'int',
    'Urgency level from 1 (low) to 5 (critical)'
)) AS T(urgency)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 8: Extract confidence score as float
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.confidence
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'confidence',
    'float',
    'Confidence score between 0.0 and 1.0'
)) AS T(confidence)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 9: Multiple extractions in one query
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T1.sentiment,
    T2.rating,
    T3.is_positive
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment')) AS T1(sentiment),
LATERAL TABLE(agmap(customer_review, 'rating', 'float')) AS T2(rating),
LATERAL TABLE(agmap(customer_review, 'is_positive', 'bool')) AS T3(is_positive)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 10: Create a view with extracted fields
-- ============================================================================
CREATE VIEW enriched_reviews AS
SELECT
    pr_id,
    customer_review,
    T1.sentiment,
    T2.rating
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment')) AS T1(sentiment),
LATERAL TABLE(agmap(customer_review, 'rating', 'float')) AS T2(rating);

-- Query the view
SELECT * FROM enriched_reviews LIMIT 5;

-- ============================================================================
-- EXAMPLE 11: Filter based on extracted field
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.sentiment
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment')) AS T(sentiment)
WHERE T.sentiment = 'positive'
LIMIT 5;

-- ============================================================================
-- EXAMPLE 12: Aggregate by extracted field
-- ============================================================================
SELECT
    T.sentiment,
    COUNT(*) as review_count
FROM pr,
LATERAL TABLE(agmap(customer_review, 'sentiment')) AS T(sentiment)
GROUP BY T.sentiment;

-- ============================================================================
-- EXAMPLE 13: Extract topic/theme
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.topic
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'topic',
    'str',
    'Main topic: quality, price, shipping, customer_service, or features'
)) AS T(topic)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 14: Extract language code
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.language
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'language',
    'str',
    'ISO 639-1 language code (e.g., en, es, fr, de)'
)) AS T(language)
LIMIT 5;

-- ============================================================================
-- EXAMPLE 15: Extract has_complaint flag
-- ============================================================================
SELECT
    pr_id,
    customer_review,
    T.has_complaint
FROM pr,
LATERAL TABLE(agmap(
    customer_review,
    'has_complaint',
    'bool',
    'Does the review contain a complaint or negative feedback?'
)) AS T(has_complaint)
WHERE T.has_complaint = true
LIMIT 5;

-- ============================================================================
-- NOTES:
-- 1. The agmap() function generates types dynamically - no Schema Registry needed
-- 2. Use field_type parameter to specify the output type: 'str', 'int', 'float', 'bool'
-- 3. Use description parameter to guide the LLM on what to extract
-- 4. Default field_type is 'str' if not specified
-- 5. For multiple fields from one schema, use 'registry' mode instead
-- ============================================================================

-- Made with Bob
