-- AGsearch Examples
-- Demonstrates vector search operations on streaming data using AG's vector store

-- ============================================================================
-- EXAMPLE 1: Basic Search on Product Reviews
-- ============================================================================
-- Search for reviews mentioning "great service" and return top 10 results
SELECT agsearch(review_text, 'great service', 10) as search_results
FROM pr;

-- ============================================================================
-- EXAMPLE 2: Search with Custom K Value
-- ============================================================================
-- Search for bug reports and return only top 5 most relevant
SELECT agsearch(issue_description, 'bug report', 5) as bug_reports
FROM issues;

-- ============================================================================
-- EXAMPLE 3: Search Customer Feedback
-- ============================================================================
-- Find feedback related to "delivery problems"
SELECT agsearch(feedback, 'delivery problems', 10) as delivery_issues
FROM customer_feedback;

-- ============================================================================
-- EXAMPLE 4: Search with Window Function
-- ============================================================================
-- Search within a time window (last hour)
SELECT agsearch(message, 'error occurred', 20) as error_messages
FROM logs
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- ============================================================================
-- EXAMPLE 5: Combined with Other Aggregations
-- ============================================================================
-- Get search results along with count
SELECT
    COUNT(*) as total_rows,
    agsearch(content, 'machine learning', 10) as ml_related
FROM articles;

-- ============================================================================
-- EXAMPLE 6: Search Across Multiple Categories
-- ============================================================================
-- Search for specific topics in different categories
SELECT
    category,
    agsearch(description, 'performance optimization', 5) as relevant_items
FROM products
GROUP BY category;

-- ============================================================================
-- EXAMPLE 7: Parse JSON Results
-- ============================================================================
-- Extract individual results from the JSON array
-- Note: This requires JSON parsing functions available in your Flink version
SELECT
    search_results
FROM (
    SELECT agsearch(review_text, 'excellent product', 10) as search_results
    FROM pr
);

-- ============================================================================
-- EXAMPLE 8: Search with Filtering
-- ============================================================================
-- Search only within high-rated reviews
SELECT agsearch(review_text, 'quality', 10) as quality_reviews
FROM pr
WHERE rating >= 4;

-- ============================================================================
-- EXAMPLE 9: Multiple Search Queries
-- ============================================================================
-- Run different searches on the same data
SELECT
    agsearch(comment, 'positive feedback', 5) as positive,
    agsearch(comment, 'negative feedback', 5) as negative
FROM comments;

-- ============================================================================
-- EXAMPLE 10: Search with Tumbling Window
-- ============================================================================
-- Search within 5-minute tumbling windows
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
    agsearch(log_message, 'warning', 10) as warnings
FROM system_logs
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);

-- ============================================================================
-- EXAMPLE 11: Explode Search Results into Multiple Rows
-- ============================================================================
-- Use explode_search_results UDTF to get each result as a separate row
SELECT
    text,
    idx
FROM (
    SELECT agsearch(review_text, 'great service', 10) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx);

-- ============================================================================
-- EXAMPLE 12: Explode with Additional Processing
-- ============================================================================
-- Get search results as rows and add computed columns
SELECT
    text,
    idx,
    LENGTH(text) as text_length,
    UPPER(SUBSTRING(text, 1, 50)) as preview
FROM (
    SELECT agsearch(review_text, 'excellent product', 5) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx)
ORDER BY idx;

-- ============================================================================
-- EXAMPLE 13: Explode with Filtering
-- ============================================================================
-- Get only the top 3 results by filtering on index
SELECT
    text,
    idx
FROM (
    SELECT agsearch(review_text, 'quality', 10) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx)
WHERE idx < 3;

-- ============================================================================
-- EXAMPLE 14: Explode with Aggregation
-- ============================================================================
-- Count how many results contain specific keywords
SELECT
    COUNT(*) as total_results,
    SUM(CASE WHEN text LIKE '%excellent%' THEN 1 ELSE 0 END) as contains_excellent,
    SUM(CASE WHEN text LIKE '%poor%' THEN 1 ELSE 0 END) as contains_poor
FROM (
    SELECT agsearch(review_text, 'product quality', 20) as search_results
    FROM pr
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx);

-- ============================================================================
-- EXAMPLE 15: Multiple Searches with Explode
-- ============================================================================
-- Perform multiple searches and explode each
SELECT
    'positive' as search_type,
    text,
    idx
FROM (
    SELECT agsearch(comment, 'positive feedback', 5) as search_results
    FROM comments
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx)

UNION ALL

SELECT
    'negative' as search_type,
    text,
    idx
FROM (
    SELECT agsearch(comment, 'negative feedback', 5) as search_results
    FROM comments
),
LATERAL TABLE(explode_search_results(search_results)) AS T(text, idx);


-- ============================================================================
-- NOTES:
-- ============================================================================
-- 1. The agsearch function returns a JSON array of results
-- 2. Each result contains: {"text": "...", "index": N}
-- 3. Results are ordered by relevance (most relevant first)
-- 4. The function uses AG's vector store with sentence transformers
-- 5. First execution may be slow due to model download
-- 6. Subsequent executions will be faster as the model is cached
-- 7. The max_k parameter defaults to 10 if not specified
-- 8. Empty results return an empty JSON array: []

-- Made with Bob
