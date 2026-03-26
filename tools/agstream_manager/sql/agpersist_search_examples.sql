-- AGPersist Search Examples
-- Demonstrates persistent vector search index usage

-- ============================================
-- SETUP: Register Functions
-- ============================================

-- Register the functions (run once per session)
CREATE TEMPORARY FUNCTION IF NOT EXISTS build_search_index
AS 'agpersist_search.build_search_index'
LANGUAGE PYTHON;

CREATE TEMPORARY FUNCTION IF NOT EXISTS search_persisted_index
AS 'agpersist_search.search_persisted_index'
LANGUAGE PYTHON;

-- ============================================
-- EXAMPLE 1: Basic Index Building
-- ============================================

-- Build a simple index from customer reviews
SELECT build_search_index(customer_review, 'reviews_index') as status
FROM pr;

-- Expected output:
-- {"status": "success", "index_name": "reviews_index", "num_items": 1000, "dimension": 384}

-- ============================================
-- EXAMPLE 2: Basic Search
-- ============================================

-- Search for reviews about service
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'great service', 10))
AS T(text, index, score);

-- ============================================
-- EXAMPLE 3: Filtered Index Building
-- ============================================

-- Build index only from high-rated reviews
SELECT build_search_index(customer_review, 'positive_reviews_index') as status
FROM pr
WHERE rating >= 4;

-- Build index from recent data
SELECT build_search_index(customer_review, 'recent_reviews_index') as status
FROM pr
WHERE review_date > CURRENT_DATE - INTERVAL '30' DAY;

-- ============================================
-- EXAMPLE 4: Search with Filtering
-- ============================================

-- Get only high-confidence results
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'quality product', 20))
AS T(text, index, score)
WHERE T.score > 0.7
ORDER BY T.score DESC;

-- Limit to top 5 results
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'fast delivery', 5))
AS T(text, index, score);

-- ============================================
-- EXAMPLE 5: Multiple Searches on Same Index
-- ============================================

-- Compare positive vs negative sentiment searches
SELECT 'positive' as category, T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'excellent service', 5))
AS T(text, index, score)

UNION ALL

SELECT 'negative' as category, T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'poor quality', 5))
AS T(text, index, score)

ORDER BY category, score DESC;

-- ============================================
-- EXAMPLE 6: Join Search Results with Original Data
-- ============================================

-- Enrich search results with original table data
SELECT
    s.text,
    s.score,
    p.rating,
    p.customer_name,
    p.product_id
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'shipping issues', 10))
AS s(text, index, score)
JOIN pr p ON s.index = p.review_id
WHERE s.score > 0.6
ORDER BY s.score DESC;

-- ============================================
-- EXAMPLE 7: Aggregation on Search Results
-- ============================================

-- Count results by score range
SELECT
    CASE
        WHEN score >= 0.8 THEN 'high'
        WHEN score >= 0.6 THEN 'medium'
        ELSE 'low'
    END as confidence,
    COUNT(*) as count
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'product quality', 50))
AS T(text, index, score)
GROUP BY
    CASE
        WHEN score >= 0.8 THEN 'high'
        WHEN score >= 0.6 THEN 'medium'
        ELSE 'low'
    END;

-- ============================================
-- EXAMPLE 8: Category-Specific Indexes
-- ============================================

-- Build separate indexes for different categories
SELECT build_search_index(customer_review, 'electronics_reviews_index') as status
FROM pr
WHERE category = 'electronics';

SELECT build_search_index(customer_review, 'clothing_reviews_index') as status
FROM pr
WHERE category = 'clothing';

-- Search specific category
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('electronics_reviews_index', 'battery life', 10))
AS T(text, index, score);

-- ============================================
-- EXAMPLE 9: Cross-Index Search
-- ============================================

-- Search multiple indexes and combine results
SELECT 'electronics' as category, T.text, T.score
FROM LATERAL TABLE(search_persisted_index('electronics_reviews_index', 'quality', 5))
AS T(text, index, score)

UNION ALL

SELECT 'clothing' as category, T.text, T.score
FROM LATERAL TABLE(search_persisted_index('clothing_reviews_index', 'quality', 5))
AS T(text, index, score)

ORDER BY score DESC;

-- ============================================
-- EXAMPLE 10: Time-Based Index Updates
-- ============================================

-- Build daily index
SELECT build_search_index(customer_review, 'reviews_2024_03_23_index') as status
FROM pr
WHERE DATE(review_date) = DATE '2024-03-23';

-- Search today's index
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_2024_03_23_index', 'delivery', 10))
AS T(text, index, score);

-- ============================================
-- EXAMPLE 11: Semantic Clustering via Search
-- ============================================

-- Find similar reviews to a specific topic
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'customer support response time', 20))
AS T(text, index, score)
WHERE T.score > 0.5;

-- ============================================
-- EXAMPLE 12: Multi-Query Analysis
-- ============================================

-- Analyze different aspects of reviews
WITH service_reviews AS (
    SELECT T.text, T.score
    FROM LATERAL TABLE(search_persisted_index('reviews_index', 'customer service', 10))
    AS T(text, index, score)
),
quality_reviews AS (
    SELECT T.text, T.score
    FROM LATERAL TABLE(search_persisted_index('reviews_index', 'product quality', 10))
    AS T(text, index, score)
),
shipping_reviews AS (
    SELECT T.text, T.score
    FROM LATERAL TABLE(search_persisted_index('reviews_index', 'shipping speed', 10))
    AS T(text, index, score)
)
SELECT
    'service' as aspect,
    AVG(score) as avg_score,
    COUNT(*) as count
FROM service_reviews
UNION ALL
SELECT 'quality', AVG(score), COUNT(*) FROM quality_reviews
UNION ALL
SELECT 'shipping', AVG(score), COUNT(*) FROM shipping_reviews;

-- ============================================
-- EXAMPLE 13: Building Index with Window
-- ============================================

-- Build index from tumbling window (streaming)
SELECT
    TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start,
    build_search_index(log_message, CONCAT('logs_', CAST(TUMBLE_START(event_time, INTERVAL '1' HOUR) AS STRING))) as status
FROM system_logs
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR);

-- ============================================
-- EXAMPLE 14: Performance Comparison
-- ============================================

-- Compare search performance (persistent vs non-persistent)
-- This demonstrates the benefit of persistent indexes

-- First search (loads index from disk)
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'query1', 10))
AS T(text, index, score);

-- Second search (index already in memory - faster)
SELECT T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'query2', 10))
AS T(text, index, score);

-- ============================================
-- EXAMPLE 15: Error Handling
-- ============================================

-- Search non-existent index (will return error)
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('nonexistent_index', 'query', 10))
AS T(text, index, score);

-- Expected: ("error: index not found", -1, 0.0)

-- ============================================
-- NOTES
-- ============================================

-- 1. Index Storage Location:
--    Default: /tmp/agstream_indexes/
--    Custom: Set AGSTREAM_INDEX_PATH environment variable

-- 2. Index Persistence:
--    Indexes survive Flink restarts and can be shared across jobs

-- 3. Index Updates:
--    To update an index, rebuild it with the same name

-- 4. Performance Tips:
--    - Build indexes for static/semi-static data
--    - Use filtered indexes for better relevance
--    - Adjust k parameter based on needs
--    - First search loads index (slower), subsequent searches are faster

-- 5. Best Practices:
--    - Use descriptive index names
--    - Include timestamps for time-based indexes
--    - Monitor index size and rebuild periodically
--    - Use category-specific indexes for better results

-- Made with Bob
