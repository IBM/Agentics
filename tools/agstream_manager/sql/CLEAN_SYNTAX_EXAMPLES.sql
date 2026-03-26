-- ============================================================================
-- AGPersist Search UDFs - Complete Reference Guide
-- ============================================================================
-- This file contains all AGPersist Search UDFs with working examples
-- Both wrapped (clean columns) and unwrapped (raw JSON) versions provided
-- ============================================================================

-- ============================================================================
-- TABLE OF CONTENTS
-- ============================================================================
-- 1. build_search_index()    - Build and persist a vector search index
-- 2. search_persisted_index() - Search a persisted index (UDTF)
-- 3. remove_search_index()   - Remove a persisted index
-- 4. list_indexes()          - List all persisted indexes
-- 5. search_index_json()     - Search index returning JSON (UDAF alternative)
-- ============================================================================

-- ============================================================================
-- UDF DESCRIPTIONS
-- ============================================================================

-- build_search_index(text_column, index_name, [override])
--   UDAF that builds a vector search index from text data and persists it to disk
--   Parameters:
--     - text_column: Column containing text to index
--     - index_name: Name for the persisted index
--     - override: (optional, default=false) If true, rebuilds existing index
--   Returns: JSON with status, index_name, message, num_items, dimension
--   Note: If index exists and override=false, returns "exists" status

-- search_persisted_index(index_name, query, k)
--   UDTF that searches a persisted index and returns results as rows
--   Parameters:
--     - index_name: Name of the persisted index
--     - query: Search query string
--     - k: Number of results to return
--   Returns: Table with columns (text, index, score)
--   Note: This is the recommended way to search - returns clean table format

-- remove_search_index(dummy_row, index_name)
--   UDAF that removes a persisted index from disk
--   Parameters:
--     - dummy_row: Required row parameter (use VALUES (1))
--     - index_name: Name of the index to remove
--   Returns: JSON with status, index_name, message

-- list_indexes()
--   UDAF that lists all persisted indexes
--   Parameters: None (use with VALUES (1))
--   Returns: Comma-separated string of index names

-- search_index_json(index_name, query, k)
--   UDAF alternative to search_persisted_index, returns JSON array
--   Parameters: Same as search_persisted_index
--   Returns: JSON array of results (use with explode_search_results)
--   Note: Use search_persisted_index instead for simpler syntax

-- ============================================================================

-- ============================================================================
-- EXAMPLE 1: Build Index - WRAPPED (Clean Table Columns)
-- ============================================================================
WITH build_data AS (
    SELECT build_search_index(customer_review, 'reviews_index') as json_result
    FROM pr
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.index_name') as index_name,
    JSON_VALUE(json_result, '$.message') as message,
    JSON_VALUE(json_result, '$.num_items') as num_items,
    JSON_VALUE(json_result, '$.dimension') as dimension
FROM build_data;

-- EXAMPLE 1b: Build Index - UNWRAPPED (Raw JSON)
SELECT build_search_index(customer_review, 'reviews_index') as status
FROM pr;

-- ============================================================================
-- EXAMPLE 2: Build Index with Override - WRAPPED (Clean Table Columns)
-- ============================================================================
WITH build_data AS (
    SELECT build_search_index(customer_review, 'reviews_index', true) as json_result
    FROM pr
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.index_name') as index_name,
    JSON_VALUE(json_result, '$.message') as message,
    JSON_VALUE(json_result, '$.num_items') as num_items,
    JSON_VALUE(json_result, '$.dimension') as dimension
FROM build_data;

-- EXAMPLE 2b: Build Index with Override - UNWRAPPED (Raw JSON)
SELECT build_search_index(customer_review, 'reviews_index', true) as status
FROM pr;

-- ============================================================================
-- EXAMPLE 3: Remove Index - WRAPPED (Clean Table Columns)
-- ============================================================================
WITH remove_data AS (
    SELECT remove_search_index(dummy, 'reviews_index') as json_result
    FROM (VALUES (1)) AS T(dummy)
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.index_name') as index_name,
    JSON_VALUE(json_result, '$.message') as message
FROM remove_data;

-- EXAMPLE 3b: Remove Index - UNWRAPPED (Raw JSON)
SELECT remove_search_index(dummy, 'reviews_index') as status
FROM (VALUES (1)) AS T(dummy);

-- ============================================================================
-- EXAMPLE 4: List Indexes - UNWRAPPED (Comma-Separated String)
-- ============================================================================
SELECT list_indexes() as indexes FROM (VALUES (1));

-- ============================================================================
-- EXAMPLE 5: Search Persisted Index (Using search_index_json + explode)
-- ============================================================================
-- Uses the same pattern as agsearch for consistency
-- Results are already sorted by relevance (highest score first)
SELECT text, idx
FROM (
    SELECT search_index_json('reviews_index', 'great service', 10) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- ============================================================================
-- COMPLETE WORKFLOW EXAMPLE
-- ============================================================================
-- This example shows a complete workflow: build, search, list, remove

-- Step 1: Build an index from product reviews
WITH build_data AS (
    SELECT build_search_index(customer_review, 'product_reviews') as json_result
    FROM pr
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.message') as message,
    JSON_VALUE(json_result, '$.num_items') as num_items
FROM build_data;

-- Step 2: Search the index for relevant reviews
SELECT text, idx
FROM (
    SELECT search_index_json('product_reviews', 'excellent quality', 5) as results
    FROM (VALUES (1))
),
LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- Step 3: List all available indexes
SELECT list_indexes() as available_indexes FROM (VALUES (1));

-- Step 4: Remove the index when done
WITH remove_data AS (
    SELECT remove_search_index(dummy, 'product_reviews') as json_result
    FROM (VALUES (1)) AS T(dummy)
)
SELECT
    JSON_VALUE(json_result, '$.status') as status,
    JSON_VALUE(json_result, '$.message') as message
FROM remove_data;

-- ============================================================================
-- ADVANCED EXAMPLES
-- ============================================================================

-- Example: Check if index exists before building (using override=false)
-- First attempt - will build if doesn't exist
SELECT build_search_index(customer_review, 'reviews_v1') as status FROM pr;

-- Second attempt - will return "exists" status and use existing index
SELECT build_search_index(customer_review, 'reviews_v1') as status FROM pr;

-- Force rebuild with override=true
SELECT build_search_index(customer_review, 'reviews_v1', true) as status FROM pr;

-- Example: Search multiple indexes and combine results
SELECT 'reviews_v1' as source, T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_v1', 'great product', 3))
AS T(text, index, score)
UNION ALL
SELECT 'reviews_v2' as source, T.text, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_v2', 'great product', 3))
AS T(text, index, score);

-- Example: Build index with filtered data
WITH filtered_reviews AS (
    SELECT customer_review
    FROM pr
    WHERE rating >= 4  -- Only index positive reviews
)
SELECT build_search_index(customer_review, 'positive_reviews') as status
FROM filtered_reviews;

-- ============================================================================
-- TROUBLESHOOTING TIPS
-- ============================================================================

-- If you get "index already exists" and want to rebuild:
--   Use override=true parameter in build_search_index()

-- If you get "Process died with exit code 0":
--   1. Reload UDFs: cd tools/agstream_manager && ./scripts/install_udfs.sh
--   2. Restart Flink: ./manage_services_full.sh restart

-- If you get OutOfMemoryError:
--   The docker-compose file has been updated with increased memory
--   Restart services: ./manage_services_full.sh restart

-- To check if an index exists:
--   SELECT list_indexes() as indexes FROM (VALUES (1));

-- ============================================================================
-- KEY SYNTAX RULES
-- ============================================================================
-- 1. Use "json_result" not "result" (result is reserved in Flink SQL)
-- 2. Use "build_data" or "remove_data" not "build_result" or "remove_result"
-- 3. Use WITH clause (CTE) for subqueries, not inline subqueries
-- 4. Use JSON_VALUE() to extract fields from JSON strings
-- 5. For remove_search_index, must use: FROM (VALUES (1)) AS T(dummy)
-- 6. For list_indexes, must use: FROM (VALUES (1))
-- 7. search_persisted_index returns clean table format (recommended)
-- ============================================================================

-- ============================================================================
-- PERFORMANCE NOTES
-- ============================================================================
-- - Indexes are persisted to disk at: /opt/flink/indexes/ (in container)
-- - Indexes persist across Flink restarts
-- - Building an index loads the SentenceTransformer model (~300MB)
-- - Resources are automatically cleaned up after each operation
-- - Use override=false (default) to reuse existing indexes
-- - Index building is parallelized across Flink task slots
-- ============================================================================

-- Made with Bob
