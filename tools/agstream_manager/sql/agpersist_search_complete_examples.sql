-- ============================================================================
-- AGPersist Search - Complete Examples with New Features
-- ============================================================================
-- This file demonstrates all AGPersist Search features including:
-- 1. Building indexes (with override parameter)
-- 2. Searching persisted indexes
-- 3. Removing indexes
-- 4. Listing indexes
--
-- NEW FEATURES:
-- - Index existence detection with informative messages
-- - Override parameter to control index rebuilding (default: false)
-- - remove_search_index() function to delete indexes
-- ============================================================================

-- First, make sure the functions are registered
-- Run: register_agpersist_search.sql

-- ============================================================================
-- SETUP: Create Source Table (Product Reviews)
-- ============================================================================

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

-- ============================================================================
-- EXAMPLE 1: Build Index (Default Behavior - Use Existing)
-- ============================================================================
-- If index already exists, it will use the existing one (override=false by default)
-- This is efficient - no need to rebuild if index already exists

-- Clean table output using JSON_VALUE
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

-- Expected output if index exists:
-- status  | index_name     | message                                              | num_items | dimension
-- exists  | reviews_index  | Index 'reviews_index' already exists. Using existing | NULL      | NULL

-- Expected output if index is new:
-- status  | index_name     | message | num_items | dimension
-- success | reviews_index  | NULL    | 100       | 384

-- ============================================================================
-- EXAMPLE 2: Build Index with Override (Force Rebuild)
-- ============================================================================
-- Set override=true to rebuild the index even if it exists
-- Use this when your data has changed and you want to update the index

-- Clean table output
WITH build_result AS (
    SELECT build_search_index(customer_review, 'reviews_index', true) as result
    FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name,
    JSON_VALUE(result, '$.message') as message,
    JSON_VALUE(result, '$.num_items') as num_items,
    JSON_VALUE(result, '$.dimension') as dimension
FROM build_result;

-- Expected output:
-- status  | index_name     | message | num_items | dimension
-- success | reviews_index  | NULL    | 100       | 384
-- Note: Console will show "⚠️  Index 'reviews_index' already exists. Overriding with new data (override=True)."

-- ============================================================================
-- EXAMPLE 3: Build Multiple Indexes for Different Purposes
-- ============================================================================

-- Index for product reviews (clean output)
WITH build_result AS (
    SELECT build_search_index(customer_review, 'reviews_index') as result
    FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name,
    JSON_VALUE(result, '$.num_items') as num_items
FROM build_result;

-- Index for product names (clean output)
WITH build_result AS (
    SELECT build_search_index(Product_name, 'products_index') as result
    FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name,
    JSON_VALUE(result, '$.num_items') as num_items
FROM build_result;

-- ============================================================================
-- EXAMPLE 4: Search Persisted Index
-- ============================================================================
-- Search the index we built

SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('reviews_index', 'great service', 10))
AS T(text, index, score)
ORDER BY T.score DESC;

-- ============================================================================
-- EXAMPLE 5: List All Indexes
-- ============================================================================
-- Returns all indexes as a comma-separated string

SELECT list_indexes() as indexes
FROM (VALUES (1));

-- Expected output:
-- "products_index,reviews_index"

-- ============================================================================
-- EXAMPLE 6: List Indexes as Separate Rows (Split the String)
-- ============================================================================
-- If you want each index as a separate row, split the comma-separated string

SELECT index_name
FROM (
    SELECT list_indexes() as all_indexes FROM (VALUES (1))
),
LATERAL TABLE(STRING_SPLIT(all_indexes, ',')) AS T(index_name)
WHERE index_name <> ''
ORDER BY index_name;

-- Expected output (one row per index):
-- products_index
-- reviews_index

-- ============================================================================
-- EXAMPLE 7: Remove an Index
-- ============================================================================
-- Delete an index when you no longer need it (clean table output)

WITH remove_result AS (
    SELECT remove_search_index('reviews_index') as result
    FROM (VALUES (1))
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name,
    JSON_VALUE(result, '$.message') as message
FROM remove_result;

-- Expected output if index exists:
-- status  | index_name     | message
-- success | reviews_index  | Index 'reviews_index' deleted successfully

-- Expected output if index doesn't exist:
-- status    | index_name     | message
-- not_found | reviews_index  | Index 'reviews_index' does not exist

-- ============================================================================
-- EXAMPLE 8: Complete Workflow - Build, Search, Remove
-- ============================================================================

-- Step 1: Check what indexes exist
SELECT list_indexes() as existing_indexes FROM (VALUES (1));

-- Step 2: Build a new index (or use existing) - clean output
WITH build_result AS (
    SELECT build_search_index(customer_review, 'temp_reviews_index') as result
    FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name
FROM build_result;

-- Step 3: Search the index
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('temp_reviews_index', 'excellent product', 5))
AS T(text, index, score)
ORDER BY T.score DESC;

-- Step 4: Remove the index when done - clean output
WITH remove_result AS (
    SELECT remove_search_index('temp_reviews_index') as result
    FROM (VALUES (1))
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.message') as message
FROM remove_result;

-- Step 5: Verify it's gone
SELECT list_indexes() as remaining_indexes FROM (VALUES (1));

-- ============================================================================
-- EXAMPLE 9: Conditional Index Building
-- ============================================================================
-- Check if index exists before deciding whether to build

-- First check if index exists
SELECT list_indexes() as all_indexes FROM (VALUES (1));

-- Check for a specific index (using LIKE)
SELECT
    CASE
        WHEN list_indexes() LIKE '%my_index%' THEN 'Index exists'
        ELSE 'Index not found'
    END as index_status
FROM (VALUES (1));

-- If you want to rebuild only if it doesn't exist, use default (override=false)
-- If you want to always rebuild, use override=true

-- ============================================================================
-- EXAMPLE 10: Index Management Best Practices
-- ============================================================================

-- 1. List all indexes to see what you have
SELECT list_indexes() as all_indexes FROM (VALUES (1));

-- 2. Build index with default behavior (efficient - reuses existing) - clean output
WITH build_result AS (
    SELECT build_search_index(customer_review, 'reviews_v1') as result FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name
FROM build_result;

-- 3. When data changes significantly, rebuild with override=true - clean output
WITH build_result AS (
    SELECT build_search_index(customer_review, 'reviews_v1', true) as result FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name,
    JSON_VALUE(result, '$.num_items') as num_items
FROM build_result;

-- 4. Or create a new version instead - clean output
WITH build_result AS (
    SELECT build_search_index(customer_review, 'reviews_v2') as result FROM pr
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.index_name') as index_name
FROM build_result;

-- 5. Remove old versions when no longer needed - clean output
WITH remove_result AS (
    SELECT remove_search_index('reviews_v1') as result FROM (VALUES (1))
)
SELECT
    JSON_VALUE(result, '$.status') as status,
    JSON_VALUE(result, '$.message') as message
FROM remove_result;

-- ============================================================================
-- EXAMPLE 11: Error Handling
-- ============================================================================

-- Searching non-existent index returns error
SELECT T.text, T.index, T.score
FROM LATERAL TABLE(search_persisted_index('nonexistent_index', 'query', 10))
AS T(text, index, score);
-- Returns: ("error: index not found", -1, 0.0)

-- Removing non-existent index returns not_found status
SELECT remove_search_index('nonexistent_index') as status FROM (VALUES (1));
-- Returns: {"status": "not_found", "index_name": "nonexistent_index", "message": "..."}

-- ============================================================================
-- TIPS AND BEST PRACTICES
-- ============================================================================
--
-- 1. DEFAULT BEHAVIOR (override=false):
--    - Use when you want to build once and reuse
--    - Efficient for static or slowly-changing data
--    - Saves computation time
--
-- 2. OVERRIDE MODE (override=true):
--    - Use when data has changed significantly
--    - Use when you want to update the index with new data
--    - Rebuilds the entire index from scratch
--
-- 3. INDEX NAMING:
--    - Use descriptive names: 'product_reviews_2024', 'customer_feedback_v2'
--    - Consider versioning: 'reviews_v1', 'reviews_v2'
--    - Use prefixes for organization: 'prod_reviews', 'test_reviews'
--
-- 4. INDEX LIFECYCLE:
--    - Build: Create index from your data
--    - Use: Search the index multiple times
--    - Update: Rebuild with override=true when data changes
--    - Clean: Remove old indexes with remove_search_index()
--
-- 5. MONITORING:
--    - Regularly check list_indexes() to see what's stored
--    - Remove unused indexes to save disk space
--    - Monitor index build status messages
--
-- ============================================================================

-- Made with Bob
