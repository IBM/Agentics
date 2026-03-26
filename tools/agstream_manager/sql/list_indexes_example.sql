-- Example: List all persisted vector search indexes
-- Two functions available with different syntax

-- First, make sure the functions are registered
-- Run: register_agpersist_search.sql

-- ============================================
-- SIMPLE SYNTAX: list_indexes() UDAF
-- ============================================
-- Returns comma-separated string of all indexes

-- Example 1: Simple list (EASIEST)
SELECT list_indexes() as indexes
FROM (VALUES (1));

-- Example 2: Check if any indexes exist
SELECT
    CASE
        WHEN list_indexes() = '' THEN 'No indexes'
        ELSE 'Indexes found: ' || list_indexes()
    END as status
FROM (VALUES (1));

-- Example 3: Count indexes (split by comma)
SELECT
    CASE
        WHEN list_indexes() = '' THEN 0
        ELSE CARDINALITY(SPLIT(list_indexes(), ','))
    END as index_count
FROM (VALUES (1));

-- ============================================
-- ADVANCED SYNTAX: list_search_indexes() UDTF
-- ============================================
-- Returns one row per index (better for filtering/joining)

-- Example 4: List all indexes as separate rows
SELECT T.indexes
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes);

-- Example 5: List indexes ordered alphabetically
SELECT T.indexes
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes)
ORDER BY T.indexes;

-- Example 6: Check if a specific index exists
SELECT
    CASE
        WHEN COUNT(*) > 0 THEN 'Index exists'
        ELSE 'Index not found'
    END as status
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes)
WHERE T.indexes = 'my_index';

-- Example 7: Count total number of indexes
SELECT COUNT(*) as total_indexes
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes);

-- Example 8: Filter indexes by pattern
SELECT T.indexes
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(list_search_indexes()) AS T(indexes)
WHERE T.indexes LIKE '%review%';

-- Made with Bob
