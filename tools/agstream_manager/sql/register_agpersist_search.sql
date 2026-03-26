-- Register AGPersist Search UDFs for persistent vector search operations
-- These UDFs allow building, searching, and managing persistent vector indexes that survive Flink restarts
--
-- Functions registered:
-- 1. build_search_index - Build/update a persistent vector index (with override parameter)
-- 2. search_persisted_index - Search a persisted index (UDTF)
-- 3. search_index_json - Search returning JSON (UDAF, recommended)
-- 4. list_search_indexes - List all indexes (UDTF)
-- 5. list_indexes - List all indexes as comma-separated string (UDAF)
-- 6. remove_search_index - Remove a persisted index (UDAF)
-- 7. explode_search_results - Parse JSON search results (UDTF)

-- Register the build_search_index UDAF
-- Builds a persistent vector index from accumulated rows
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS build_search_index
AS 'agpersist_search.build_search_index'
LANGUAGE PYTHON;

-- Register the search_persisted_index UDTF (direct table function - may have issues)
-- Searches a previously built persistent index
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS search_persisted_index
AS 'agpersist_search.search_persisted_index'
LANGUAGE PYTHON;

-- Register the search_index_json UDAF (JSON-returning version - RECOMMENDED)
-- Returns JSON that can be used with explode_search_results
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS search_index_json
AS 'agpersist_search.search_index_json'
LANGUAGE PYTHON;

-- Register the list_search_indexes UDTF
-- Lists all available persisted search indexes (requires LATERAL TABLE syntax)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS list_search_indexes
AS 'agpersist_search.list_search_indexes'
LANGUAGE PYTHON;

-- Register the list_indexes UDAF (simpler syntax)
-- Returns comma-separated list of indexes
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS list_indexes
AS 'agpersist_search.list_indexes'
LANGUAGE PYTHON;

-- Register the remove_search_index UDAF
-- Removes a persisted vector search index and all its files
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS remove_search_index
AS 'agpersist_search.remove_search_index'
LANGUAGE PYTHON;

-- Register explode_search_results from agsearch (for parsing JSON results)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS explode_search_results
AS 'agsearch.explode_search_results'
LANGUAGE PYTHON;

-- Verify registration
SHOW FUNCTIONS;

-- Made with Bob
