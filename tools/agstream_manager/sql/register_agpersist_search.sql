-- Register AGPersist Search UDFs for persistent vector search operations
-- These UDFs allow building and searching persistent vector indexes that survive Flink restarts

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

-- Register explode_search_results from agsearch (for parsing JSON results)
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS explode_search_results
AS 'agsearch.explode_search_results'
LANGUAGE PYTHON;

-- Verify registration
SHOW FUNCTIONS;

-- Made with Bob
