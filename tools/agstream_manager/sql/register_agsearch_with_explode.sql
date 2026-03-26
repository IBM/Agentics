-- Register AGsearch and explode_search_results functions
-- This script registers both the agsearch UDAF and the explode_search_results UDTF

-- Register agsearch (aggregate function for vector search)
CREATE TEMPORARY FUNCTION IF NOT EXISTS agsearch
AS 'agsearch.agsearch'
LANGUAGE PYTHON;

-- Register explode_search_results (table function to explode JSON results)
CREATE TEMPORARY FUNCTION IF NOT EXISTS explode_search_results
AS 'explode_search_results.explode_search_results'
LANGUAGE PYTHON;

-- Verify both are registered
SHOW FUNCTIONS;

-- Usage Example:
-- SELECT text, idx
-- FROM (
--     SELECT agsearch(customer_review, 'great service', 10) as results FROM pr
-- ),
-- LATERAL TABLE(explode_search_results(results)) AS T(text, idx)
-- LIMIT 5;

-- Made with Bob
