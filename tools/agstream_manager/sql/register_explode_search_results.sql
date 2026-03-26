-- Register explode_search_results UDTF
-- This function explodes agsearch JSON array results into multiple rows

CREATE TEMPORARY FUNCTION IF NOT EXISTS explode_search_results
AS 'explode_search_results.explode_search_results'
LANGUAGE PYTHON;

-- Verify registration
SHOW FUNCTIONS;

-- Usage Example:
-- SELECT text, idx
-- FROM (
--     SELECT agsearch(customer_review, 'great service', 10) as results FROM pr
-- ),
-- LATERAL TABLE(explode_search_results(results)) AS T(text, idx);

-- Made with Bob
