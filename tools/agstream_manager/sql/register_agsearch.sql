-- Register AGsearch UDF for vector search operations
-- This UDF performs semantic search on accumulated table rows using AG's vector store

-- Register the agsearch UDAF
CREATE TEMPORARY FUNCTION IF NOT EXISTS agsearch AS 'agsearch.agsearch' LANGUAGE PYTHON;

-- Verify registration
SHOW FUNCTIONS;

-- Made with Bob
