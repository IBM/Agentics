-- Register AGGenerate UDF
--
-- This UDF generates prototypical instances of a given type using LLM-powered generation.
-- It supports both dynamic mode (single field) and registry mode (schema-based).
--
-- Usage after registration:
--
-- Dynamic mode (single field):
--   SELECT T.product_name
--   FROM LATERAL TABLE(aggenerate('product_name', 'str', 'Creative tech product names'))
--   AS T(product_name)
--   LIMIT 10;
--
-- Registry mode (schema-based):
--   SELECT T.sentiment_label, T.sentiment_score
--   FROM LATERAL TABLE(aggenerate('Sentiment', 'registry'))
--   AS T(sentiment_label, sentiment_score)
--   LIMIT 5;

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS aggenerate
AS 'aggenerate.aggenerate' LANGUAGE PYTHON;

-- Verify registration
SHOW FUNCTIONS;

-- Made with Bob
