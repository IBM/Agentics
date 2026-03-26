-- ============================================================================
-- AGGenerate UDF Examples
-- ============================================================================
--
-- The aggenerate UDF generates prototypical instances using LLM-powered generation.
--
-- Signature:
--   aggenerate(n_instances, field_name, field_type, description, instructions)
--
-- Parameters:
--   - n_instances: Number of instances to generate (REQUIRED, integer)
--   - field_name: Name of the field (optional, default: "generated_value")
--   - field_type: Type - 'str', 'int', 'float', 'bool' (optional, default: "str")
--   - description: Description to guide generation (optional)
--   - instructions: Additional instructions (optional)
--
-- The function generates exactly n_instances rows (no LIMIT needed).
-- ============================================================================

-- ============================================================================
-- Example 1: Basic String Generation
-- ============================================================================
-- Generate 5 creative tech product names

SELECT T.generated_text
FROM TABLE(aggenerate('5', 'product_name', 'str', 'Creative tech product names')) AS T(generated_text);

-- Expected output: 5 rows with creative product names like:
-- "QuantumSync Pro"
-- "NeuralFlow Hub"
-- "CloudVault Elite"
-- etc.


-- ============================================================================
-- Example 2: Integer Generation
-- ============================================================================
-- Generate 10 product ratings (1-5)

SELECT T.generated_text
FROM TABLE(aggenerate('10', 'rating', 'int', 'Product rating from 1 to 5')) AS T(generated_text);

-- Expected output: 10 rows with integers like:
-- 4
-- 5
-- 3
-- etc.


-- ============================================================================
-- Example 3: Float Generation
-- ============================================================================
-- Generate 8 product prices

SELECT T.generated_text
FROM TABLE(aggenerate('8', 'price', 'float', 'Product price in USD'));

-- Expected output: 8 rows with float values like:
-- 29.99
-- 149.50
-- 79.95
-- etc.


-- ============================================================================
-- Example 4: With Additional Instructions
-- ============================================================================
-- Generate prices with specific constraints

SELECT T.generated_text
FROM TABLE(aggenerate('5', 'price', 'float', 'Product price', 'Between 10 and 100 dollars'));

-- Expected output: 5 rows with prices between 10 and 100


-- ============================================================================
-- Example 5: Boolean Generation
-- ============================================================================
-- Generate availability flags

SELECT T.generated_text
FROM TABLE(aggenerate('6', 'in_stock', 'bool', 'Product availability status')) AS T(generated_text);

-- Expected output: 6 rows with true/false values


-- ============================================================================
-- Example 6: Minimal Parameters (using defaults)
-- ============================================================================
-- Generate with just n_instances (uses default field_name and type)

SELECT T.generated_text
FROM TABLE(aggenerate('3')) AS T(generated_text);

-- Expected output: 3 rows with generated string values


-- ============================================================================
-- Example 7: Generate Product Descriptions
-- ============================================================================
-- Generate detailed product descriptions

SELECT T.generated_text
FROM TABLE(aggenerate(
    5,
    'description',
    'str',
    'Product description for tech gadgets',
    'Make them engaging and highlight key features'
)) AS T(generated_text);


-- ============================================================================
-- Example 8: Generate Email Addresses
-- ============================================================================

SELECT T.generated_text
FROM TABLE(aggenerate(
    7,
    'email',
    'str',
    'Professional email addresses',
    'Use realistic names and domains'
)) AS T(generated_text);


-- ============================================================================
-- Example 9: Generate Company Names
-- ============================================================================

SELECT T.generated_text
FROM TABLE(aggenerate(
    10,
    'company_name',
    'str',
    'Tech startup company names',
    'Modern, innovative, memorable'
)) AS T(generated_text);


-- ============================================================================
-- Example 10: Generate Quantities
-- ============================================================================

SELECT T.generated_text
FROM TABLE(aggenerate(
    15,
    'quantity',
    'int',
    'Inventory quantity',
    'Realistic stock levels between 0 and 1000'
)) AS T(generated_text);


-- ============================================================================
-- Example 11: Using Generated Data in a Table
-- ============================================================================
-- Create a table with generated product names

CREATE TEMPORARY VIEW generated_products AS
SELECT
    ROW_NUMBER() OVER () as product_id,
    T.generated_text as product_name
FROM TABLE(aggenerate('20', 'product_name', 'str', 'Tech product names')) AS T(generated_text);

-- Query the generated products
SELECT * FROM generated_products LIMIT 10;


-- ============================================================================
-- Example 12: Generate Multiple Fields (using CROSS JOIN)
-- ============================================================================
-- Generate a complete product catalog with names and prices
-- Note: This creates a cartesian product, so use same n_instances for both

CREATE TEMPORARY VIEW product_catalog AS
SELECT
    ROW_NUMBER() OVER () as id,
    names.generated_text as product_name,
    prices.generated_text as price
FROM
    TABLE(aggenerate('10', 'product_name', 'str', 'Tech products')) AS names(generated_text),
    TABLE(aggenerate('10', 'price', 'float', 'Product prices', 'Between 20 and 500')) AS prices(generated_text)
WHERE names.generated_text IS NOT NULL AND prices.generated_text IS NOT NULL
LIMIT 10;

SELECT * FROM product_catalog;


-- ============================================================================
-- Example 13: Generate Test Data for Reviews
-- ============================================================================

SELECT T.generated_text as review
FROM TABLE(aggenerate(
    5,
    'review',
    'str',
    'Customer product reviews',
    'Mix of positive and negative, realistic tone'
)) AS T(generated_text);


-- ============================================================================
-- Example 14: Generate Discount Percentages
-- ============================================================================

SELECT T.generated_text as discount
FROM TABLE(aggenerate(
    8,
    'discount',
    'int',
    'Discount percentage',
    'Common discount values like 10, 15, 20, 25, 30, 50'
)) AS T(generated_text);


-- ============================================================================
-- Example 15: Generate Category Names
-- ============================================================================

SELECT T.generated_text as category
FROM TABLE(aggenerate(
    6,
    'category',
    'str',
    'Product categories for an electronics store'
)) AS T(generated_text);


-- ============================================================================
-- Notes:
-- ============================================================================
--
-- 1. The function generates exactly n_instances rows - no LIMIT clause needed
-- 2. All parameters after n_instances are optional
-- 3. Default field_type is 'str' if not specified
-- 4. The description parameter guides what kind of values to generate
-- 5. The instructions parameter provides additional constraints
-- 6. Generated values are returned as strings (cast if needed)
-- 7. Maximum n_instances is capped at 100 for safety
--
-- ============================================================================

-- Made with Bob


-- ============================================================================
-- Example 16: JSON Mode - Multi-Field Generation WITHOUT Registry
-- ============================================================================
-- NEW! Generate multi-field JSON without needing Schema Registry connection
-- Use format: "field1:type1,field2:type2" in description parameter

-- Generate product reviews with multiple fields
SELECT T.generated_text
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(aggenerate('5', 'Product_name:str,customer_review:str', '', 'json', 'Generate realistic product reviews')) AS T(generated_text)
LIMIT 5;

-- Expected output: 5 rows with JSON like:
-- {"Product_name":"SmartWatch Pro","customer_review":"Excellent battery life and sleek design"}
-- {"Product_name":"Wireless Earbuds","customer_review":"Great sound quality, comfortable fit"}
-- etc.


-- ============================================================================
-- Example 17: JSON Mode with Field Extraction
-- ============================================================================
-- Generate JSON and extract individual fields using JSON_VALUE

SELECT
    JSON_VALUE(T.generated_text, '$.Product_name') AS Product_name,
    JSON_VALUE(T.generated_text, '$.customer_review') AS customer_review
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(aggenerate('5', 'Product_name:str,customer_review:str', '', 'json')) AS T(generated_text)
LIMIT 5;

-- Expected output: Table with separate columns:
-- Product_name          | customer_review
-- ----------------------|------------------------------------------
-- SmartWatch Pro        | Excellent battery life and sleek design
-- Wireless Earbuds      | Great sound quality, comfortable fit
-- etc.


-- ============================================================================
-- Example 18: JSON Mode with Mixed Types
-- ============================================================================
-- Generate data with different field types

SELECT
    JSON_VALUE(T.generated_text, '$.product_name') AS product_name,
    CAST(JSON_VALUE(T.generated_text, '$.price') AS DOUBLE) AS price,
    CAST(JSON_VALUE(T.generated_text, '$.rating') AS INT) AS rating,
    CAST(JSON_VALUE(T.generated_text, '$.in_stock') AS BOOLEAN) AS in_stock
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(aggenerate('5', 'product_name:str,price:float,rating:int,in_stock:bool', '', 'json', 'Generate product catalog data')) AS T(generated_text)
LIMIT 5;

-- Expected output: Table with typed columns:
-- product_name     | price  | rating | in_stock
-- -----------------|--------|--------|----------
-- Laptop Pro       | 1299.99| 5      | true
-- Gaming Mouse     | 79.99  | 4      | true
-- etc.


-- ============================================================================
-- Example 19: JSON Mode - Create View for Easy Querying
-- ============================================================================
-- Create a view that automatically extracts fields

CREATE TEMPORARY VIEW generated_products AS
SELECT
    JSON_VALUE(T.generated_text, '$.product_name') AS product_name,
    CAST(JSON_VALUE(T.generated_text, '$.price') AS DOUBLE) AS price,
    JSON_VALUE(T.generated_text, '$.description') AS description
FROM (VALUES (1)) AS dummy(x),
LATERAL TABLE(aggenerate('10', 'product_name:str,price:float,description:str', '', 'json', 'Generate tech products')) AS T(generated_text);

-- Then query the view
SELECT * FROM generated_products LIMIT 5;


-- ============================================================================
-- JSON Mode vs Registry Mode Comparison
-- ============================================================================
--
-- JSON Mode (NEW):
--   ✓ No Schema Registry connection needed
--   ✓ Define fields inline: "field1:type1,field2:type2"
--   ✓ Use JSON_VALUE() to extract fields
--   ✓ Works anywhere, anytime
--   ✗ Returns single JSON column (need to extract)
--
-- Registry Mode:
--   ✓ Uses existing schema definitions
--   ✓ Consistent with other data
--   ✗ Requires Schema Registry connection
--   ✗ Returns single JSON column (need to extract)
--
-- Recommendation: Use JSON mode for quick prototyping and when Schema Registry
-- is not available. Use Registry mode for production with existing schemas.
-- ============================================================================
