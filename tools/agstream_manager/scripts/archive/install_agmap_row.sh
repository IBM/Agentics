#!/bin/bash

# Install agmap_row and agmap_table_dynamic UDFs in Flink

echo "📦 Installing agmap UDFs in Flink..."

# Copy the UDF to Flink container
docker cp ../udfs/agmap_row.py flink-jobmanager:/opt/flink/udfs/

echo "✅ Copied agmap_row.py to Flink container"

# Register the UDFs in Flink SQL
docker exec -i flink-jobmanager /opt/flink/bin/sql-client.sh embedded <<EOF
-- Load the UDFs
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_row AS 'agmap_row.agmap_row' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table AS 'agmap_row.agmap_table' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS agmap_table_dynamic AS 'agmap_row.agmap_table_dynamic' LANGUAGE PYTHON;

-- Show registered functions
SHOW FUNCTIONS;
EOF

echo "✅ All agmap UDFs installed and registered!"
echo ""
echo "📝 Usage examples:"
echo ""
echo "1. agmap_row (ROW type with multiple fields):"
echo "   SELECT "
echo "       customer_review,"
echo "       agmap_row('Sentiment', customer_review).sentiment_label as label,"
echo "       agmap_row('Sentiment', customer_review).sentiment_score as score"
echo "   FROM pr LIMIT 5;"
echo ""
echo "2. agmap_table (Table function with multiple columns):"
echo "   SELECT customer_review, T.sentiment_label, T.sentiment_score"
echo "   FROM pr, LATERAL TABLE(agmap_table('Sentiment', customer_review)) AS T(sentiment_label, sentiment_score)"
echo "   LIMIT 5;"
echo ""
echo "3. agmap_table_dynamic (Dynamic single-field generation):"
echo "   SELECT customer_review, T.sentiment"
echo "   FROM pr, LATERAL TABLE(agmap_table_dynamic('sentiment', customer_review, 'str', 'The sentiment')) AS T(sentiment)"
echo "   LIMIT 5;"

# Made with Bob
