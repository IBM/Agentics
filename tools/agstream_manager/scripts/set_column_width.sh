#!/bin/bash

# Quick command to set Flink SQL column display width
# Usage: ./set_column_width.sh <width>
# Example: ./set_column_width.sh 100

set -e

WIDTH="${1:-100}"
FLINK_SQL_CLIENT="${FLINK_SQL_CLIENT:-./scripts/flink_sql.sh}"

echo "============================================================"
echo "📏 Set Flink SQL Column Width"
echo "============================================================"
echo "Column Width: $WIDTH"
echo "============================================================"
echo ""

# Create SQL command
SQL_COMMAND="SET 'sql-client.display.max-column-width' = '$WIDTH';"

echo "Executing SQL command:"
echo "$SQL_COMMAND"
echo ""

# Execute the command
echo "$SQL_COMMAND" | $FLINK_SQL_CLIENT

echo ""
echo "============================================================"
echo "✅ Column width set to $WIDTH"
echo "============================================================"
echo ""
echo "💡 This setting applies to the current Flink SQL session."
echo "   To make it permanent, add to your SQL scripts:"
echo "   SET 'sql-client.display.max-column-width' = '$WIDTH';"
echo ""
echo "📊 Common column width values:"
echo "   - 50:  Narrow columns (default)"
echo "   - 100: Medium columns (recommended)"
echo "   - 200: Wide columns (for long text)"
echo "   - 500: Very wide columns (for JSON/large text)"
echo ""
echo "💡 Tips for better display:"
echo "   - Widen your terminal window"
echo "   - Select fewer columns in your queries"
echo "   - Use LIMIT to reduce number of rows"
echo "   - Consider exporting to file for large results"
echo "============================================================"

# Made with Bob
