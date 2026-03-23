#!/bin/bash

# Quick command to set Flink SQL parallelism
# Usage: ./set_flink_parallelism.sh <parallelism>
# Example: ./set_flink_parallelism.sh 20

set -e

PARALLELISM="${1:-4}"
FLINK_SQL_CLIENT="${FLINK_SQL_CLIENT:-./scripts/flink_sql.sh}"

echo "============================================================"
echo "⚙️  Set Flink SQL Parallelism"
echo "============================================================"
echo "Parallelism: $PARALLELISM"
echo "============================================================"
echo ""

# Create SQL command
SQL_COMMAND="SET 'parallelism.default' = '$PARALLELISM';"

echo "Executing SQL command:"
echo "$SQL_COMMAND"
echo ""

# Execute the command
echo "$SQL_COMMAND" | $FLINK_SQL_CLIENT

echo ""
echo "============================================================"
echo "✅ Parallelism set to $PARALLELISM"
echo "============================================================"
echo ""
echo "💡 This setting applies to the current Flink SQL session."
echo "   To make it permanent, add to your SQL scripts:"
echo "   SET 'parallelism.default' = '$PARALLELISM';"
echo ""
echo "📊 Common parallelism values:"
echo "   - 1:  Single-threaded (debugging)"
echo "   - 4:  Small workloads"
echo "   - 10: Medium workloads"
echo "   - 20: Large workloads"
echo "   - 50: Very large workloads"
echo ""
echo "⚠️  Note: Parallelism should match or be less than:"
echo "   - Number of Kafka topic partitions"
echo "   - Number of TaskManager task slots"
echo "============================================================"

# Made with Bob
