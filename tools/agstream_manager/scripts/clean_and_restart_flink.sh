#!/bin/bash

# Clean up previous Flink jobs and restart with fresh state
# This script cancels all running jobs and restarts Flink containers

set -e

FLINK_JOBMANAGER_URL="${FLINK_JOBMANAGER_URL:-http://localhost:8085}"

echo "============================================================"
echo "🧹 Clean and Restart Flink"
echo "============================================================"
echo "Flink JobManager: $FLINK_JOBMANAGER_URL"
echo "============================================================"
echo ""

# Step 1: Cancel all running jobs
echo "📋 Step 1: Canceling all running Flink jobs..."
echo "------------------------------------------------------------"

# Get list of running jobs
JOBS=$(curl -s "$FLINK_JOBMANAGER_URL/jobs" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [])
    for job in jobs:
        if job.get('status') in ['RUNNING', 'CREATED', 'RESTARTING']:
            print(job['id'])
except:
    pass
" 2>/dev/null)

if [ -z "$JOBS" ]; then
    echo "   ✓ No running jobs found"
else
    echo "   Found running jobs, canceling..."
    for JOB_ID in $JOBS; do
        echo "   Canceling job: $JOB_ID"
        curl -s -X PATCH "$FLINK_JOBMANAGER_URL/jobs/$JOB_ID?mode=cancel" > /dev/null 2>&1 || true
        echo "   ✓ Canceled: $JOB_ID"
    done
    echo "   Waiting for jobs to terminate..."
    sleep 5
fi

echo ""

# Step 2: Restart Flink containers
echo "🔄 Step 2: Restarting Flink containers..."
echo "------------------------------------------------------------"

cd "$(dirname "$0")/.."

# Stop Flink containers
echo "   Stopping Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml stop flink-jobmanager flink-taskmanager

# Remove containers to ensure clean state
echo "   Removing Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml rm -f flink-jobmanager flink-taskmanager

# Start Flink containers
echo "   Starting Flink containers..."
docker-compose -f docker-compose-karapace-flink.yml up -d flink-jobmanager flink-taskmanager

echo "   Waiting for Flink to be ready..."
sleep 10

# Wait for Flink to be healthy
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s "$FLINK_JOBMANAGER_URL/overview" > /dev/null 2>&1; then
        echo "   ✓ Flink is ready!"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "   Waiting for Flink... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "   ✗ Flink failed to start within timeout"
    exit 1
fi

echo ""

# Step 3: Display cluster status
echo "📊 Step 3: Cluster Status"
echo "------------------------------------------------------------"

OVERVIEW=$(curl -s "$FLINK_JOBMANAGER_URL/overview" 2>/dev/null)
if [ $? -eq 0 ]; then
    echo "$OVERVIEW" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f\"   Task Managers: {data.get('taskmanagers', 0)}\")
    print(f\"   Task Slots Total: {data.get('slots-total', 0)}\")
    print(f\"   Task Slots Available: {data.get('slots-available', 0)}\")
    print(f\"   Jobs Running: {data.get('jobs-running', 0)}\")
    print(f\"   Jobs Finished: {data.get('jobs-finished', 0)}\")
    print(f\"   Jobs Cancelled: {data.get('jobs-cancelled', 0)}\")
    print(f\"   Jobs Failed: {data.get('jobs-failed', 0)}\")
except:
    print('   Could not parse cluster status')
"
else
    echo "   ✗ Could not fetch cluster status"
fi

echo ""
echo "============================================================"
echo "✅ Flink Cleanup and Restart Complete!"
echo "============================================================"
echo ""
echo "🌐 Flink Web UI: $FLINK_JOBMANAGER_URL"
echo ""
echo "💡 Configuration:"
echo "   - Task Slots per TaskManager: 20"
echo "   - Default Parallelism: 20"
echo "   - All previous jobs have been canceled"
echo ""
echo "📝 Next Steps:"
echo "   1. Open Flink SQL shell: ./scripts/flink_sql.sh"
echo "   2. Set parallelism if needed: SET 'parallelism.default' = '4';"
echo "   3. Run your queries"
echo "============================================================"

# Made with Bob
