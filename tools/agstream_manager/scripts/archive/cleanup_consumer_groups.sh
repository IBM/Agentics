#!/bin/bash
# Cleanup orphaned consumer groups from Kafka
# This script identifies and removes inactive consumer groups

set -e

KAFKA_CONTAINER="kafka"
KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP_SERVER="localhost:9092"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Check if running in dry-run mode
DRY_RUN=${1:-"--dry-run"}

print_header "Consumer Group Cleanup Utility"
echo ""

if [ "$DRY_RUN" = "--dry-run" ]; then
    print_warn "Running in DRY-RUN mode (no changes will be made)"
    print_info "To actually delete groups, run: $0 --delete"
    echo ""
elif [ "$DRY_RUN" = "--delete" ]; then
    print_warn "Running in DELETE mode (consumer groups will be removed)"
    echo ""
else
    print_error "Invalid argument. Use --dry-run or --delete"
    exit 1
fi

# Get all consumer groups
print_info "Fetching consumer groups..."
GROUPS=$(docker exec $KAFKA_CONTAINER $KAFKA_BIN/kafka-consumer-groups.sh \
    --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null)

TOTAL_GROUPS=$(echo "$GROUPS" | wc -l | tr -d ' ')
print_info "Found $TOTAL_GROUPS consumer groups"
echo ""

# Categorize groups
AGSTREAMSQL_GROUPS=$(echo "$GROUPS" | grep "^agstreamsql-" || true)
AGSTREAM_GROUPS=$(echo "$GROUPS" | grep "^agstream-" | grep -v "^agstreamsql-" || true)
KARAPACE_GROUPS=$(echo "$GROUPS" | grep "^karapace-" || true)
OTHER_GROUPS=$(echo "$GROUPS" | grep -v "^agstreamsql-" | grep -v "^agstream-" | grep -v "^karapace-" || true)

AGSTREAMSQL_COUNT=$(echo "$AGSTREAMSQL_GROUPS" | grep -c "." || echo "0")
AGSTREAM_COUNT=$(echo "$AGSTREAM_GROUPS" | grep -c "." || echo "0")
KARAPACE_COUNT=$(echo "$KARAPACE_GROUPS" | grep -c "." || echo "0")
OTHER_COUNT=$(echo "$OTHER_GROUPS" | grep -c "." || echo "0")

print_header "Consumer Group Summary"
echo "  AGStreamSQL (UUID-based): $AGSTREAMSQL_COUNT"
echo "  AGStream (named):         $AGSTREAM_COUNT"
echo "  Karapace:                 $KARAPACE_COUNT"
echo "  Other:                    $OTHER_COUNT"
echo "  Total:                    $TOTAL_GROUPS"
echo ""

# Function to check if a consumer group is active
check_group_status() {
    local group=$1
    local status=$(docker exec $KAFKA_CONTAINER $KAFKA_BIN/kafka-consumer-groups.sh \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --describe --group "$group" 2>/dev/null | grep -c "STABLE\|EMPTY" || echo "0")
    echo $status
}

# Analyze AGStreamSQL groups (UUID-based, likely orphaned)
print_header "Analyzing AGStreamSQL Consumer Groups"
print_info "These are UUID-based groups that may be orphaned..."
echo ""

INACTIVE_COUNT=0
ACTIVE_COUNT=0

if [ -n "$AGSTREAMSQL_GROUPS" ]; then
    # Sample first 10 groups to check status
    SAMPLE_GROUPS=$(echo "$AGSTREAMSQL_GROUPS" | head -10)

    for group in $SAMPLE_GROUPS; do
        # Check if group has any active consumers
        DESC=$(docker exec $KAFKA_CONTAINER $KAFKA_BIN/kafka-consumer-groups.sh \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --describe --group "$group" 2>/dev/null || true)

        if echo "$DESC" | grep -q "Consumer group.*has no active members"; then
            ((INACTIVE_COUNT++))
        else
            ((ACTIVE_COUNT++))
        fi
    done

    print_info "Sample analysis (first 10 groups):"
    echo "  Active:   $ACTIVE_COUNT"
    echo "  Inactive: $INACTIVE_COUNT"
    echo ""

    # Estimate total inactive
    if [ $INACTIVE_COUNT -gt 0 ]; then
        ESTIMATED_INACTIVE=$((AGSTREAMSQL_COUNT * INACTIVE_COUNT / 10))
        print_warn "Estimated inactive AGStreamSQL groups: ~$ESTIMATED_INACTIVE"
    fi
fi

echo ""
print_header "Cleanup Options"
echo ""
echo "1. Delete ALL AGStreamSQL UUID-based groups ($AGSTREAMSQL_COUNT groups)"
echo "   These are temporary groups created by test scripts and consumers"
echo ""
echo "2. Keep named AGStream groups ($AGSTREAM_COUNT groups)"
echo "   These are persistent listener groups"
echo ""
echo "3. Keep Karapace groups ($KARAPACE_COUNT groups)"
echo "   These are system groups"
echo ""

if [ "$DRY_RUN" = "--delete" ]; then
    print_warn "Proceeding with deletion of AGStreamSQL UUID-based groups..."
    echo ""

    DELETED=0
    FAILED=0

    for group in $AGSTREAMSQL_GROUPS; do
        if docker exec $KAFKA_CONTAINER $KAFKA_BIN/kafka-consumer-groups.sh \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --delete --group "$group" 2>/dev/null; then
            ((DELETED++))
            if [ $((DELETED % 50)) -eq 0 ]; then
                print_info "Deleted $DELETED groups..."
            fi
        else
            ((FAILED++))
        fi
    done

    echo ""
    print_header "Cleanup Complete"
    print_info "Deleted: $DELETED groups"
    if [ $FAILED -gt 0 ]; then
        print_warn "Failed: $FAILED groups"
    fi

    # Show remaining groups
    REMAINING=$(docker exec $KAFKA_CONTAINER $KAFKA_BIN/kafka-consumer-groups.sh \
        --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | wc -l | tr -d ' ')
    print_info "Remaining consumer groups: $REMAINING"

else
    print_info "DRY-RUN: Would delete $AGSTREAMSQL_COUNT AGStreamSQL groups"
    print_info "Run with --delete to actually remove these groups"
fi

echo ""
print_header "Recommendations"
echo ""
echo "1. Use named consumer groups for listeners:"
echo "   consumer_group='agstream-{listener_name}'"
echo ""
echo "2. Clean up test consumer groups after use"
echo ""
echo "3. Monitor consumer group count regularly:"
echo "   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \\"
echo "     --bootstrap-server localhost:9092 --list | wc -l"
echo ""

# Made with Bob
