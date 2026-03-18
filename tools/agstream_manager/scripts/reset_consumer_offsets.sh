#!/bin/bash
# reset_consumer_offsets.sh
# Reset Kafka consumer group offsets to earliest position
# This fixes the issue where listeners don't consume messages because
# the consumer group offset is already at the end of the topic.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
CONSUMER_GROUP=""
TOPIC=""
RESET_TO="earliest"
DRY_RUN=false

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Reset Kafka consumer group offsets to fix listener consumption issues.

OPTIONS:
    -g, --group GROUP       Consumer group ID (required)
    -t, --topic TOPIC       Topic name (optional, resets all topics if not specified)
    -b, --broker BROKER     Kafka broker address (default: localhost:9092)
    -r, --reset-to POSITION Reset position: earliest, latest, or specific offset (default: earliest)
    -d, --dry-run           Show what would be reset without actually resetting
    -h, --help              Show this help message

EXAMPLES:
    # Reset specific consumer group for a topic to earliest
    $0 -g flink-listener-1 -t movies

    # Reset all topics for a consumer group
    $0 -g my-consumer-group

    # Dry run to see what would be reset
    $0 -g flink-listener-1 -t movies --dry-run

    # Reset to latest instead of earliest
    $0 -g flink-listener-1 -t movies --reset-to latest

COMMON USE CASES:
    1. Listener not consuming messages (offset at end):
       $0 -g <consumer-group> -t <topic>

    2. Reset all listeners for a topic:
       $0 -g <consumer-group>

    3. Check current offsets before resetting:
       $0 -g <consumer-group> -t <topic> --dry-run

EOF
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -g|--group)
            CONSUMER_GROUP="$2"
            shift 2
            ;;
        -t|--topic)
            TOPIC="$2"
            shift 2
            ;;
        -b|--broker)
            KAFKA_BROKER="$2"
            shift 2
            ;;
        -r|--reset-to)
            RESET_TO="$2"
            shift 2
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Validate required arguments
if [ -z "$CONSUMER_GROUP" ]; then
    echo -e "${RED}Error: Consumer group (-g) is required${NC}"
    usage
fi

# Check if kafka-consumer-groups command is available
if ! command -v kafka-consumer-groups &> /dev/null; then
    echo -e "${RED}Error: kafka-consumer-groups command not found${NC}"
    echo "Please ensure Kafka is installed and kafka-consumer-groups is in your PATH"
    exit 1
fi

echo -e "${GREEN}=== Kafka Consumer Offset Reset Tool ===${NC}"
echo "Broker: $KAFKA_BROKER"
echo "Consumer Group: $CONSUMER_GROUP"
echo "Topic: ${TOPIC:-all topics}"
echo "Reset To: $RESET_TO"
echo "Dry Run: $DRY_RUN"
echo ""

# Build the base command
CMD="kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --group $CONSUMER_GROUP"

# First, describe the consumer group to see current state
echo -e "${YELLOW}Current consumer group state:${NC}"
$CMD --describe || {
    echo -e "${YELLOW}Warning: Could not describe consumer group (it may not exist yet)${NC}"
}
echo ""

# Build the reset command
RESET_CMD="$CMD --reset-offsets --to-$RESET_TO"

if [ -n "$TOPIC" ]; then
    RESET_CMD="$RESET_CMD --topic $TOPIC"
else
    RESET_CMD="$RESET_CMD --all-topics"
fi

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY RUN - Showing what would be reset:${NC}"
    $RESET_CMD --dry-run
    echo ""
    echo -e "${GREEN}To actually reset, run without --dry-run flag${NC}"
else
    echo -e "${YELLOW}Resetting offsets...${NC}"
    $RESET_CMD --execute
    echo ""
    echo -e "${GREEN}✓ Offsets reset successfully!${NC}"
    echo ""
    echo -e "${YELLOW}New consumer group state:${NC}"
    $CMD --describe
fi

echo ""
echo -e "${GREEN}=== Reset Complete ===${NC}"
echo ""
echo "Next steps:"
echo "1. Restart your listener/consumer"
echo "2. It should now consume messages from the $RESET_TO position"
echo ""
echo "To verify consumption, check:"
echo "  - Consumer lag: $CMD --describe"
echo "  - Topic messages: kafka-console-consumer --bootstrap-server $KAFKA_BROKER --topic $TOPIC --from-beginning --max-messages 5"

# Made with Bob
