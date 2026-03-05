#!/bin/bash

# AGStream Manager Services Script
# Manages Kafka, Karapace and AGStream Manager (unified service)

set -e

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGSTREAM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$AGSTREAM_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose-karapace.yml"

# Colors for output
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

# Kill processes on specific ports
kill_port() {
    local port=$1
    if lsof -ti:$port > /dev/null 2>&1; then
        print_warn "Killing process on port $port"
        lsof -ti:$port | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
}

# Start all services
start_services() {
    print_header "Starting AGStream Manager"

    cd "$PROJECT_ROOT"

    # Start Docker services (Kafka + Karapace)
    print_info "Starting Kafka and Karapace Schema Registry..."
    docker compose -f "$COMPOSE_FILE" up -d

    print_info "Waiting for Kafka to initialize (15 seconds)..."
    sleep 15

    cd "$AGSTREAM_DIR/backend"

    # Kill existing AGStream Manager process
    kill_port 5003

    # Start AGStream Manager (unified service)
    print_info "Starting AGStream Manager (Port 5003)..."
    nohup python3 agstream_manager_service.py > /tmp/agstream_manager.log 2>&1 &
    echo $! > /tmp/agstream_manager.pid

    sleep 3

    print_header "AGStream Manager Started!"
    echo ""
    print_info "Access Points:"
    print_info "  📊 Main Dashboard: http://localhost:5003"
    print_info "  📨 Manage Topics: http://localhost:5003 (Topics tab)"
    print_info "  ⚡ Define Transductions: http://localhost:5003 (Transductions tab)"
    print_info "  🎧 Manage Listeners: http://localhost:5003 (Listeners tab)"
    echo ""
    print_info "Infrastructure:"
    print_info "  - Kafka: localhost:9092"
    print_info "  - Schema Registry: http://localhost:8081"
    print_info "  - Kafka UI: http://localhost:8080"
    echo ""
    print_info "Logs: tail -f /tmp/agstream_manager.log"
    echo ""
    print_info "Opening dashboard..."
    open "$AGSTREAM_DIR/frontend/agstream_manager.html" 2>/dev/null || xdg-open "$AGSTREAM_DIR/frontend/agstream_manager.html" 2>/dev/null || print_warn "Please open frontend/agstream_manager.html in your browser"
}

# Stop all services
stop_services() {
    print_header "Stopping AGStream Manager"

    # Stop AGStream Manager
    if [ -f /tmp/agstream_manager.pid ]; then
        print_info "Stopping AGStream Manager..."
        kill $(cat /tmp/agstream_manager.pid) 2>/dev/null || true
        rm /tmp/agstream_manager.pid
    fi

    # Stop Docker services
    cd "$PROJECT_ROOT"
    print_info "Stopping Kafka and Karapace..."
    docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true

    print_info "Services stopped"
}

# Clean restart (clears Kafka data)
clean_restart() {
    print_header "Clean Restart (Deleting Kafka Data)"

    stop_services

    # Load environment variables
    if [ -f "$PROJECT_ROOT/.env" ]; then
        set -a
        source "$PROJECT_ROOT/.env"
        set +a
    fi

    # Clear Kafka data
    AGSTREAM_BACKENDS="${AGSTREAM_BACKENDS:-$PROJECT_ROOT/agstream-backends}"
    KAFKA_DATA_DIR="${AGSTREAM_BACKENDS}/kafka-data"

    print_warn "Clearing Kafka data from: $KAFKA_DATA_DIR"
    rm -rf "$KAFKA_DATA_DIR"/*

    print_info "Kafka data cleared. Restarting..."
    sleep 2

    start_services
}

# Show status
show_status() {
    print_header "AGStream Manager Status"

    echo ""
    print_info "Docker Services:"
    if docker ps > /dev/null 2>&1; then
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "kafka|karapace" || print_warn "No Docker services running"
    else
        print_error "Docker daemon not accessible"
    fi

    echo ""
    print_info "AGStream Manager:"
    if [ -f /tmp/agstream_manager.pid ] && kill -0 $(cat /tmp/agstream_manager.pid) 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} Running (Port 5003, PID: $(cat /tmp/agstream_manager.pid))"
    else
        echo -e "  ${RED}✗${NC} Not Running"
    fi
    echo ""
}

# Main
case "${1:-}" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        sleep 2
        start_services
        ;;
    clean-restart)
        clean_restart
        ;;
    status)
        show_status
        ;;
    logs)
        tail -f /tmp/agstream_manager.log
        ;;
    *)
        echo "AGStream Manager - Unified Streaming Agent Management"
        echo ""
        echo "Usage: $0 {start|stop|restart|clean-restart|status|logs}"
        echo ""
        echo "Commands:"
        echo "  start         - Start AGStream Manager and Kafka"
        echo "  stop          - Stop all services"
        echo "  restart       - Restart all services"
        echo "  clean-restart - Stop, clear Kafka data, and restart"
        echo "  status        - Show service status"
        echo "  logs          - Show AGStream Manager logs"
        echo ""
        echo "Examples:"
        echo "  $0 start"
        echo "  $0 status"
        echo "  $0 logs"
        exit 1
        ;;
esac
