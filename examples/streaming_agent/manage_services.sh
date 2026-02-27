#!/bin/bash

# Agentics Services Manager
# Manages Kafka, Karapace services and local PyFlink listener

set -e

COMPOSE_FILE="docker-compose-karapace-flink.yml"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to kill processes on specific ports
kill_port() {
    local port=$1
    print_info "Checking port $port..."
    local max_attempts=3
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if lsof -ti:$port > /dev/null 2>&1; then
            print_warn "Killing process on port $port (attempt $attempt/$max_attempts)"
            lsof -ti:$port | xargs kill -9 2>/dev/null || true
            sleep 2
            attempt=$((attempt + 1))
        else
            print_info "Port $port is free"
            return 0
        fi
    done

    # Final check
    if lsof -ti:$port > /dev/null 2>&1; then
        print_error "Failed to free port $port after $max_attempts attempts"
        return 1
    fi
}

# Function to start services
start_services() {
    print_info "Starting all services..."

    # Kill any processes that might be using our ports
    kill_port 9092  # Kafka
    kill_port 8081  # Schema Registry
    kill_port 8082  # REST Proxy
    kill_port 8080  # Kafka UI
    kill_port 8000  # Schema Registry UI
    kill_port 8501  # Streamlit

    cd "$PROJECT_DIR"

    print_info "Starting Docker Compose services (Kafka + UIs)..."
    docker compose -f "$COMPOSE_FILE" up -d

    print_info "Waiting for Kafka to initialize (15 seconds)..."
    sleep 15

    print_info "Starting Streamlit Chat UI..."
    nohup streamlit run examples/streaming_agent/streamlit_chat_client.py > /tmp/streamlit.log 2>&1 &
    echo $! > /tmp/streamlit.pid
    sleep 3

    print_info "Starting Local PyFlink Listener..."
    # Run listener locally (not in Docker)
    nohup python3 examples/streaming_agent/start_listener.py > /tmp/listener.log 2>&1 &
    echo $! > /tmp/listener.pid
    sleep 2

    print_info "Services started successfully!"
    print_info ""
    print_info "Access points:"
    print_info "  - Kafka: localhost:9092"
    print_info "  - Karapace Schema Registry: http://localhost:8081"
    print_info "  - Karapace REST Proxy: http://localhost:8082"
    print_info "  - Kafka UI: http://localhost:8080"
    print_info "  - Schema Registry UI: http://localhost:8000"
    print_info "  - Streamlit Chat: http://localhost:8501"
    print_info "  - Services Dashboard: open services_dashboard.html"
    print_info ""
    print_info "Logs:"
    print_info "  - Streamlit: tail -f /tmp/streamlit.log"
    print_info "  - Listener: tail -f /tmp/listener.log"
    print_info ""
    print_info "Check status with: $0 status"
}

# Function to stop services
stop_services() {
    print_info "Stopping all services..."

    # Stop Streamlit
    if [ -f /tmp/streamlit.pid ]; then
        print_info "Stopping Streamlit..."
        kill $(cat /tmp/streamlit.pid) 2>/dev/null || true
        rm /tmp/streamlit.pid
    fi

    # Stop Local Listener
    if [ -f /tmp/listener.pid ]; then
        print_info "Stopping Local PyFlink Listener..."
        kill $(cat /tmp/listener.pid) 2>/dev/null || true
        rm /tmp/listener.pid
    fi

    # Stop Docker services
    cd "$PROJECT_DIR"
    docker compose -f "$COMPOSE_FILE" down

    print_info "Services stopped successfully!"
}

# Function to restart services
restart_services() {
    print_info "Restarting all services..."
    stop_services
    sleep 2
    start_services
}

# Function to show service status
show_status() {
    cd "$PROJECT_DIR"
    print_info "Service status:"
    docker compose -f "$COMPOSE_FILE" ps
}

# Function to show logs
show_logs() {
    cd "$PROJECT_DIR"
    if [ -z "$1" ]; then
        docker compose -f "$COMPOSE_FILE" logs -f
    else
        docker compose -f "$COMPOSE_FILE" logs -f "$1"
    fi
}

# Function to start listener only
start_listener() {
    print_info "Starting Local PyFlink Listener..."

    # Check if already running
    if [ -f /tmp/listener.pid ] && kill -0 $(cat /tmp/listener.pid) 2>/dev/null; then
        print_warn "Listener is already running (PID: $(cat /tmp/listener.pid))"
        return 0
    fi

    cd "$PROJECT_DIR"
    nohup python3 examples/streaming_agent/start_listener.py > /tmp/listener.log 2>&1 &
    echo $! > /tmp/listener.pid
    sleep 2

    print_info "Listener started (PID: $(cat /tmp/listener.pid))"
    print_info "View logs: tail -f /tmp/listener.log"
}

# Function to stop listener only
stop_listener() {
    if [ -f /tmp/listener.pid ]; then
        print_info "Stopping Local PyFlink Listener..."
        kill $(cat /tmp/listener.pid) 2>/dev/null || true
        rm /tmp/listener.pid
        print_info "Listener stopped"
    else
        print_warn "Listener is not running"
    fi
}

# Function to view listener logs
view_listener_logs() {
    if [ -f /tmp/listener.log ]; then
        tail -f /tmp/listener.log
    else
        print_error "No listener logs found"
    fi
}

# Main script logic
case "$1" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$2"
        ;;
    start-listener)
        start_listener
        ;;
    stop-listener)
        stop_listener
        ;;
    listener-logs)
        view_listener_logs
        ;;
    dashboard)
        print_info "Opening services dashboard..."
        open services_dashboard.html 2>/dev/null || xdg-open services_dashboard.html 2>/dev/null || print_info "Please open services_dashboard.html in your browser"
        ;;
    *)
        echo "Agentics Services Manager (Local PyFlink)"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|logs|start-listener|stop-listener|listener-logs|dashboard}"
        echo ""
        echo "Commands:"
        echo "  start            - Start all services (Kafka, UIs, Listener, Streamlit)"
        echo "  stop             - Stop all services"
        echo "  restart          - Restart all services"
        echo "  status           - Show Docker service status"
        echo "  logs [service]   - Show Docker logs (optionally for specific service)"
        echo "  start-listener   - Start only the PyFlink listener"
        echo "  stop-listener    - Stop only the PyFlink listener"
        echo "  listener-logs    - View listener logs"
        echo "  dashboard        - Open services dashboard in browser"
        echo ""
        echo "Examples:"
        echo "  $0 start                    # Start everything"
        echo "  $0 logs kafka               # View Kafka logs"
        echo "  $0 start-listener           # Start just the listener"
        echo "  $0 listener-logs            # View listener logs"
        echo "  $0 dashboard                # Open web dashboard"
        echo ""
        echo "Access Points:"
        echo "  - Kafka UI: http://localhost:8080"
        echo "  - Schema Registry UI: http://localhost:8000"
        echo "  - Streamlit Chat: http://localhost:8501"
        echo "  - Services Dashboard: open services_dashboard.html"
        exit 1
        ;;
esac

# Made with Bob
