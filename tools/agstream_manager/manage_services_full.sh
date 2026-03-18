#!/bin/bash

# Agentics Services Manager (Full Stack with Flink)
# Manages Kafka, Karapace, Flink services and local PyFlink listener

set -e

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-karapace-flink.yml"
PROJECT_DIR="$SCRIPT_DIR"

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
    print_info "Starting all services (Full Stack with Flink)..."

    # Check Python dependencies
    print_info "Checking Python dependencies..."
    if command -v uv > /dev/null 2>&1; then
        print_info "Using uv for Python execution"
        PYTHON_CMD="uv run python"
        # Ensure required packages are installed in uv environment
        if ! uv run python -c "import flask_sock; import confluent_kafka; import kafka" 2>/dev/null; then
            print_warn "Installing missing dependencies"
            uv pip install flask-sock kafka-python-ng confluent-kafka || {
                print_error "Failed to install dependencies"
                exit 1
            }
        fi
    else
        print_info "Using system python3"
        PYTHON_CMD="python3"
        if ! python3 -c "import flask_sock" 2>/dev/null; then
            print_warn "Installing missing dependency: flask-sock"
            pip install flask-sock || {
                print_error "Failed to install flask-sock"
                exit 1
            }
        fi
    fi

    # Check if Colima is running, start if needed
    print_info "Checking Docker/Colima status..."

    # Set DOCKER_HOST for Colima early
    export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
    print_info "DOCKER_HOST set to: $DOCKER_HOST"

    local colima_was_stopped=false
    if ! colima status > /dev/null 2>&1; then
        print_warn "Colima is not running. Starting Colima..."
        colima start
        colima_was_stopped=true
        sleep 10
    fi

    # Verify Docker daemon is accessible with multiple checks
    local docker_ok=false
    local max_retries=2
    local retry=0

    while [ $retry -le $max_retries ]; do
        if docker ps > /dev/null 2>&1 && docker info > /dev/null 2>&1; then
            docker_ok=true
            break
        fi

        if [ $retry -lt $max_retries ]; then
            if [ "$colima_was_stopped" = true ] && [ $retry -eq 0 ]; then
                # Just started Colima, give it more time before restarting
                print_warn "Docker daemon initializing, waiting longer..."
                sleep 10
            else
                # Colima was already running but Docker not responding - restart needed
                print_warn "Docker daemon not responding (attempt $((retry+1))/$((max_retries+1))). Restarting Colima..."
                colima restart
                sleep 15
            fi
        fi
        retry=$((retry+1))
    done

    if [ "$docker_ok" = false ]; then
        print_error "Docker daemon still not accessible after $((max_retries+1)) attempts"
        print_error "Please run 'colima restart' manually and try again"
        exit 1
    fi

    print_info "Docker daemon is fully accessible ✓"

    # Load environment variables from .env file
    if [ -f "$PROJECT_DIR/.env" ]; then
        # Use set -a to automatically export variables, then source the file
        set -a
        source "$PROJECT_DIR/.env"
        set +a
    fi

    # Set AGStream backends root directory
    AGSTREAM_BACKENDS="${AGSTREAM_BACKENDS:-./agstream-backends}"

    # Set Kafka data directory as subdirectory
    export KAFKA_DATA_DIR="${AGSTREAM_BACKENDS}/kafka-data"

    # Create AGStream backends directory structure
    print_info "Setting up AGStream backends directory: $AGSTREAM_BACKENDS"
    mkdir -p "$AGSTREAM_BACKENDS"
    mkdir -p "$KAFKA_DATA_DIR"
    mkdir -p "${AGSTREAM_BACKENDS}/saved_functions"

    print_info "Kafka data directory: $KAFKA_DATA_DIR"
    print_info "Functions directory: ${AGSTREAM_BACKENDS}/saved_functions"

    # Set ownership to user 1000:1000 (Kafka container user)
    # This ensures Kafka can write to the directory
    if [ "$(uname)" = "Darwin" ]; then
        # On macOS, just ensure directory exists and is writable
        chmod -R 777 "$KAFKA_DATA_DIR"
    else
        # On Linux, set proper ownership
        sudo chown -R 1000:1000 "$KAFKA_DATA_DIR" 2>/dev/null || chmod -R 777 "$KAFKA_DATA_DIR"
    fi

    print_info "Kafka data will persist in: $KAFKA_DATA_DIR"

    # Kill any processes that might be using our ports
    kill_port 9092  # Kafka
    kill_port 8081  # Schema Registry
    kill_port 8082  # REST Proxy (Karapace)
    kill_port 8083  # Function Persistence Service
    kill_port 8080  # Kafka UI
    kill_port 8000  # Schema Registry UI
    kill_port 8085  # Flink Web UI
    kill_port 8501  # Streamlit
    kill_port 5003  # AGstream Manager

    cd "$PROJECT_DIR"

    print_info "Starting Docker Compose services (Kafka + Flink + UIs)..."
    docker compose -f "$COMPOSE_FILE" up -d

    print_info "Waiting for services to initialize (20 seconds)..."
    sleep 20

    print_info "Starting Function Persistence Service..."
    # Run persistence service locally
    nohup $PYTHON_CMD examples/streaming_agent/function_persistence_service.py > /tmp/persistence.log 2>&1 &
    echo $! > /tmp/persistence.pid
    sleep 2

    print_info "Starting AGstream Manager (Unified Schema + Transduction + Listener Management)..."
    # Run AGstream manager locally
    nohup $PYTHON_CMD tools/agstream_manager/backend/agstream_manager_service.py > /tmp/agstream_manager.log 2>&1 &
    echo $! > /tmp/agstream_manager.pid
    sleep 2

    print_info "Services started successfully!"
    print_info ""
    print_info "Access points:"
    print_info "  - Kafka: localhost:9092"
    print_info "  - Karapace Schema Registry: http://localhost:8081"
    print_info "  - Karapace REST Proxy: http://localhost:8082"
    print_info "  - Function Persistence API: http://localhost:8083"
    print_info "  - AGstream Manager: http://localhost:5003 (Unified Platform)"
    print_info "  - Kafka UI: http://localhost:8080"
    print_info "  - Schema Registry UI: http://localhost:8000"
    print_info "  - Flink Web UI: http://localhost:8085"
    print_info ""
    print_info "Flink SQL Client:"
    print_info "  - Run: cd tools/agstream_manager && ./scripts/flink_sql_client.sh"
    print_info ""
    print_info "Logs:"
    print_info "  - Persistence: tail -f /tmp/persistence.log"
    print_info "  - AGstream Manager: tail -f /tmp/agstream_manager.log"
    print_info ""
    print_info "Check status with: $0 status"
    print_info ""
    print_info "Note: Services dashboard not available in full stack mode"
    print_info "Access services directly via the URLs listed above"
}

# Function to stop services
stop_services() {
    print_info "Stopping all services..."

    # Stop Persistence Service
    if [ -f /tmp/persistence.pid ]; then
        print_info "Stopping Function Persistence Service..."
        kill $(cat /tmp/persistence.pid) 2>/dev/null || true
        rm /tmp/persistence.pid
    fi

    # Stop AGstream Manager
    if [ -f /tmp/agstream_manager.pid ]; then
        print_info "Stopping AGstream Manager..."
        kill $(cat /tmp/agstream_manager.pid) 2>/dev/null || true
        rm /tmp/agstream_manager.pid
    fi

    # Check if Docker is accessible before trying to stop containers
    if docker ps > /dev/null 2>&1; then
        # Stop Docker services
        cd "$PROJECT_DIR"
        docker compose -f "$COMPOSE_FILE" down
    else
        print_warn "Docker daemon not accessible, skipping container shutdown"
        print_warn "Containers will be stopped when services restart"
    fi

    print_info "Services stopped successfully!"
}

# Function to clean Kafka data and restart
clean_restart() {
    print_info "Performing clean restart (will delete all Kafka data)..."

    # Stop services first
    stop_services

    # Load environment variables to get KAFKA_DATA_DIR
    if [ -f "$PROJECT_DIR/.env" ]; then
        set -a
        source "$PROJECT_DIR/.env"
        set +a
    fi

    # Set AGStream backends root directory
    AGSTREAM_BACKENDS="${AGSTREAM_BACKENDS:-./agstream-backends}"
    export KAFKA_DATA_DIR="${AGSTREAM_BACKENDS}/kafka-data"

    # Clear Kafka data
    print_warn "Clearing Kafka data from: $KAFKA_DATA_DIR"
    rm -rf "$KAFKA_DATA_DIR"/*

    print_info "Kafka data cleared. Starting services..."
    sleep 2

    # Start services
    start_services
}

# Function to restart services
restart_services() {
    print_info "Restarting all services..."
    stop_services
    sleep 2
    start_services

    # Verify AGStream Manager started, restart if needed
    sleep 3
    if [ -f /tmp/agstream_manager.pid ] && kill -0 $(cat /tmp/agstream_manager.pid) 2>/dev/null; then
        print_info "AGStream Manager is running"
    else
        print_warn "AGStream Manager failed to start, restarting..."
        start_agstream_manager
    fi
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

# Function to start Flink SQL client
start_flink_sql() {
    print_info "Starting Flink SQL Client..."
    cd "$PROJECT_DIR/tools/agstream_manager"
    ./scripts/flink_sql_client.sh
}

# Function to start Streamlit only
start_streamlit() {
    print_info "Starting Streamlit service..."

    # Check if already running
    if [ -f /tmp/streamlit.pid ] && kill -0 $(cat /tmp/streamlit.pid) 2>/dev/null; then
        print_warn "Streamlit is already running (PID: $(cat /tmp/streamlit.pid))"
        return 0
    fi

    kill_port 8501

    cd "$PROJECT_DIR"
    nohup streamlit run examples/streaming_agent/streamlit_chat_client.py > /tmp/streamlit.log 2>&1 &
    echo $! > /tmp/streamlit.pid
    sleep 2

    print_info "Streamlit started (PID: $(cat /tmp/streamlit.pid))"
    print_info "Access at: http://localhost:8501"
    print_info "View logs: tail -f /tmp/streamlit.log"
}

# Function to stop Streamlit only
stop_streamlit() {
    if [ -f /tmp/streamlit.pid ]; then
        print_info "Stopping Streamlit service..."
        kill $(cat /tmp/streamlit.pid) 2>/dev/null || true
        rm /tmp/streamlit.pid
        print_info "Streamlit stopped"
    else
        print_warn "Streamlit is not running"
    fi
}

# Function to view Streamlit logs
view_streamlit_logs() {
    if [ -f /tmp/streamlit.log ]; then
        tail -f /tmp/streamlit.log
    else
        print_error "No Streamlit logs found"
    fi
}

# Function to start persistence service only
start_persistence() {
    print_info "Starting Function Persistence Service..."

    # Check if already running
    if [ -f /tmp/persistence.pid ] && kill -0 $(cat /tmp/persistence.pid) 2>/dev/null; then
        print_warn "Persistence service is already running (PID: $(cat /tmp/persistence.pid))"
        return 0
    fi

    kill_port 8083

    # Determine Python command
    if command -v uv > /dev/null 2>&1; then
        PYTHON_CMD="uv run python"
    else
        PYTHON_CMD="python3"
    fi

    cd "$PROJECT_DIR"
    nohup $PYTHON_CMD examples/streaming_agent/function_persistence_service.py > /tmp/persistence.log 2>&1 &
    echo $! > /tmp/persistence.pid
    sleep 2

    print_info "Persistence service started (PID: $(cat /tmp/persistence.pid))"
    print_info "Access at: http://localhost:8083"
    print_info "View logs: tail -f /tmp/persistence.log"
}

# Function to stop persistence service only
stop_persistence() {
    if [ -f /tmp/persistence.pid ]; then
        print_info "Stopping Function Persistence Service..."
        kill $(cat /tmp/persistence.pid) 2>/dev/null || true
        rm /tmp/persistence.pid
        print_info "Persistence service stopped"
    else
        print_warn "Persistence service is not running"
    fi
}

# Function to view persistence logs
view_persistence_logs() {
    if [ -f /tmp/persistence.log ]; then
        tail -f /tmp/persistence.log
    else
        print_error "No persistence logs found"
    fi
}

# Function to start AGstream manager only
start_agstream_manager() {
    print_info "Starting AGstream Manager..."

    # Determine Python command
    if command -v uv > /dev/null 2>&1; then
        print_info "Using uv for Python execution"
        PYTHON_CMD="uv run python"
        # Check dependencies
        if ! uv run python -c "import flask_sock; import confluent_kafka; import kafka" 2>/dev/null; then
            print_warn "Installing missing dependencies"
            uv pip install flask-sock kafka-python-ng confluent-kafka || {
                print_error "Failed to install dependencies"
                return 1
            }
        fi
    else
        print_info "Using system python3"
        PYTHON_CMD="python3"
        # Check Python dependencies
        print_info "Checking Python dependencies..."
        if ! python3 -c "import flask_sock" 2>/dev/null; then
            print_warn "Installing missing dependency: flask-sock"
            pip install flask-sock || {
                print_error "Failed to install flask-sock"
                return 1
            }
        fi
    fi

    # Check if already running
    if [ -f /tmp/agstream_manager.pid ] && kill -0 $(cat /tmp/agstream_manager.pid) 2>/dev/null; then
        print_warn "AGstream Manager is already running (PID: $(cat /tmp/agstream_manager.pid))"
        return 0
    fi

    kill_port 5003

    cd "$PROJECT_DIR"
    nohup $PYTHON_CMD backend/agstream_manager_service.py > /tmp/agstream_manager.log 2>&1 &
    echo $! > /tmp/agstream_manager.pid
    sleep 2

    print_info "AGstream Manager started (PID: $(cat /tmp/agstream_manager.pid))"
    print_info "Access at: http://localhost:5003"
    print_info "View logs: tail -f /tmp/agstream_manager.log"
}

# Function to stop AGstream manager only
stop_agstream_manager() {
    if [ -f /tmp/agstream_manager.pid ]; then
        print_info "Stopping AGstream Manager..."
        kill $(cat /tmp/agstream_manager.pid) 2>/dev/null || true
        rm /tmp/agstream_manager.pid
        print_info "AGstream Manager stopped"
    else
        print_warn "AGstream Manager is not running"
    fi
}

# Function to view AGstream manager logs
view_agstream_manager_logs() {
    if [ -f /tmp/agstream_manager.log ]; then
        tail -f /tmp/agstream_manager.log
    else
        print_error "No AGstream Manager logs found"
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
    clean-restart)
        clean_restart
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$2"
        ;;
    flink-sql)
        start_flink_sql
        ;;
    start-streamlit)
        start_streamlit
        ;;
    stop-streamlit)
        stop_streamlit
        ;;
    streamlit-logs)
        view_streamlit_logs
        ;;
    start-persistence)
        start_persistence
        ;;
    stop-persistence)
        stop_persistence
        ;;
    persistence-logs)
        view_persistence_logs
        ;;
    start-agstream-manager)
        start_agstream_manager
        ;;
    stop-agstream-manager)
        stop_agstream_manager
        ;;
    agstream-manager-logs)
        view_agstream_manager_logs
        ;;
    dashboard)
        print_info "Dashboard not available in full stack mode"
        print_info "Access services directly:"
        print_info "  - Kafka UI: http://localhost:8080"
        print_info "  - Schema Registry UI: http://localhost:8000"
        print_info "  - Flink Web UI: http://localhost:8085"
        print_info "  - AGstream Manager: http://localhost:5003"
        ;;
    *)
        echo "Agentics Services Manager (Full Stack with Flink)"
        echo ""
        echo "Usage: $0 {start|stop|restart|clean-restart|status|logs|flink-sql|start-persistence|stop-persistence|persistence-logs|start-agstream-manager|stop-agstream-manager|agstream-manager-logs|start-streamlit|stop-streamlit|streamlit-logs|dashboard}"
        echo ""
        echo "Commands:"
        echo "  start                          - Start all services (Kafka, Flink, UIs, Managers, Persistence) and open dashboard"
        echo "  stop                           - Stop all services"
        echo "  restart                        - Restart all services"
        echo "  clean-restart                  - Stop services, clear Kafka data, and restart (fixes topic deletion issues)"
        echo "  status                         - Show Docker service status"
        echo "  logs [service]                 - Show Docker logs (optionally for specific service)"
        echo "  flink-sql                      - Start Flink SQL Client (interactive)"
        echo "  start-persistence              - Start only the Function Persistence service"
        echo "  stop-persistence               - Stop only the Function Persistence service"
        echo "  persistence-logs               - View persistence service logs"
        echo "  start-agstream-manager         - Start only the AGstream Manager"
        echo "  stop-agstream-manager          - Stop only the AGstream Manager"
        echo "  agstream-manager-logs          - View AGstream Manager logs"
        echo "  start-streamlit                - Start only the Streamlit service"
        echo "  stop-streamlit                 - Stop only the Streamlit service"
        echo "  streamlit-logs                 - View Streamlit logs"
        echo "  dashboard                      - Open services dashboard in browser"
        echo ""
        echo "Examples:"
        echo "  $0 start                         # Start everything (with Flink)"
        echo "  $0 flink-sql                     # Open Flink SQL Client"
        echo "  $0 clean-restart                 # Clean restart (clears Kafka data)"
        echo "  $0 logs kafka                    # View Kafka logs"
        echo "  $0 logs flink-jobmanager         # View Flink JobManager logs"
        echo "  $0 start-persistence             # Start just persistence service"
        echo "  $0 persistence-logs              # View persistence logs"
        echo "  $0 start-agstream-manager        # Start just AGstream Manager"
        echo "  $0 agstream-manager-logs         # View AGstream Manager logs"
        echo "  $0 start-streamlit               # Start just Streamlit"
        echo "  $0 streamlit-logs                # View Streamlit logs"
        echo "  $0 dashboard                     # Open web dashboard"
        echo ""
        echo "Access Points:"
        echo "  - Kafka UI: http://localhost:8080"
        echo "  - Schema Registry UI: http://localhost:8000"
        echo "  - Flink Web UI: http://localhost:8085"
        echo "  - Function Persistence API: http://localhost:8083"
        echo "  - AGstream Manager: http://localhost:5003 (Unified Platform)"
        echo "  - Services Dashboard: examples/streaming_agent/services_dashboard.html"
        echo ""
        echo "Flink SQL:"
        echo "  - Interactive Client: $0 flink-sql"
        echo "  - Alternative: cd tools/agstream_manager && ./scripts/flink_sql_client.sh"
        exit 1
        ;;
esac

# Made with Bob
