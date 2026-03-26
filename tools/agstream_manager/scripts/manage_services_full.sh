#!/bin/bash

# Agentics Services Manager (Full Stack with Flink)
# Manages Kafka, Karapace, Flink services and local PyFlink listener

set -e

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose-karapace-flink.yml"

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

    # Auto-install Agentics and UDFs if enabled
    AUTO_INSTALL_ON_STARTUP="${AUTO_INSTALL_ON_STARTUP:-true}"

    if [ "$AUTO_INSTALL_ON_STARTUP" = "true" ]; then
        print_info "Auto-installing components to Flink containers..."

        # Wait for Flink containers to be fully ready
        print_info "Waiting for Flink containers to be ready..."
        sleep 5

        # Verify .env file is mounted (it's now mounted via docker-compose volumes)
        print_info "Verifying .env file is available in Flink containers..."
        ENV_FILE_FOUND=false

        # Check if .env exists in the expected location
        if [ -f "$PROJECT_DIR/../../.env" ]; then
            print_info "✓ .env file found at $PROJECT_DIR/../../.env"
            print_info "  (Automatically mounted to Flink containers via docker-compose)"
            ENV_FILE_FOUND=true
        elif [ -f "$PROJECT_DIR/.env" ]; then
            print_warn "⚠ .env found at $PROJECT_DIR/.env but should be at $PROJECT_DIR/../../.env"
            print_warn "  Please move it to the agentics base directory for proper mounting"
        else
            print_error "✗ .env file not found!"
            print_error "  Expected location: $PROJECT_DIR/../../.env (agentics base directory)"
            print_error "  UDFs will not work without API keys!"
            print_error "  Create .env file with OPENAI_API_KEY or ANTHROPIC_API_KEY"
        fi

        if [ "$ENV_FILE_FOUND" = true ]; then
            # Verify it's actually mounted in the container
            if docker exec flink-taskmanager test -f /opt/flink/.env 2>/dev/null; then
                print_info "✓ .env successfully mounted in Flink containers"
                print_info "  API keys are available for UDFs"
            else
                print_error "✗ .env not mounted in containers!"
                print_error "  Check docker-compose-karapace-flink.yml volume configuration"
            fi
        fi

        # Check if Agentics is already installed (skip heavy reinstall)
        print_info "Checking if Agentics is already installed..."
        AGENTICS_INSTALLED=$(docker exec flink-taskmanager bash -c "python3 -c 'import agentics; print(\"installed\")' 2>/dev/null" || echo "not_installed")

        if [ "$AGENTICS_INSTALLED" = "installed" ]; then
            print_info "✓ Agentics already installed, skipping heavy reinstall"
            print_info "  (Use 'cd scripts && ./install_agentics_in_flink.sh --force' to force reinstall)"
        else
            # Install Agentics
            if [ -f "$SCRIPT_DIR/install_agentics_in_flink.sh" ]; then
                print_info "Installing Agentics package..."
                bash "$SCRIPT_DIR/install_agentics_in_flink.sh" || {
                    print_warn "Failed to install Agentics automatically"
                    print_warn "You can install manually with: cd scripts && ./install_agentics_in_flink.sh"
                }
            else
                print_warn "Agentics install script not found, skipping"
            fi
        fi

        # Always install UDFs (they're lightweight)
        if [ -f "$SCRIPT_DIR/install_udfs.sh" ]; then
            print_info "Installing UDFs..."
            bash "$SCRIPT_DIR/install_udfs.sh" || {
                print_warn "Failed to install UDFs automatically"
                print_warn "You can install manually with: cd scripts && ./install_udfs.sh"
            }
        else
            print_warn "UDF install script not found, skipping"
        fi

        print_info "Auto-installation complete!"
        echo ""
    else
        print_info "Auto-installation disabled (set AUTO_INSTALL_ON_STARTUP=true in .env to enable)"
        print_info "To install manually:"
        print_info "  - Agentics: cd scripts && ./install_agentics_in_flink.sh"
        print_info "  - UDFs: cd scripts && ./install_udfs.sh"
        echo ""
    fi

    print_info "Starting Function Persistence Service..."
    # Run persistence service locally
    nohup $PYTHON_CMD backend/persistent_function_store.py > /tmp/persistence.log 2>&1 &
    echo $! > /tmp/persistence.pid
    sleep 2

    print_info "Starting AGstream Manager (Unified Schema + Transduction + Listener Management)..."
    # Run AGstream manager locally
    nohup $PYTHON_CMD backend/agstream_manager_service.py > /tmp/agstream_manager.log 2>&1 &
    echo $! > /tmp/agstream_manager.pid
    sleep 5

    # Generate and install Flink table definitions
    print_info "Generating Flink table definitions from AGStream Manager..."
    INIT_SCRIPT="$SCRIPT_DIR/init_flink_tables.py"
    SQL_FILE="/tmp/flink_init_tables.sql"

    # Always create empty init file in container first (ensures it exists)
    print_info "Creating empty init file in Flink container..."
    if docker exec flink-jobmanager bash -c "echo '-- Flink SQL Initialization' > /tmp/init_tables.sql" 2>/dev/null; then
        print_info "✓ Empty init file created"
    else
        print_warn "Could not create init file in container"
    fi

    if [ -f "$INIT_SCRIPT" ]; then
        # Wait for AGStream Manager to be ready
        MAX_RETRIES=10
        RETRY=0
        while [ $RETRY -lt $MAX_RETRIES ]; do
            if curl -s http://localhost:5003/api/channels > /dev/null 2>&1; then
                print_info "✓ AGStream Manager is ready"
                break
            fi
            RETRY=$((RETRY + 1))
            if [ $RETRY -eq $MAX_RETRIES ]; then
                print_warn "AGStream Manager not responding, using empty init file"
                break
            fi
            sleep 1
        done

        if [ $RETRY -lt $MAX_RETRIES ]; then
            # Generate SQL from channels
            if python3 "$INIT_SCRIPT" > "$SQL_FILE" 2>/dev/null; then
                # Copy to Flink container
                if docker cp "$SQL_FILE" flink-jobmanager:/tmp/init_tables.sql 2>/dev/null; then
                    print_info "✓ Table definitions installed in Flink"
                else
                    print_warn "Could not copy table definitions to Flink (using empty init file)"
                fi
            else
                print_warn "Could not generate table definitions (using empty init file)"
            fi
        fi
    else
        print_warn "Table generation script not found (using empty init file)"
    fi

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
    print_info "  - Run: cd tools/agstream_manager/scripts && ./flink_sql.sh"
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

# Function to clean Flink resources
clean_flink_resources() {
    print_info "Cleaning Flink resources..."

    # Check if Flink containers are running
    if docker ps --format '{{.Names}}' | grep -q "flink-jobmanager"; then
        print_info "Cancelling all running Flink jobs via REST API..."

        # Use REST API to cancel jobs (more reliable)
        FLINK_URL="http://localhost:8085"
        JOBS=$(curl -s "$FLINK_URL/jobs" 2>/dev/null | python3 -c "
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

        if [ -n "$JOBS" ]; then
            JOB_COUNT=$(echo "$JOBS" | wc -l | tr -d ' ')
            print_info "Found $JOB_COUNT running job(s), cancelling..."
            for JOB_ID in $JOBS; do
                print_info "  Cancelling job: $JOB_ID"
                curl -s -X PATCH "$FLINK_URL/jobs/$JOB_ID?mode=cancel" > /dev/null 2>&1 || true
            done
            print_info "Waiting for jobs to terminate..."
            sleep 3
        else
            print_info "No running jobs found"
        fi

        print_info "Clearing Flink checkpoints and savepoints..."
        docker exec flink-jobmanager bash -c "rm -rf /tmp/flink-checkpoints/* /tmp/flink-savepoints/* 2>/dev/null" || true
        docker exec flink-taskmanager bash -c "rm -rf /tmp/flink-checkpoints/* /tmp/flink-savepoints/* 2>/dev/null" || true

        print_info "✓ Flink resources cleaned (jobs cancelled, checkpoints cleared)"
    else
        print_warn "Flink containers not running, skipping resource cleanup"
    fi
}

# Function to restart services (QUICK - just restart containers)
restart_services() {
    print_info "🔄 Quick Restart - Restarting all services..."
    print_info "   (This is fast - just stops/starts existing containers)"

    # Clean Flink resources before stopping
    clean_flink_resources

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

    print_info "✓ Quick restart complete!"
}

# Function to build/rebuild everything (SLOW - full rebuild)
build_services() {
    print_info "🔨 Full Build - Rebuilding Docker images and reinstalling everything..."
    print_info "   (This is slow - rebuilds Flink images, reinstalls Agentics & UDFs)"
    print_info ""

    cd "$PROJECT_DIR"

    # Rebuild Flink images
    print_info "Step 1/3: Rebuilding Flink Docker images..."
    docker-compose -f "$COMPOSE_FILE" build flink-jobmanager flink-taskmanager

    print_info ""
    print_info "Step 2/3: Restarting services..."
    restart_services

    print_info ""
    print_info "Step 3/3: Reinstalling Agentics and UDFs..."
    if [ -f "$SCRIPT_DIR/install_agentics_in_flink.sh" ]; then
        bash "$SCRIPT_DIR/install_agentics_in_flink.sh" --force || {
            print_warn "Agentics installation had issues, but continuing..."
        }
    fi

    if [ -f "$SCRIPT_DIR/install_udfs.sh" ]; then
        bash "$SCRIPT_DIR/install_udfs.sh" || {
            print_warn "UDF installation had issues, but continuing..."
        }
    fi

    print_info ""
    print_info "✓ Full build complete!"
    print_info "   All Docker images rebuilt, Agentics and UDFs reinstalled"
}

# Function to clean everything (SAFE - clears all data)
clean_services() {
    print_info "🧹 Clean - Removing all data and resources..."
    print_info "   (This is safe but destructive - clears Kafka data, Flink state, etc.)"
    print_info ""

    # Confirm with user
    read -p "⚠️  This will delete ALL Kafka data and Flink state. Continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Clean cancelled"
        return 0
    fi

    print_info "Step 1/4: Stopping all services..."
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

    print_info ""
    print_info "Step 2/4: Clearing Kafka data from: $KAFKA_DATA_DIR"
    rm -rf "$KAFKA_DATA_DIR"/*

    print_info ""
    print_info "Step 3/4: Clearing Flink state..."
    docker exec flink-jobmanager bash -c "rm -rf /tmp/flink-* 2>/dev/null" 2>/dev/null || true
    docker exec flink-taskmanager bash -c "rm -rf /tmp/flink-* 2>/dev/null" 2>/dev/null || true

    print_info ""
    print_info "Step 4/4: Starting services..."
    sleep 2
    start_services

    print_info ""
    print_info "✓ Clean complete!"
    print_info "   All data cleared, services restarted with fresh state"
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
    cd "$SCRIPT_DIR"
    ./flink_sql.sh
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

    # Need to run from the agentics root directory (2 levels up from PROJECT_DIR)
    AGENTICS_ROOT="$(cd "$PROJECT_DIR/../.." && pwd)"
    cd "$AGENTICS_ROOT"
    nohup $PYTHON_CMD tools/agstream_manager/backend/function_persistence_service.py > /tmp/persistence.log 2>&1 &
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
    build)
        build_services
        ;;
    clean)
        clean_services
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
        echo "╔════════════════════════════════════════════════════════════════╗"
        echo "║   Agentics Services Manager (Full Stack with Flink)           ║"
        echo "╚════════════════════════════════════════════════════════════════╝"
        echo ""
        echo "Usage: $0 {start|stop|restart|build|clean|status|logs|flink-sql|...}"
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "🚀 MAIN COMMANDS (Use these for most operations)"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "  restart                        - 🔄 Quick restart (FAST - just restart containers)"
        echo "                                   Use for: code changes, config updates, quick fixes"
        echo ""
        echo "  build                          - 🔨 Full rebuild (SLOW - rebuild images + reinstall)"
        echo "                                   Use for: Dockerfile changes, dependency updates"
        echo "                                   Rebuilds Flink images, reinstalls Agentics & UDFs"
        echo ""
        echo "  clean                          - 🧹 Clean everything (SAFE - clears all data)"
        echo "                                   Use for: fresh start, fix corrupted state"
        echo "                                   Clears Kafka data, Flink state, then restarts"
        echo "                                   ⚠️  Requires confirmation (destructive)"
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "📋 OTHER COMMANDS"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "  start / stop                   - Start/stop all services"
        echo "  status                         - Show Docker service status"
        echo "  logs [service]                 - Show Docker logs"
        echo "  flink-sql                      - Start Flink SQL Client"
        echo "  start-persistence              - Start only persistence service"
        echo "  start-agstream-manager         - Start only AGstream Manager"
        echo "  start-streamlit                - Start only Streamlit"
        echo "  dashboard                      - Show access URLs"
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "💡 EXAMPLES"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "  # Quick restart after code changes"
        echo "  $0 restart"
        echo ""
        echo "  # Full rebuild after Dockerfile changes"
        echo "  $0 build"
        echo ""
        echo "  # Clean everything and start fresh"
        echo "  $0 clean"
        echo ""
        echo "  # View logs"
        echo "  $0 logs kafka"
        echo "  $0 logs flink-jobmanager"
        echo ""
        echo "  # Open Flink SQL"
        echo "  $0 flink-sql"
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
        echo "  - Alternative: cd tools/agstream_manager/scripts && ./flink_sql.sh"
        exit 1
        ;;
esac

# Made with Bob
