# Running Flink Tests

This guide explains how to run tests that require the Flink Docker environment.

## Prerequisites

1. **Docker and Docker Compose** installed
2. **AGStream Manager services** running

## Quick Start

### 1. Start AGStream Services

From the `tools/agstream_manager` directory:

```bash
cd tools/agstream_manager
./manage_services.sh start
```

This starts:
- Kafka (localhost:9092)
- Schema Registry/Karapace (localhost:8081)
- Flink JobManager and TaskManager
- AGStream Manager backend (localhost:5003)

### 2. Run Tests Inside Flink Container

The Flink-dependent tests require modules (`pyflink`, `semantic_operators`) that are only available inside the Flink Docker container.

**Option A: Use the helper script (Recommended)**
```bash
cd tools/agstream_manager

# Run all Flink tests
./scripts/run_flink_tests.sh

# Run specific test file
./scripts/run_flink_tests.sh test_all_udfs.py
./scripts/run_flink_tests.sh test_row_sentiment.py
```

The script automatically:
- Checks if Flink container is running
- Installs pytest if needed
- Copies tests to the container
- Runs the tests
- Cleans up after completion

**Option B: Manual execution**
```bash
# Install pytest in container (one-time setup)
docker exec flink-jobmanager python3 -m pip install pytest pytest-asyncio

# Copy tests to container
docker cp tools/agstream_manager/tests/ flink-jobmanager:/opt/flink/tests/

# Run tests
docker exec flink-jobmanager python3 -m pytest /opt/flink/tests/ -v

# Run specific test
docker exec flink-jobmanager python3 -m pytest /opt/flink/tests/test_all_udfs.py -v

# Cleanup
docker exec flink-jobmanager rm -rf /opt/flink/tests/
```

## Tests Requiring Flink Environment

**test_schema_enforcement.py** - Schema enforcement with pyflink

This test requires the Flink Docker container to run properly.

## Tests That Run Locally

These tests don't require Flink:

```bash
cd tools/agstream_manager
uv sync --all-groups

# Run individual tests
uv run pytest tests/test_create_listener.py
uv run pytest tests/test_listener_persistence.py
uv run pytest tests/test_schema_manager.py
uv run pytest tests/test_topic_deletion.py
uv run pytest tests/test_listener_ui_fixed.py
```

## Troubleshooting

### Container Not Found
```bash
# Check if containers are running
docker ps | grep flink

# If not running, start services
./manage_services.sh start
```

### Module Not Found Errors
If you see `ModuleNotFoundError: No module named 'semantic_operators'` or `'pyflink'`:
- These modules are only available inside the Flink container
- Run tests using `docker exec` as shown above

### Permission Errors
```bash
# If you get permission errors, try with sudo
sudo docker exec -it flink-jobmanager bash
```

## Viewing Test Results

Test results and HTML reports are generated in the container. To copy them out:

```bash
# Copy HTML report from container
docker cp flink-jobmanager:/opt/flink/report.html ./flink_test_report.html

# View in browser
open flink_test_report.html  # macOS
xdg-open flink_test_report.html  # Linux
```

## Stopping Services

```bash
cd tools/agstream_manager
./manage_services.sh stop
```

## Additional Resources

- [AGStream Manager README](../README.md)
- [Flink SQL Usage Guide](../FLINK_SQL_USAGE.md)
- [Setup Guide](../SETUP_GUIDE.md)
