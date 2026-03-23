# Agentics Test Suite

This directory contains the test suite for the Agentics framework, organized into different test groups using pytest markers.

## Test Groups

### Core Tests (`@pytest.mark.core`)
Core functionality tests that don't require external infrastructure like Kafka or Schema Registry. These tests include:

- **test_examples.py**: Tests for example scripts (hello_world, emotion_extractor, generate_tweets, etc.)
- **test_listener_manager.py**: Unit tests for the ListenerManager using mocked AGStream
- **test_package.py**: Package import and installation tests
- **test_package_generation.py**: Package build and distribution tests
- **test_tutorials.py**: Tutorial notebook execution tests

### AGStream Tests (`@pytest.mark.agstream`)
Integration tests that require Kafka, Schema Registry (Karapace), and optionally Flink infrastructure.

**Test Locations:**
- `tests/agstream_tests/` - Core AGStream integration tests
- `tools/agstream_manager/tests/` - AGStream Manager and Flink SQL tests

**Running AGStream Tests:**

```bash
# Install agstream dependencies
uv sync --group agstream

# Run agstream tests (some may require Flink environment)
uv run pytest -m agstream
```

**Important Notes:**
- Some tests in `tools/agstream_manager/tests/` require Flink-specific modules (`pyflink`, `semantic_operators`) that are only available in the Flink Docker container
- These tests are designed to run inside the Flink environment or can be run as standalone scripts
- Tests that can run outside Flink will execute normally with `pytest -m agstream`

**Test Coverage:**
- Avro message production and consumption
- Schema registry integration
- Topic management
- Transducible function execution
- Listener lifecycle management
- Flink SQL UDF testing (requires Flink environment)

## Running Tests

### Run All Tests
```bash
uv run pytest
```

### Run Only Core Tests
```bash
uv run pytest -m core
```

### Run Only AGStream Tests

**Core AGStream Tests (from project root):**
```bash
# Install agstream dependencies first
uv sync --group agstream

# Run agstream tests
uv run pytest -m agstream
```

**AGStream Manager Tests (from tools/agstream_manager):**

```bash
cd tools/agstream_manager
uv sync --all-groups
```

**Run tests locally (without Flink):**
```bash
# These tests work without Flink Docker environment
uv run pytest tests/test_create_listener.py
uv run pytest tests/test_listener_persistence.py
uv run pytest tests/test_schema_manager.py
uv run pytest tests/test_topic_deletion.py
```

**Run Flink-dependent tests (requires Flink Docker environment):**

```bash
# Start Flink services first
cd tools/agstream_manager
./manage_services.sh start

# Use the helper script to run tests in Flink container
./scripts/run_flink_tests.sh

# Or run specific test
./scripts/run_flink_tests.sh test_schema_enforcement.py
```

**Tests requiring Flink Docker environment:**
- `test_schema_enforcement.py` - Schema enforcement with pyflink

The helper script automatically:
- Installs pytest in the Flink container
- Sets up the correct PYTHONPATH
- Copies tests to the container
- Runs the tests
- Cleans up afterwards

See [Flink Tests Guide](../tools/agstream_manager/tests/FLINK_TESTS.md) for detailed instructions.

### Run Tests with Verbose Output
```bash
uv run pytest -v
```

### Run Specific Test File
```bash
uv run pytest tests/test_examples.py
```

### Run with Custom Timeout
```bash
uv run pytest --timeout=60
```

## Prerequisites

### Core Tests
- Python 3.10-3.12
- Dependencies installed via `uv sync`

### AGStream Tests
In addition to core requirements:
- Kafka running on localhost:9092
- Schema Registry (Karapace) on localhost:8081
- AGStream Manager service on localhost:5003 (optional for some tests)

To start the required infrastructure:
```bash
cd tools/agstream_manager
./manage_services.sh start
```

## Test Reports

After running tests, an HTML report is generated at `report.html` in the project root. Open it in a browser to view detailed test results.

## Continuous Integration

The test suite is designed to run in CI/CD pipelines. Core tests should always pass, while AGStream tests may be skipped if the required infrastructure is not available.

## Adding New Tests

When adding new tests:

1. **For core functionality**: Add `@pytest.mark.core` decorator
2. **For AGStream features**: Add `@pytest.mark.agstream` decorator
3. **For module-level marking**: Use `pytestmark = pytest.mark.core` or `pytestmark = pytest.mark.agstream`

Example:
```python
import pytest

@pytest.mark.core
def test_my_feature():
    assert True
```

Or for entire module:
```python
import pytest

pytestmark = pytest.mark.agstream

def test_kafka_integration():
    # Test code here
    pass
