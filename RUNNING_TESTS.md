# Running Tests with UV

This guide explains how to run all tests in the Agentics project using the `uv` package manager.

## Quick Start

**For fast validation (recommended):**
```bash
# Core tests only (~30 seconds)
uv run pytest tests/ -m core -v

# Quick Flink diagnostic test (~10 seconds)
cd tools/agstream_manager && ./scripts/run_flink_tests.sh test_agmap_simple.py
```

**For full test suite (~15-20 minutes):**
```bash
uv run pytest tests/ -v
cd tools/agstream_manager && ./scripts/run_flink_tests.sh
```

## Test Organization

Tests are organized into two main categories:

1. **Core Tests** - Framework tests that don't require Kafka/Flink infrastructure (~30 seconds)
2. **AGStream Tests** - Integration tests that require running Kafka/Flink services (~2 minutes)
3. **Flink Container Tests** - UDF tests with real LLM API calls (~10-15 minutes) ⚠️ SLOW

## Prerequisites

### For Core Tests
```bash
# Install dependencies
uv sync --all-groups
```

### For AGStream Tests
```bash
# Start Kafka/Flink services
cd tools/agstream_manager
./manage_services.sh start

# Wait for services to be ready (check with docker ps)
```

## Running Tests

### 1. Run All Core Tests (Fast)
```bash
# From project root
uv run pytest tests/ -m core -v
```

### 2. Run All AGStream Tests (Requires Services)
```bash
# From project root
uv run pytest tests/ -m agstream -v
```

### 3. Run All Tests
```bash
# From project root
uv run pytest tests/ -v
```

### 4. Run Specific Test File
```bash
# Core test example
uv run pytest tests/test_package.py -v

# AGStream test example (requires services)
uv run pytest tests/agstream_tests/test_agstream_integration.py -v
```

### 5. Run Tests in AGStream Manager Directory

The `tools/agstream_manager/tests/` directory contains tests that must run inside the Flink Docker container because they require `pyflink` and access to Flink's environment.

#### Run Quick Diagnostic Test (Recommended First)
```bash
cd tools/agstream_manager
./scripts/run_flink_tests.sh test_agmap_simple.py
```
This runs 1 test in ~10 seconds to verify setup.

#### Run All Flink Container Tests (SLOW - 10-15 minutes)
```bash
cd tools/agstream_manager
./scripts/run_flink_tests.sh
```
⚠️ **Warning:** These tests make real LLM API calls and take 10-15 minutes to complete.

#### Run Specific Flink Container Test
```bash
cd tools/agstream_manager
# Quick diagnostic (1 test, ~10s)
./scripts/run_flink_tests.sh test_agmap_simple.py

# Python function tests (18 tests, ~5-10min) - SLOW
./scripts/run_flink_tests.sh test_agmap_agreduce.py

# SQL tests (10+ tests, ~3-5min) - SLOW
./scripts/run_flink_tests.sh test_agmap_agreduce_sql.py
```

#### What the Script Does
1. Checks if Flink container is running
2. Installs pytest in the container if needed
3. Sets up the test environment (PYTHONPATH, symlinks)
4. Copies test files to `/opt/flink/tests/`
5. Runs tests with proper environment variables
6. Cleans up test files after completion

## Test Markers

Tests use pytest markers for categorization:

- `@pytest.mark.core` - Core framework tests (no infrastructure needed)
- `@pytest.mark.agstream` - Tests requiring Kafka/Flink services

## Common Issues

### Issue: "ModuleNotFoundError: No module named 'pyflink'"
**Solution:** These tests must run inside the Flink container using `run_flink_tests.sh`

### Issue: "Connection refused" or Kafka errors
**Solution:** Start services with `cd tools/agstream_manager && ./manage_services.sh start`

### Issue: Tests hang or timeout
**Solution:**
- Check API keys are set in `.env` file
- AGMap/AGReduce tests make real LLM API calls and can take 5-30 seconds each
- Use the diagnostic test to verify connectivity: `./scripts/run_flink_tests.sh test_agmap_simple.py`

### Issue: "kafka-python-ng" not found
**Solution:** Run `uv sync --all-groups` from project root

## Test Execution Times

- **Core tests**: ~10-30 seconds (no LLM calls)
- **AGStream integration tests**: ~1-2 minutes (includes Kafka operations)
- **Flink container tests**: ~3-5 minutes (includes LLM API calls)
  - `test_agmap_simple.py`: ~2 minutes (1 test, 120s timeout)
  - `test_agmap_agreduce.py`: ~2-3 minutes (8 tests with LLM calls)
  - `test_agmap_agreduce_sql.py`: ~1-2 minutes (4 SQL tests with Flink)

## Environment Variables

Required for AGStream tests:
```bash
# In project root .env
OPENAI_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here  # Optional
GEMINI_API_KEY=your_key_here     # Optional
```

The Flink container automatically loads these from the `.env` file in the project root.

## Continuous Integration

For CI/CD pipelines:

```bash
# Run only core tests (fast, no infrastructure)
uv run pytest tests/ -m core -v

# For full testing, start services first
cd tools/agstream_manager
./manage_services.sh start
cd ../..
uv run pytest tests/ -v
cd tools/agstream_manager
./scripts/run_flink_tests.sh
```

## Test Coverage

Generate coverage report:
```bash
uv run pytest tests/ --cov=src/agentics --cov-report=html
```

View coverage:
```bash
open htmlcov/index.html
```

## Debugging Tests

Run with verbose output and show print statements:
```bash
uv run pytest tests/test_file.py -v -s
```

Run specific test function:
```bash
uv run pytest tests/test_file.py::test_function_name -v -s
```

Run with pdb debugger on failure:
```bash
uv run pytest tests/test_file.py --pdb
```

## Summary

| Test Type | Command | Requirements | Duration |
|-----------|---------|--------------|----------|
| Core only | `uv run pytest tests/ -m core -v` | None | ~30s |
| AGStream only | `uv run pytest tests/ -m agstream -v` | Services running | ~2min |
| All project tests | `uv run pytest tests/ -v` | Services running | ~3min |
| Flink container tests | `cd tools/agstream_manager && ./scripts/run_flink_tests.sh` | Services running | ~10min |
| Everything | Run all commands above | Services running | ~15min |
