# Running Tests with UV

This guide explains how to run all tests in the Agentics project using the `uv` package manager.

## Quick Start

**For fast validation (recommended):**
```bash
# Core tests only (~30 seconds)
uv run pytest tests/ -m core -v
```

**For full test suite:**
```bash
uv run pytest tests/ -v
```

## Test Organization

Tests are organized into two main categories:

1. **Core Tests** - Framework tests that don't require Kafka/Flink infrastructure (~30 seconds)
2. **AGStream Tests** - Integration tests that require running Kafka/Flink services (~2 minutes)

## Prerequisites

### For Core Tests
```bash
# Install dependencies
uv sync --all-groups
```

### For AGStream Tests
```bash
# AGStream tests require Kafka/Flink services to be running
# Refer to AGStream documentation for service setup
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


## Test Markers

Tests use pytest markers for categorization:

- `@pytest.mark.core` - Core framework tests (no infrastructure needed)
- `@pytest.mark.agstream` - Tests requiring Kafka/Flink services

## Common Issues

### Issue: "Connection refused" or Kafka errors
**Solution:** Ensure Kafka/Flink services are running (refer to AGStream documentation)

### Issue: Tests hang or timeout
**Solution:**
- Check API keys are set in `.env` file
- Some tests make real LLM API calls and can take 5-30 seconds each

### Issue: "kafka-python-ng" not found
**Solution:** Run `uv sync --all-groups` from project root

## Test Execution Times

- **Core tests**: ~10-30 seconds (no LLM calls)
- **AGStream integration tests**: ~1-2 minutes (includes Kafka operations)

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

# For full testing (requires services)
uv run pytest tests/ -v
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
