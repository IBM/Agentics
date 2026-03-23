# AGStream Manager Tests

This directory contains tests for the AGStream Manager, organized into two categories:

## Flink Container Tests (Current Directory)

These tests **MUST** run inside the Flink Docker container because they require:
- `pyflink` library (only available in Flink container)
- Access to Flink's Table API and SQL environment
- `semantic_operators` module from agentics
- Real LLM API calls for AGMap/AGReduce operations

### Test Files

1. **`test_agmap_simple.py`** - Diagnostic test (1 test, ~5-10 seconds)
   - Quick connectivity check with 30-second timeout
   - Verifies API keys and imports
   - Tests basic AGMap sentiment analysis

2. **`test_agmap_agreduce.py`** - Python function tests (18 tests, ~5-10 minutes)
   - Tests AGMap dynamic mode (on-the-fly type generation)
   - Tests AGMap registry mode (Schema Registry integration)
   - Tests AGReduce aggregation operations
   - Makes real LLM API calls (slow but thorough)

3. **`test_agmap_agreduce_sql.py`** - Flink SQL tests (10+ tests, ~3-5 minutes)
   - Tests registered UDFs in Flink SQL
   - End-to-end SQL query execution
   - Tests both AGMap and AGReduce in SQL context

### Running Flink Container Tests

```bash
# From tools/agstream_manager directory

# Run all Flink tests
./scripts/run_flink_tests.sh

# Run specific test file
./scripts/run_flink_tests.sh test_agmap_simple.py
./scripts/run_flink_tests.sh test_agmap_agreduce.py
./scripts/run_flink_tests.sh test_agmap_agreduce_sql.py
```

### What the Script Does

1. Checks if Flink container is running
2. Installs pytest in container if needed
3. Sets up environment (PYTHONPATH, symlinks)
4. Copies test files to `/opt/flink/tests/`
5. Runs tests with proper environment
6. Cleans up after completion

## Non-Flink Tests (non_flink_tests/ subdirectory)

These are API and integration tests that don't require the Flink container:

- `test_schema_enforcement.py` - Schema registry enforcement tests
- `test_topic_deletion.py` - Kafka topic deletion tests
- `test_create_listener.py` - Listener creation API tests
- `test_listener_persistence.py` - Listener persistence tests
- `test_listener_ui_fixed.py` - UI integration tests
- `test_schema_manager.py` - Schema manager tests

### Running Non-Flink Tests

These tests require:
- AGStream Manager backend service running on port 5003
- Kafka/Schema Registry services running

```bash
# From project root
cd tools/agstream_manager
uv run pytest non_flink_tests/ -v
```

## Test Markers

- `@pytest.mark.flink` - Tests that run in Flink container
- `@pytest.mark.agstream` - Tests that require Kafka/Flink services

## Environment Requirements

### Required Environment Variables

Create a `.env` file in the project root with:

```bash
OPENAI_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here  # Optional
GEMINI_API_KEY=your_key_here     # Optional
```

The Flink container automatically loads these from the project root `.env` file.

### Required Services

Start services before running tests:

```bash
cd tools/agstream_manager
./manage_services.sh start

# Verify services are running
docker ps
```

## Troubleshooting

### Tests Hang or Timeout

**Cause:** AGMap/AGReduce tests make real LLM API calls which can take 5-30 seconds each.

**Solutions:**
- Use the diagnostic test first: `./scripts/run_flink_tests.sh test_agmap_simple.py`
- Check API keys are set in `.env` file
- Verify network connectivity to LLM providers
- Be patient - 18 tests × 10 seconds = ~3 minutes minimum

### Import Errors (pyflink, semantic_operators)

**Cause:** These modules only exist in the Flink container.

**Solution:** Always use `run_flink_tests.sh` script to run these tests.

### "Container not running" Error

**Cause:** Flink services are not started.

**Solution:**
```bash
cd tools/agstream_manager
./manage_services.sh start
```

### Kafka Connection Errors

**Cause:** Kafka/Schema Registry services not ready.

**Solution:** Wait 30-60 seconds after starting services for full initialization.

## Test Execution Times

| Test File | Tests | Duration | Notes |
|-----------|-------|----------|-------|
| test_agmap_simple.py | 1 | ~10s | Diagnostic with timeout |
| test_agmap_agreduce.py | 18 | ~5-10min | Real LLM calls |
| test_agmap_agreduce_sql.py | 10+ | ~3-5min | SQL execution |
| **Total** | **29+** | **~10-15min** | Full suite |

## CI/CD Integration

For continuous integration:

```bash
# Start services
cd tools/agstream_manager
./manage_services.sh start

# Wait for services to be ready
sleep 60

# Run tests
./scripts/run_flink_tests.sh

# Stop services
./manage_services.sh stop
```

## Development Tips

1. **Use the diagnostic test first** to verify setup:
   ```bash
   ./scripts/run_flink_tests.sh test_agmap_simple.py
   ```

2. **Run specific tests** during development:
   ```bash
   ./scripts/run_flink_tests.sh test_agmap_agreduce.py::TestAGMapDynamic::test_agmap_sentiment_string
   ```

3. **Check logs** if tests fail:
   ```bash
   docker logs flink-jobmanager
   ```

4. **Restart services** if needed:
   ```bash
   ./manage_services.sh restart
