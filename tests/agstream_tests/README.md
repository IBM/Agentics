# AGStream Tests

Integration tests for AGStream functionality.

## Prerequisites

**Required Dependencies:**
```bash
# Install AGStream dependencies
uv sync --group agstream
```

**Required Services:**
- Kafka running on localhost:9092
- Schema Registry (Karapace) on localhost:8081
- Optional: LLM API key for transduction tests

## Quick Start

```bash
# Run the main integration test
python tests/agstream_tests/test_agstream_integration.py

# Or with pytest
pytest tests/agstream_tests/ -v
```

**Note:** These tests are automatically skipped when kafka dependencies are not installed. Core framework tests can run without AGStream dependencies.

## Test Coverage

The integration test suite covers:
- ✅ Schema registration with Avro
- ✅ Message production to Kafka topics
- ✅ Message consumption from Kafka topics
- ✅ Transducible function execution
- ✅ Full produce-consume-transform cycle

## Troubleshooting

### Check Schema Registry
```bash
curl http://localhost:8081/subjects
```

Refer to AGStream documentation for service setup and detailed information.
