# AGStream Tests

Integration tests for AGStream functionality.

## Prerequisites

**Required Dependencies:**
```bash
# Install AGStream dependencies (kafka, flink)
cd tools/agstream_manager
uv sync
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

### Start Services
```bash
cd /Users/gliozzo/Code/agentics911/agentics
./manage_services_full.sh start
```

### Check Schema Registry
```bash
curl http://localhost:8081/subjects
```

For detailed AGStream documentation, see [`tools/agstream_manager/README.md`](../../tools/agstream_manager/README.md).
