# AGStream Manager Tests

Test suite for AGStream Manager UDFs and functionality.

## 📁 Test Files

### UDF Tests

#### `test_all_udfs.py`
Comprehensive test suite for all UDFs. **Requires running Flink cluster.**

```bash
cd tools/agstream_manager/tests
python test_all_udfs.py
```

Tests:
- Sentiment analysis UDFs
- Text processing UDFs
- Row-level sentiment analysis
- Universal transduction UDTF
- Helper functions

#### `test_all_udfs_standalone.py`
Standalone UDF tests that don't require Flink. **Fastest way to test UDF logic.**

```bash
cd tools/agstream_manager/tests
python test_all_udfs_standalone.py
```

Tests UDF functions directly in Python without Flink SQL overhead.

#### `test_row_sentiment.py`
Specific tests for row-level sentiment analysis UDF.

```bash
cd tools/agstream_manager/tests
python test_row_sentiment.py
```

#### `test_sentiment_standalone.py`
Standalone sentiment analysis tests.

```bash
cd tools/agstream_manager/tests
python test_sentiment_standalone.py
```

#### `test_row_sentiment.sql`
SQL test queries for row-level sentiment analysis. Use in Flink SQL client.

```bash
# Copy to Flink and run
cat test_row_sentiment.sql | ./manage_services_full.sh flink-sql
```

### AGStream Manager Tests

#### `test_schema_manager.py`
Tests for schema management functionality.

#### `test_schema_enforcement.py`
Tests for schema validation and enforcement.

#### `test_listener_persistence.py`
Tests for listener persistence across restarts.

#### `test_create_listener.py`
Tests for listener creation and management.

#### `test_listener_ui_fixed.py`
Tests for listener UI functionality.

#### `test_topic_deletion.py`
Tests for topic deletion and cleanup.

## 🚀 Running Tests

### Prerequisites

1. **Start AGStream Manager services:**
   ```bash
   cd tools/agstream_manager
   ./manage_services_full.sh start
   ```

2. **Ensure UDFs are installed:**
   ```bash
   ./scripts/install_udfs.sh
   ```

### Run All UDF Tests

```bash
cd tools/agstream_manager/tests

# With Flink (comprehensive)
python test_all_udfs.py

# Standalone (fast)
python test_all_udfs_standalone.py
```

### Run Specific Tests

```bash
cd tools/agstream_manager/tests

# Sentiment analysis
python test_sentiment_standalone.py
python test_row_sentiment.py

# Schema management
python test_schema_manager.py

# Listener functionality
python test_listener_persistence.py
```

### Run SQL Tests

```bash
cd tools/agstream_manager

# Open Flink SQL client
./manage_services_full.sh flink-sql

# Then paste contents of test_row_sentiment.sql
```

## 📊 Test Categories

### Unit Tests (Standalone)
- `test_all_udfs_standalone.py`
- `test_sentiment_standalone.py`

**Pros:** Fast, no dependencies
**Cons:** Doesn't test Flink integration

### Integration Tests (Requires Flink)
- `test_all_udfs.py`
- `test_row_sentiment.py`

**Pros:** Tests full stack
**Cons:** Slower, requires running services

### SQL Tests
- `test_row_sentiment.sql`

**Pros:** Tests actual SQL usage
**Cons:** Manual execution

## 🔧 Test Development

### Adding New UDF Tests

1. Create test file in this directory
2. Import UDF functions from `../udfs/`
3. Write test cases
4. Run tests

Example:
```python
import sys
sys.path.insert(0, '../udfs')
from semantic_operators import analyze_sentiment

def test_sentiment():
    result = analyze_sentiment("I love this!")
    assert "POSITIVE" in result
    print("✓ Test passed")

if __name__ == "__main__":
    test_sentiment()
```

### Best Practices

1. **Use standalone tests for quick iteration**
2. **Use integration tests for final validation**
3. **Test edge cases** (None, empty strings, special characters)
4. **Test error handling**
5. **Document expected behavior**

## 🆘 Troubleshooting

### Tests Fail with "Module not found"

```bash
# Ensure you're in the tests directory
cd tools/agstream_manager/tests

# Check Python path
python -c "import sys; print(sys.path)"
```

### Flink Tests Fail

```bash
# Check if Flink is running
docker ps | grep flink

# Restart if needed
cd tools/agstream_manager
./manage_services_full.sh restart
```

### UDF Not Found in Tests

```bash
# Reinstall UDFs
cd tools/agstream_manager
./scripts/install_udfs.sh
```

## 📖 Related Documentation

- [UDF Guide](../udfs/UDF_GUIDE.md) - Complete UDF documentation
- [AGStream Manager Guide](../docs/AGSTREAM_MANAGER_GUIDE.md) - Full system guide
- [UDFs README](../udfs/README.md) - UDF directory overview

## 🎯 Quick Test Commands

```bash
# Fast standalone tests
cd tools/agstream_manager/tests && python test_all_udfs_standalone.py

# Full integration tests
cd tools/agstream_manager/tests && python test_all_udfs.py

# Specific UDF test
cd tools/agstream_manager/tests && python test_row_sentiment.py
