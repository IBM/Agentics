# AGStream Manager UDFs

Python User-Defined Functions (UDFs) for Flink SQL with Agentics integration.

## 📁 Directory Structure

```
udfs/
├── README.md                          # This file
├── UDF_GUIDE.md                       # Complete UDF documentation
├── AGSTREAM_UTILITIES_GUIDE.md        # AGStream utilities documentation
├── semantic_operators.py              # Main UDF implementations
├── generic_semantic_udf.py            # Generic semantic UDF with schema registry
├── agstream_utilities.py              # Schema Registry query utilities
├── udfs_example.py                    # Simple example UDFs
└── examples/
    ├── sentiment_analysis_example.py  # Sentiment analysis example
    └── document_summarization_example.py  # Document summarization example
```

**Note:** Test files have been moved to [`../tests/`](../tests/)

## 🚀 Quick Start

### 1. Install UDFs

```bash
cd tools/agstream_manager
./scripts/install_udfs.sh
```

### 2. Register in Flink SQL

```sql
CREATE TEMPORARY SYSTEM FUNCTION analyze_sentiment
AS 'semantic_operators.analyze_sentiment'
LANGUAGE PYTHON;
```

### 3. Use in Queries

```sql
SELECT
    text,
    analyze_sentiment(text) as sentiment
FROM messages
LIMIT 10;
```

## 📖 Documentation

See **[UDF_GUIDE.md](UDF_GUIDE.md)** for complete documentation including:
- Installation and setup
- Creating custom UDFs
- Testing strategies
- Available UDFs
- Troubleshooting

## 🔧 Core UDF Files

### semantic_operators.py
Main UDF implementations including:
- `analyze_sentiment(text)` - Single-field sentiment analysis
- `analyze_row_sentiment(row_json)` - Row-level sentiment analysis
- `extract_sentiment_label(json)` - Extract sentiment label
- `extract_confidence(json)` - Extract confidence score
- `categorize_text(text)` - Text categorization
- `summarize_text(text)` - Text summarization
- `extract_entities(text)` - Named entity recognition
- `answer_question(question)` - Question answering
- `UniversalTransductionUDTF` - Universal transduction UDTF

### generic_semantic_udf.py
Generic semantic UDF with schema registry integration for flexible transformations.

### agstream_utilities.py
Utility UDFs for interacting with AGStream environment:
- `list_registered_types()` - List all registered types in Schema Registry
- `get_type_schema(type_name)` - Get complete schema for a type
- `get_type_fields(type_name)` - Get field names and types
- `type_exists(type_name)` - Check if a type is registered
- `schema_registry_info()` - Get Schema Registry connection info

See [AGSTREAM_UTILITIES_GUIDE.md](AGSTREAM_UTILITIES_GUIDE.md) for detailed documentation.

### udfs_example.py
Simple example UDFs for learning:
- `add_prefix(text)` - Add prefix to text
- `uppercase_text(text)` - Convert to uppercase
- `word_count(text)` - Count words
- `format_with_confidence(text, confidence)` - Format with confidence
- `normalize_score(score)` - Normalize scores

## 🧪 Testing

All test files are located in [`../tests/`](../tests/)

### Run All Tests (Requires Flink)
```bash
cd tools/agstream_manager/tests
python test_all_udfs.py
```

### Run Standalone Tests (No Flink)
```bash
cd tools/agstream_manager/tests
python test_all_udfs_standalone.py
```

### Test Specific UDF
```bash
cd tools/agstream_manager/tests
python test_row_sentiment.py
python test_sentiment_standalone.py
```

## 📝 Creating Custom UDFs

1. Create a new Python file in this directory
2. Define your UDF using PyFlink decorators
3. Install: `./scripts/install_udfs.sh`
4. Register in Flink SQL
5. Use in queries

Example:
```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

@udf(result_type=DataTypes.STRING())
def my_custom_udf(text):
    if text is None:
        return None
    return text.upper()
```

See [UDF_GUIDE.md](UDF_GUIDE.md) for detailed instructions.

## 🔗 Related Documentation

- [UDF Guide](UDF_GUIDE.md) - Complete UDF documentation
- [AGStream Manager Guide](../docs/AGSTREAM_MANAGER_GUIDE.md) - Full system guide
- [Main README](../README.md) - AGStream Manager overview

## 💡 Examples

The `examples/` directory contains complete working examples:
- **sentiment_analysis_example.py** - Sentiment analysis with Agentics
- **document_summarization_example.py** - Document summarization

## 🆘 Need Help?

See the [Troubleshooting section](UDF_GUIDE.md#troubleshooting) in UDF_GUIDE.md for common issues and solutions.
