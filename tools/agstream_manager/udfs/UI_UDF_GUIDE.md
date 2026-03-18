# Using Python UDFs in AGStream Manager UI

## ✅ Python UDFs Are Already Available!

The AGStream Manager UI's SQL terminal connects to the same Flink container where Python UDFs are installed. You can use them immediately!

## 🚀 Quick Start

### 1. Open the SQL Terminal in UI

Click the "Open Flink SQL Terminal" button in the AGStream Manager UI.

### 2. Register Your UDFs

```sql
-- Register the add_prefix function
CREATE TEMPORARY SYSTEM FUNCTION add_prefix
AS 'udfs_example.add_prefix'
LANGUAGE PYTHON;

-- Register other functions
CREATE TEMPORARY SYSTEM FUNCTION uppercase_text
AS 'udfs_example.uppercase_text'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION word_count
AS 'udfs_example.word_count'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION format_with_confidence
AS 'udfs_example.format_with_confidence'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION normalize_score
AS 'udfs_example.normalize_score'
LANGUAGE PYTHON;
```

### 3. Use UDFs with AGStream Topics

The UI automatically creates tables for your AGStream channels (Q3, A3, etc.). Use UDFs on them:

```sql
-- Add prefix to questions
SELECT add_prefix(text) as prefixed_question FROM Q3;

-- Convert answers to uppercase
SELECT uppercase_text(text) as upper_answer FROM A3;

-- Count words in questions
SELECT text, word_count(text) as num_words FROM Q3;

-- Format with confidence
SELECT format_with_confidence(text, confidence) as formatted FROM A3;
```

## 📚 Available UDFs

All UDFs are defined in `/opt/flink/udfs/udfs_example.py`:

### 1. `add_prefix(text: STRING) -> STRING`
Adds "Q: " prefix to text.

**Example:**
```sql
SELECT add_prefix(text) FROM Q3;
-- Input: "What is AI?"
-- Output: "Q: What is AI?"
```

### 2. `uppercase_text(text: STRING) -> STRING`
Converts text to uppercase.

**Example:**
```sql
SELECT uppercase_text(text) FROM A3;
-- Input: "hello world"
-- Output: "HELLO WORLD"
```

### 3. `word_count(text: STRING) -> INT`
Counts the number of words in text.

**Example:**
```sql
SELECT text, word_count(text) as words FROM Q3;
-- Input: "What is machine learning?"
-- Output: text="What is machine learning?", words=4
```

### 4. `format_with_confidence(text: STRING, confidence: DOUBLE) -> STRING`
Formats text with confidence level indicator.

**Example:**
```sql
SELECT format_with_confidence(text, confidence) FROM A3;
-- Input: text="AI is...", confidence=0.95
-- Output: "[HIGH] AI is..."
```

### 5. `normalize_score(score: DOUBLE) -> DOUBLE`
Normalizes a score to 0-1 range.

**Example:**
```sql
SELECT normalize_score(score) FROM results_table;
-- Input: 1.5
-- Output: 1.0
```

## 🔧 Creating Custom UDFs

### 1. Edit the UDF File

Edit `tools/agstream_manager/udfs/udfs_example.py`:

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

@udf(result_type=DataTypes.STRING())
def my_custom_function(input_text):
    """Your custom logic here"""
    if input_text is None:
        return None
    return f"Processed: {input_text}"
```

### 2. Register in SQL

```sql
CREATE TEMPORARY SYSTEM FUNCTION my_custom_function
AS 'udfs_example.my_custom_function'
LANGUAGE PYTHON;
```

### 3. Use It

```sql
SELECT my_custom_function(text) FROM Q3;
```

## 💡 Tips

1. **UDFs persist in session**: Once registered, UDFs remain available until you close the SQL terminal.

2. **Reuse across queries**: Register once, use in multiple queries.

3. **Combine with AGStream**: UDFs work seamlessly with AGStream topics created via the UI.

4. **Real-time processing**: UDFs process streaming data in real-time as it arrives.

5. **Error handling**: Always handle NULL values in your UDF code.

## 🎯 Complete Example

```sql
-- Register UDFs
CREATE TEMPORARY SYSTEM FUNCTION add_prefix AS 'udfs_example.add_prefix' LANGUAGE PYTHON;
CREATE TEMPORARY SYSTEM FUNCTION word_count AS 'udfs_example.word_count' LANGUAGE PYTHON;

-- Use with AGStream topics
SELECT
    add_prefix(text) as formatted_question,
    word_count(text) as question_length
FROM Q3
WHERE word_count(text) > 3;
```

## 🔄 Updates

Changes to UDF files are reflected immediately (via volume mount). No need to rebuild the Docker image for UDF updates!

## 📝 Notes

- UDFs run in the Flink TaskManager containers
- Python 3.10.12 and PyFlink 1.18.1 are pre-installed
- UDF files are located at `/opt/flink/udfs/` in the containers
- All Python standard library modules are available

---

**Python UDFs are production-ready in the AGStream Manager UI!** 🎉
