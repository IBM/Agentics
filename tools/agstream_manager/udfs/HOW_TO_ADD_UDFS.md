# How to Add Custom Python UDFs

## 📝 Quick Guide

### Step 1: Edit the UDF File

Open `tools/agstream_manager/udfs/udfs_example.py` and add your function:

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

@udf(result_type=DataTypes.STRING())
def my_custom_function(input_text):
    """Your function description"""
    # Handle NULL values
    if input_text is None:
        return None

    # Your logic here
    result = input_text.upper()
    return result
```

### Step 2: Save the File

Changes are automatically available via volume mount - **no rebuild needed!**

### Step 3: Register in Flink SQL

```sql
CREATE TEMPORARY SYSTEM FUNCTION my_custom_function
AS 'udfs_example.my_custom_function'
LANGUAGE PYTHON;
```

### Step 4: Use It

```sql
SELECT my_custom_function(text) FROM Q3;
```

## 🎯 Complete Examples

### Example 1: Simple String Processing

```python
@udf(result_type=DataTypes.STRING())
def reverse_text(text):
    """Reverse the input text"""
    if text is None:
        return None
    return text[::-1]
```

**Usage:**
```sql
CREATE TEMPORARY SYSTEM FUNCTION reverse_text AS 'udfs_example.reverse_text' LANGUAGE PYTHON;
SELECT reverse_text(text) FROM Q3;
```

### Example 2: Numeric Calculation

```python
@udf(result_type=DataTypes.DOUBLE())
def calculate_discount(price, discount_percent):
    """Calculate discounted price"""
    if price is None or discount_percent is None:
        return 0.0
    return float(price) * (1 - float(discount_percent) / 100)
```

**Usage:**
```sql
CREATE TEMPORARY SYSTEM FUNCTION calculate_discount AS 'udfs_example.calculate_discount' LANGUAGE PYTHON;
SELECT price, calculate_discount(price, 10) as discounted_price FROM products;
```

### Example 3: Boolean Logic

```python
@udf(result_type=DataTypes.BOOLEAN())
def is_long_text(text, min_length):
    """Check if text exceeds minimum length"""
    if text is None:
        return False
    return len(text) >= int(min_length)
```

**Usage:**
```sql
CREATE TEMPORARY SYSTEM FUNCTION is_long_text AS 'udfs_example.is_long_text' LANGUAGE PYTHON;
SELECT text FROM Q3 WHERE is_long_text(text, 100);
```

### Example 4: Array/List Output

```python
@udf(result_type=DataTypes.ARRAY(DataTypes.STRING()))
def extract_hashtags(text):
    """Extract hashtags from text"""
    if text is None:
        return []
    import re
    return re.findall(r'#\w+', text)
```

**Usage:**
```sql
CREATE TEMPORARY SYSTEM FUNCTION extract_hashtags AS 'udfs_example.extract_hashtags' LANGUAGE PYTHON;
SELECT text, extract_hashtags(text) as hashtags FROM tweets;
```

### Example 5: JSON Processing

```python
@udf(result_type=DataTypes.STRING())
def extract_json_field(json_str, field_name):
    """Extract field from JSON string"""
    if json_str is None or field_name is None:
        return None
    import json
    try:
        data = json.loads(json_str)
        return str(data.get(field_name, ''))
    except:
        return None
```

**Usage:**
```sql
CREATE TEMPORARY SYSTEM FUNCTION extract_json_field AS 'udfs_example.extract_json_field' LANGUAGE PYTHON;
SELECT extract_json_field(metadata, 'author') as author FROM documents;
```

## 📚 Data Types Reference

### Common PyFlink Data Types:

```python
DataTypes.STRING()          # Text
DataTypes.INT()             # Integer
DataTypes.BIGINT()          # Long integer
DataTypes.DOUBLE()          # Floating point
DataTypes.FLOAT()           # Single precision float
DataTypes.BOOLEAN()         # True/False
DataTypes.TIMESTAMP()       # Timestamp
DataTypes.DATE()            # Date
DataTypes.ARRAY(DataTypes.STRING())  # Array of strings
DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())  # Map/dictionary
```

### Multiple Parameters:

```python
@udf(result_type=DataTypes.STRING())
def combine_fields(field1, field2, separator):
    """Combine multiple fields with separator"""
    if field1 is None or field2 is None:
        return None
    sep = separator if separator else " "
    return f"{field1}{sep}{field2}"
```

## 🔧 Best Practices

### 1. Always Handle NULL Values

```python
@udf(result_type=DataTypes.STRING())
def safe_function(text):
    if text is None:  # ✅ Always check for None
        return None
    return text.upper()
```

### 2. Add Docstrings

```python
@udf(result_type=DataTypes.INT())
def count_vowels(text):
    """
    Count the number of vowels in text.

    Args:
        text: Input string

    Returns:
        Number of vowels (a, e, i, o, u)
    """
    if text is None:
        return 0
    return sum(1 for char in text.lower() if char in 'aeiou')
```

### 3. Use Type Conversion

```python
@udf(result_type=DataTypes.DOUBLE())
def safe_divide(numerator, denominator):
    """Safely divide two numbers"""
    if numerator is None or denominator is None:
        return 0.0
    denom = float(denominator)
    if denom == 0:
        return 0.0
    return float(numerator) / denom
```

### 4. Import Libraries Inside Functions (for complex imports)

```python
@udf(result_type=DataTypes.STRING())
def parse_date(date_str):
    """Parse date string to ISO format"""
    if date_str is None:
        return None
    from datetime import datetime
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.isoformat()
    except:
        return None
```

## 🚀 Advanced Examples

### Using Regular Expressions

```python
@udf(result_type=DataTypes.STRING())
def extract_email(text):
    """Extract first email address from text"""
    if text is None:
        return None
    import re
    match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    return match.group(0) if match else None
```

### Text Analysis

```python
@udf(result_type=DataTypes.DOUBLE())
def calculate_readability(text):
    """Calculate simple readability score (avg word length)"""
    if text is None:
        return 0.0
    words = text.split()
    if not words:
        return 0.0
    total_chars = sum(len(word) for word in words)
    return total_chars / len(words)
```

### Conditional Logic

```python
@udf(result_type=DataTypes.STRING())
def categorize_length(text):
    """Categorize text by length"""
    if text is None:
        return "EMPTY"
    length = len(text)
    if length < 50:
        return "SHORT"
    elif length < 200:
        return "MEDIUM"
    elif length < 500:
        return "LONG"
    else:
        return "VERY_LONG"
```

## 🎓 Learning Path

### Beginner: Start with these

1. String manipulation (uppercase, lowercase, trim)
2. Simple calculations (add, multiply, percentage)
3. Boolean checks (contains, starts_with, is_empty)

### Intermediate: Try these

1. Text parsing (extract words, sentences)
2. Data validation (email, phone, URL)
3. Array operations (split, join, filter)

### Advanced: Master these

1. JSON/XML parsing
2. Regular expressions
3. Statistical calculations
4. Custom algorithms

## 📋 Testing Your UDFs

### Test in SQL Terminal

```sql
-- Create test table
CREATE TABLE test_data (text STRING) WITH ('connector' = 'datagen');

-- Register your UDF
CREATE TEMPORARY SYSTEM FUNCTION my_function AS 'udfs_example.my_function' LANGUAGE PYTHON;

-- Test it
SELECT my_function(text) FROM test_data LIMIT 10;
```

### Test with Sample Data

```sql
-- Create table with specific values
CREATE TABLE test_values (
    id INT,
    text STRING
) WITH (
    'connector' = 'values',
    'data-id' = '1,Hello World',
    'data-id' = '2,Test Data'
);

SELECT id, my_function(text) FROM test_values;
```

## 🔄 Workflow

1. **Edit** `udfs_example.py` - Add your function
2. **Save** - Changes are immediately available
3. **Register** in SQL - `CREATE TEMPORARY SYSTEM FUNCTION ...`
4. **Test** - Run queries to verify
5. **Iterate** - Modify and test again (no restart needed!)

## 💡 Tips

- **Start simple**: Test with basic logic first
- **Handle errors**: Use try/except for complex operations
- **Test incrementally**: Add one feature at a time
- **Use print for debugging**: Output goes to Flink logs
- **Check Flink logs**: `docker logs flink-taskmanager -f`

## 📖 Resources

- PyFlink UDF Documentation: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/python/table/udfs/python_udfs/
- Python Standard Library: https://docs.python.org/3/library/
- Regular Expressions: https://docs.python.org/3/library/re.html

---

**Happy UDF Development!** 🎉
