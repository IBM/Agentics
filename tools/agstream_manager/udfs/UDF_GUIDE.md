# Flink SQL UDF Guide

Complete guide for using Python UDFs (User-Defined Functions) with Agentics in Flink SQL.

## Table of Contents
1. [Quick Start](#quick-start)
2. [Installation](#installation)
3. [Creating Custom UDFs](#creating-custom-udfs)
4. [Testing UDFs](#testing-udfs)
5. [Available UDFs](#available-udfs)
6. [Advanced Features](#advanced-features)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### 1. Install UDFs (One-time setup)

```bash
cd tools/agstream_manager
./scripts/install_udfs.sh
```

### 2. Open Flink SQL Terminal

**Option A: Via Web UI**
- Open browser to `http://localhost:8081`
- Click "SQL Client" in the left sidebar

**Option B: Via AGStream Manager UI**
- Go to `http://localhost:5003`
- Click "Open Flink SQL Terminal"

**Option C: Via Command Line**
```bash
./manage_services_full.sh flink-sql
```

### 3. Register UDFs

```sql
-- Sentiment analysis
CREATE TEMPORARY SYSTEM FUNCTION analyze_sentiment
AS 'semantic_operators.analyze_sentiment'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION extract_sentiment_label
AS 'semantic_operators.extract_sentiment_label'
LANGUAGE PYTHON;

-- Row-level sentiment
CREATE TEMPORARY SYSTEM FUNCTION analyze_row_sentiment
AS 'semantic_operators.analyze_row_sentiment'
LANGUAGE PYTHON;
```

### 4. Use UDFs

```sql
-- Simple sentiment analysis
SELECT
    text,
    extract_sentiment_label(analyze_sentiment(text)) as sentiment
FROM messages
LIMIT 10;

-- Row-level sentiment (considers all fields)
SELECT
    id,
    extract_sentiment_label(
        analyze_row_sentiment(CAST(ROW(*) AS STRING))
    ) as sentiment
FROM messages
LIMIT 10;
```

---

## Installation

### Prerequisites

1. **Start Services**
   ```bash
   cd tools/agstream_manager
   ./manage_services_full.sh start
   ```

2. **Copy Environment Variables (CRITICAL - Contains API keys)**
   ```bash
   ./scripts/copy_env_to_flink.sh
   ```

3. **Verify .env is loaded**
   ```bash
   docker exec flink-taskmanager cat /opt/flink/.env | grep SELECTED_LLM
   ```

### Auto-Installation

When `AUTO_INSTALL_ON_STARTUP=true` in `.env`, UDFs are automatically installed on startup.

### Manual Installation

```bash
cd tools/agstream_manager

# Install Agentics package
./scripts/install_agentics_in_flink.sh

# Install UDFs
./scripts/install_udfs.sh
```

---

## Creating Custom UDFs

### Basic UDF Template

Create a Python file in `tools/agstream_manager/udfs/`:

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

### UDF with Agentics

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf
from pydantic import BaseModel, Field
from typing import Optional
from agentics import AG
import asyncio
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/opt/flink/.env')

# Define Pydantic model
class MyOutput(BaseModel):
    result: str
    confidence: float

# Helper to run async code
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# Cache AG instance
_ag_instance = None
def get_ag():
    global _ag_instance
    if _ag_instance is None:
        _ag_instance = AG(atype=MyOutput)
    return _ag_instance

@udf(result_type=DataTypes.STRING())
def my_agentics_udf(text):
    """Process text with Agentics"""
    if text is None:
        return None

    try:
        ag = get_ag()
        result = run_async(ag << text)
        return result.result if hasattr(result, 'result') else str(result)
    except Exception as e:
        return f"ERROR: {str(e)[:50]}"
```

### Best Practices

1. **Cache AG instances** - Create once, reuse across calls
2. **Handle async properly** - Use `run_async()` helper
3. **Error handling** - Always wrap in try-except
4. **Load environment** - Use `load_dotenv('/opt/flink/.env')`
5. **Type hints** - Use proper Pydantic models
6. **NULL handling** - Check for None inputs

### Deploying Custom UDFs

1. Save your UDF file in `tools/agstream_manager/udfs/`
2. Run installation script:
   ```bash
   ./scripts/install_udfs.sh
   ```
3. Register in Flink SQL:
   ```sql
   CREATE TEMPORARY SYSTEM FUNCTION my_function
   AS 'my_file.my_function'
   LANGUAGE PYTHON;
   ```

---

## Testing UDFs

### Method 1: Python Unit Test (Fastest)

Test directly in Python without Flink:

```bash
cd tools/agstream_manager/udfs
python test_row_sentiment.py
```

### Method 2: Flink SQL with Test Data

```sql
-- Test with inline data
SELECT
    text,
    extract_sentiment_label(analyze_sentiment(text)) as sentiment
FROM (
    VALUES
        ('This product is terrible'),
        ('I love this! It is amazing!'),
        ('Who is maradona?')
) AS t(text);
```

Expected output:
```
                  text                      sentiment
    This product is terrible                NEGATIVE
    I love this! It is amazing!             POSITIVE
    Who is maradona?                        NEUTRAL
```

### Method 3: Test with DataGen Connector

```sql
CREATE TABLE test_messages (
    id BIGINT,
    title STRING,
    content STRING,
    author STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10'
);

SELECT
    id,
    title,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment
FROM test_messages
LIMIT 3;
```

### Method 4: Standalone Test Script

```bash
cd tools/agstream_manager/udfs
python test_all_udfs_standalone.py
```

---

## Available UDFs

### Sentiment Analysis

#### `analyze_sentiment(text)`
Analyzes sentiment of a single text field.

```sql
SELECT analyze_sentiment(text) FROM messages;
-- Returns: {"label":"POSITIVE","confidence":0.92,"reasoning":"..."}
```

#### `analyze_row_sentiment(row_json)`
Analyzes sentiment considering all fields in the row.

```sql
SELECT analyze_row_sentiment(CAST(ROW(*) AS STRING)) FROM messages;
-- Returns: {"label":"POSITIVE","confidence":0.92,"reasoning":"..."}
```

#### `extract_sentiment_label(json)`
Extracts just the sentiment label from JSON result.

```sql
SELECT extract_sentiment_label(analyze_sentiment(text)) FROM messages;
-- Returns: "POSITIVE", "NEGATIVE", or "NEUTRAL"
```

#### `extract_confidence(json)`
Extracts confidence score from JSON result.

```sql
SELECT extract_confidence(analyze_sentiment(text)) FROM messages;
-- Returns: 0.92 (float between 0 and 1)
```

### Text Processing

#### `categorize_text(text)`
Categorizes text into topics.

```sql
SELECT categorize_text(text) FROM messages;
```

#### `summarize_text(text)`
Generates text summaries.

```sql
SELECT summarize_text(content) FROM articles;
```

#### `extract_entities(text)`
Named entity recognition.

```sql
SELECT extract_entities(text) FROM documents;
```

### Question Answering

#### `answer_question(question)`
Answers questions using LLM.

```sql
SELECT answer_question(text) FROM questions;
```

---

## Advanced Features

### Universal Transduction UDTF

A Table-Valued Function that works with any transduction by reading schemas from Schema Registry.

#### Register UDTF

```sql
CREATE TEMPORARY SYSTEM FUNCTION universal_transduce
AS 'semantic_operators.UniversalTransductionUDTF'
LANGUAGE PYTHON;
```

#### Usage

```sql
-- Automatically fetches target schema from 'sentiment-output' topic
SELECT * FROM TABLE(
    universal_transduce(
        'sentiment-output',
        CAST(ROW(text) AS STRING)
    )
) LIMIT 10;
```

**Output columns automatically match schema:**
```
label    | confidence | reasoning
---------|------------|----------------------------------
POSITIVE | 0.92       | Enthusiastic language detected
NEGATIVE | 0.88       | Critical tone throughout
NEUTRAL  | 0.75       | Balanced perspective
```

### Row-Level Processing

Process entire rows with full context:

```sql
-- Method 1: All columns
SELECT
    id,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment
FROM messages;

-- Method 2: Specific columns
SELECT
    id,
    analyze_row_sentiment(
        CAST(ROW(title, content, author) AS STRING)
    ) as sentiment
FROM messages;

-- Method 3: Using views
CREATE VIEW messages_with_json AS
SELECT
    *,
    CAST(ROW(*) AS STRING) as row_json
FROM messages;

SELECT
    id,
    analyze_row_sentiment(row_json) as sentiment
FROM messages_with_json;
```

### Parallelization

Flink automatically parallelizes UDF execution:

```sql
-- Set parallelism level
SET 'parallelism.default' = '8';

-- Flink processes 8 rows in parallel
SELECT analyze_sentiment(text) FROM messages;
```

---

## Troubleshooting

### UDFs Not Found After Restart

**Problem:** UDFs work initially but disappear after Flink restart.

**Solution:**
```bash
cd tools/agstream_manager
./scripts/install_udfs.sh
```

Or enable auto-installation in `.env`:
```bash
AUTO_INSTALL_ON_STARTUP=true
```

### Import Errors

**Problem:** `ModuleNotFoundError: No module named 'agentics'`

**Solution:**
```bash
cd tools/agstream_manager/scripts
./install_agentics_in_flink.sh
```

### Sentiment Returns NEUTRAL for Everything

**Problem:** All sentiment analysis returns NEUTRAL.

**Solution:**

1. Check environment variables:
   ```bash
   docker logs flink-taskmanager 2>&1 | grep "Loaded .env"
   ```

2. Verify .env was copied:
   ```bash
   docker exec flink-taskmanager cat /opt/flink/.env | grep SELECTED_LLM
   ```

3. Restart Flink SQL session and re-register UDFs

### NoResourceAvailableException

**Problem:** Flink TaskManager isn't running or out of resources.

**Solution:**
```bash
# Check if TaskManager is running
docker ps | grep flink-taskmanager

# Restart Flink
cd tools/agstream_manager
./manage_services_full.sh restart
```

### API Key Errors

**Problem:** `Provided llm object must be...` error

**Solution:**
1. Ensure `.env` file has valid API keys
2. Copy to Flink containers:
   ```bash
   ./scripts/copy_env_to_flink.sh
   ```
3. Restart Flink SQL session

### UDF Not in SHOW FUNCTIONS List

**Problem:** UDF not appearing in function list.

**Solution:** UDFs are session-specific. Register them in each SQL session:
```sql
CREATE TEMPORARY SYSTEM FUNCTION my_function
AS 'my_file.my_function'
LANGUAGE PYTHON;
```

### Verification Commands

```bash
# Check if Agentics is installed
docker exec flink-taskmanager python3 -c "import agentics; print('✓ Installed')"

# Check UDF file exists
docker exec flink-taskmanager ls -la /opt/flink/semantic_operators.py

# Check environment variables
docker exec flink-taskmanager cat /opt/flink/.env

# View TaskManager logs
docker logs flink-taskmanager --tail 100

# View JobManager logs
docker logs flink-jobmanager --tail 100
```

---

## After Reboot Checklist

Quick steps to get UDFs working after system reboot:

1. **Start Services**
   ```bash
   cd tools/agstream_manager
   ./manage_services_full.sh start
   ```

2. **Wait for initialization** (about 30 seconds)

3. **Open Flink SQL Terminal**

4. **Register Functions**
   ```sql
   CREATE TEMPORARY SYSTEM FUNCTION analyze_sentiment
   AS 'semantic_operators.analyze_sentiment' LANGUAGE PYTHON;

   CREATE TEMPORARY SYSTEM FUNCTION extract_sentiment_label
   AS 'semantic_operators.extract_sentiment_label' LANGUAGE PYTHON;
   ```

5. **Test**
   ```sql
   SELECT
       text,
       extract_sentiment_label(analyze_sentiment(text)) as sentiment
   FROM messages
   LIMIT 3;
   ```

---

## Quick Reference

### Essential Commands

```bash
# Install UDFs
./scripts/install_udfs.sh

# Copy environment variables
./scripts/copy_env_to_flink.sh

# Open Flink SQL
./manage_services_full.sh flink-sql

# Restart services
./manage_services_full.sh restart
```

### Essential SQL

```sql
-- Register sentiment UDF
CREATE TEMPORARY SYSTEM FUNCTION analyze_sentiment
AS 'semantic_operators.analyze_sentiment' LANGUAGE PYTHON;

-- Use sentiment UDF
SELECT extract_sentiment_label(analyze_sentiment(text)) FROM messages;

-- Show all functions
SHOW FUNCTIONS;

-- Describe table
DESCRIBE messages;
```

---

**For more information, see:**
- [AGStream Manager Guide](../docs/AGSTREAM_MANAGER_GUIDE.md)
- [Main README](../README.md)
- [Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
