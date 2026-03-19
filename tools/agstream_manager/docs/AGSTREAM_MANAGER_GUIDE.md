# AGStream Manager Complete Guide

## Table of Contents
1. [Overview](#overview)
2. [Installation & Setup](#installation--setup)
3. [Resource Management](#resource-management)
4. [UDF Development](#udf-development)
5. [Sentiment Analysis](#sentiment-analysis)
6. [Troubleshooting](#troubleshooting)
7. [Performance Optimization](#performance-optimization)

---

## Overview

AGStream Manager provides automatic installation and management of Agentics and Python UDFs in Flink containers, enabling seamless stream processing with LLM-powered transformations.

### Key Features
- **Auto-Installation**: Automatic setup of Agentics package and UDFs on startup
- **Smart Caching**: Avoids unnecessary reinstallations
- **Resource Management**: Automatic cleanup of Flink resources
- **Row-Level Processing**: Analyze entire rows with full context
- **Multiple UDFs**: Sentiment analysis, categorization, summarization, and more

---

## Installation & Setup

### Quick Start

1. **Configure Environment**
   ```bash
   cd tools/agstream_manager
   cp .env.example .env
   # Edit .env with your API keys
   ```

2. **Start Services**
   ```bash
   ./manage_services_full.sh start
   ```

3. **Access Flink SQL**
   - Web UI: http://localhost:5003
   - Click "Flink SQL" tab → "▶️ Start Terminal"

### Configuration (.env)

```bash
# Auto-installation
AUTO_INSTALL_ON_STARTUP=true

# UDF directory (relative or absolute path)
UDF_FOLDER=udfs

# Container names (usually don't need to change)
FLINK_JOBMANAGER_CONTAINER=flink-jobmanager
FLINK_TASKMANAGER_CONTAINER=flink-taskmanager

# LLM Configuration
SELECTED_LLM=watsonx
WATSONX_APIKEY=your_key_here
# or
OPENAI_API_KEY=your_key_here
```

### Startup Process

When you run `./manage_services_full.sh start`:

1. Docker Compose starts Kafka, Flink, and other services
2. 20-second wait for services to initialize
3. Auto-installation phase (if enabled):
   - Builds Agentics wheel package
   - Installs Agentics in JobManager and TaskManager
   - Copies all UDF files to containers
   - Copies `.env` file for API keys
4. Services ready for use

### UDF Directory Structure

```
agstream_manager/
├── .env                          # Configuration
├── udfs/                         # Default UDF directory
│   ├── semantic_operators.py    # Main UDFs
│   └── custom_udfs.py           # Your custom UDFs
├── scripts/
│   ├── install_agentics_in_flink.sh
│   └── install_udfs.sh
└── manage_services_full.sh
```

### Custom UDF Locations

```bash
# Relative paths (from agstream_manager directory)
UDF_FOLDER=udfs                           # Default
UDF_FOLDER=my_custom_udfs                 # Custom in agstream_manager
UDF_FOLDER=../../../my_project/flink_udfs # Parent project

# Absolute paths
UDF_FOLDER=/Users/username/my_udfs
UDF_FOLDER=/opt/company/shared_udfs
```

---

## Resource Management

### Smart Installation

The system automatically checks if Agentics is installed before reinstalling:

```bash
# Smart restart (checks first, skips if installed)
./manage_services_full.sh restart

# Force reinstall if needed
cd scripts && ./install_agentics_in_flink.sh --force
```

**Benefits:**
- Avoids 5-10 minute reinstallation on every restart
- Only reinstalls when necessary
- UDFs always reinstalled (lightweight, <1 second)

### Installation Behavior

| Scenario | Agentics | UDFs | .env |
|----------|----------|------|------|
| Container recreation | ✅ Installed | ✅ Installed | ✅ Copied |
| Container restart | 🔍 Checked | ✅ Installed | ✅ Copied |
| Manual install | ✅ Installed | ✅ Installed | ✅ Copied |

### Commands Summary

| Command | Flink Cleanup | Agentics Install | Use Case |
|---------|---------------|------------------|----------|
| `start` | No | Smart check | Initial startup |
| `restart` | **Yes** | Smart check | Regular restart |
| `clean-restart` | Yes | Full install | Clear all data |
| `clean-flink` | **Yes** | No | Clean resources only |

### Manual Installation

```bash
# Install everything
cd scripts
./install_agentics_in_flink.sh  # Install Agentics package
./install_udfs.sh                # Install UDFs

# Install only UDFs (faster, when only UDF code changed)
cd scripts
./install_udfs.sh

# Copy environment variables
./copy_env_to_flink.sh
```

### Avoiding Reinstallation

#### Option 1: Disable Auto-Installation (Fastest)

```bash
# In .env
AUTO_INSTALL_ON_STARTUP=false
```

Then manually install once:
```bash
cd tools/agstream_manager
./scripts/install_agentics_in_flink.sh
./scripts/install_udfs.sh
```

**Pros:** Very fast restarts, no reinstallation  
**Cons:** Must manually reinstall after code changes

#### Option 2: Use Docker Volumes (Recommended for Production)

Modify `docker-compose-karapace-flink.yml`:

```yaml
services:
  flink-jobmanager:
    volumes:
      - flink-python-packages:/usr/local/lib/python3.10/dist-packages
      # ... other volumes

  flink-taskmanager:
    volumes:
      - flink-python-packages:/usr/local/lib/python3.10/dist-packages
      # ... other volumes

volumes:
  flink-python-packages:
    driver: local
```

**Pros:** Packages persist across restarts, auto-installation still works  
**Cons:** Requires docker-compose modification

### Resource Cleanup

Automatic cleanup on restart:
- All running Flink jobs cancelled
- Checkpoints cleared
- Savepoints cleared
- Temporary files removed

Manual cleanup:
```bash
./manage_services_full.sh clean-flink
```

Use when:
- Flink is running out of resources
- Jobs are stuck or not responding
- You want to clear state without full restart

---

## UDF Development

### Basic UDF Template

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

@udf(result_type=DataTypes.STRING())
def my_simple_udf(text):
    """Simple text transformation"""
    return text.upper()
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
class SentimentResult(BaseModel):
    sentiment: Optional[str] = Field(None, description="Sentiment: POSITIVE, NEGATIVE, or NEUTRAL")
    confidence: Optional[float] = Field(None, description="Confidence score 0-1")

# Helper to run async code in sync context
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# Cache AG instance for efficiency
_ag_sentiment = None
def get_ag_sentiment():
    global _ag_sentiment
    if _ag_sentiment is None:
        _ag_sentiment = AG(atype=SentimentResult)
    return _ag_sentiment

@udf(result_type=DataTypes.STRING())
def analyze_sentiment(text):
    """Analyze sentiment using Agentics AG class"""
    if text is None:
        return None
    
    try:
        ag = get_ag_sentiment()
        result = run_async(ag << text)
        
        if isinstance(result, list) and len(result) > 0:
            return result[0].sentiment
        elif hasattr(result, 'sentiment'):
            return result.sentiment
        
        return "NEUTRAL"
    except Exception as e:
        return f"ERROR: {str(e)[:50]}"
```

### Best Practices

1. **Cache AG instances**: Create once, reuse across calls
2. **Handle async properly**: Use `run_async()` helper for AG operations
3. **Error handling**: Always wrap in try-except
4. **Load environment**: Use `load_dotenv()` for API keys
5. **Type hints**: Use proper Pydantic models
6. **Return types**: Match PyFlink DataTypes

### Using UDFs in Flink SQL

#### 1. Register UDF

```sql
CREATE TEMPORARY SYSTEM FUNCTION my_function 
AS 'my_udfs.my_function' 
LANGUAGE PYTHON;
```

Format: `'filename_without_py.function_name'`

#### 2. Use in Queries

```sql
SELECT 
    text,
    my_function(text) as result
FROM my_topic
LIMIT 10;
```

---

## Sentiment Analysis

### Available Functions

#### Single-Field Sentiment Analysis

```sql
-- Register functions
CREATE TEMPORARY SYSTEM FUNCTION analyze_sentiment
AS 'semantic_operators.analyze_sentiment'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION extract_sentiment_label
AS 'semantic_operators.extract_sentiment_label'
LANGUAGE PYTHON;

-- Use in query
SELECT 
    text,
    extract_sentiment_label(analyze_sentiment(text)) as sentiment
FROM messages;
```

#### Row-Level Sentiment Analysis

Analyzes sentiment considering all fields in the row as context.

```sql
-- Register function
CREATE TEMPORARY SYSTEM FUNCTION analyze_row_sentiment
AS 'semantic_operators.analyze_row_sentiment'
LANGUAGE PYTHON;

-- Analyze entire row
SELECT 
    id,
    title,
    analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment_json,
    extract_sentiment_label(
        analyze_row_sentiment(CAST(ROW(*) AS STRING))
    ) as sentiment_label
FROM messages;
```

### Quick Test

```sql
-- Test with sample data
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

**Expected Output:**
```
                  text                      sentiment
    This product is terrible                NEGATIVE
    I love this! It is amazing!             POSITIVE
    Who is maradona?                        NEUTRAL
```

### Row-Level Analysis Examples

#### Example 1: Multi-Field Context

```sql
SELECT 
    id,
    analyze_row_sentiment(
        CAST(ROW(title, content, author, timestamp) AS STRING)
    ) as sentiment
FROM posts;
```

#### Example 2: Filter by Sentiment

```sql
SELECT *
FROM (
    SELECT 
        id,
        title,
        content,
        analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment_json
    FROM messages
)
WHERE extract_sentiment_label(sentiment_json) = 'POSITIVE';
```

#### Example 3: Using Views

```sql
-- Create view with JSON representation
CREATE VIEW messages_with_json AS
SELECT 
    *,
    CAST(ROW(*) AS STRING) as row_json
FROM messages;

-- Use the view
SELECT 
    id,
    title,
    analyze_row_sentiment(row_json) as sentiment
FROM messages_with_json;
```

### Comparison: Single-Field vs Row-Level

| Feature | `analyze_sentiment` | `analyze_row_sentiment` |
|---------|-------------------|------------------------|
| Input | Single text field | Entire row (JSON) |
| Context | Only the text | All fields in row |
| Use Case | Simple text analysis | Complex multi-field analysis |
| Performance | Faster | Slower (more context) |
| Token Usage | Lower | Higher |

### Output Format

```json
{
  "label": "POSITIVE",
  "confidence": 0.92,
  "reasoning": "The title expresses enthusiasm and the content describes successful outcomes."
}
```

Error format:
```json
{
  "label": "ERROR",
  "confidence": 0.0,
  "reasoning": "Error: [error message]"
}
```

---

## Troubleshooting

### UDFs Not Found After Restart

**Problem:** UDFs work initially but disappear after Flink restart.

**Solution:**
- Check `AUTO_INSTALL_ON_STARTUP=true` in `.env`
- Or manually run: `cd scripts && ./install_udfs.sh`

### Import Errors in UDFs

**Problem:** `ModuleNotFoundError: No module named 'agentics'`

**Solution:**
```bash
cd scripts
./install_agentics_in_flink.sh
```

### Sentiment Returns NEUTRAL for Everything

**Problem:** All sentiment analysis returns NEUTRAL regardless of input.

**Solution:**

1. **Check environment variables are loaded:**
   ```bash
   docker logs flink-taskmanager 2>&1 | grep "Loaded .env"
   ```
   Should see: `✓ Loaded .env from /opt/flink/.env`

2. **Verify .env was copied:**
   ```bash
   docker exec flink-taskmanager cat /opt/flink/.env | grep SELECTED_LLM
   ```

3. **Restart Flink SQL session:**
   - Stop current terminal (⏹️ Stop Terminal)
   - Start new terminal (▶️ Start Terminal)
   - Re-register UDFs

4. **Check API keys:**
   ```bash
   docker exec flink-taskmanager bash -c "source /opt/flink/.env && echo \$WATSONX_APIKEY"
   ```

### LLM API Errors in UDFs

**Problem:** `Provided llm object must be...` error

**Solution:**
1. Copy `.env` file to Flink containers:
   ```bash
   docker cp .env flink-taskmanager:/opt/flink/.env
   docker cp .env flink-jobmanager:/opt/flink/.env
   ```
2. Ensure UDF loads it: `load_dotenv('/opt/flink/.env')`
3. Restart Flink SQL session

### Flink Resource Issues

**Problem:** TaskManager crashes with "No resources available"

**Solution:**
```bash
./manage_services_full.sh clean-flink
# or
./manage_services_full.sh restart
```

### Permission Errors

**Problem:** `Permission denied` when installing packages

**Solution:** Install scripts run as root automatically. If issues persist:
```bash
docker exec -u root flink-taskmanager bash
# Then manually install
```

### UDF Directory Not Found

**Problem:** `Error: UDF directory not found at /path/to/udfs`

**Solution:** Check `UDF_FOLDER` setting in `.env` and ensure directory exists.

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

## Performance Optimization

### Installation Time

| Operation | Time | Impact |
|-----------|------|--------|
| Smart check (installed) | <1s | Minimal |
| Smart check (not installed) | 5-10min | One-time |
| UDF installation | <1s | Minimal |
| Flink cleanup | 2-5s | Minimal |
| Full Agentics reinstall | 5-10min | Significant |

### Runtime Performance

**AG Instance Caching:**
```python
# Cache AG instance globally
_ag_sentiment = None
def get_ag_sentiment():
    global _ag_sentiment
    if _ag_sentiment is None:
        _ag_sentiment = AG(atype=SentimentResult)
    return _ag_sentiment
```

**Parallelization:**
```sql
-- Set parallelism level
SET 'parallelism.default' = '8';

-- Flink processes 8 rows in parallel
SELECT analyze_sentiment(text) FROM messages;
```

**Filtering Before Analysis:**
```sql
SELECT analyze_sentiment(text)
FROM messages
WHERE content IS NOT NULL AND LENGTH(content) > 10;
```

### Optimization Tips

1. **Batch processing**: Process multiple rows together when possible
2. **Cache results**: Store frequently used results
3. **Async batching**: Group multiple LLM calls
4. **Resource allocation**: Increase TaskManager memory for LLM operations
5. **Field selection**: Only include relevant fields in row-level analysis
6. **Use Flink parallelism**: Don't implement asyncio batching within UDF

### Performance Considerations

- **Token Usage**: More fields = more tokens = higher cost
- **Latency**: Row-level analysis slower than single-field
- **LLM latency**: Typically 1-5 seconds per call
- **Parallelization**: Flink handles this automatically across task slots

---

## Additional Resources

### Available UDFs

The `semantic_operators.py` file includes:
- `analyze_sentiment(text)` - Full sentiment analysis (returns JSON)
- `analyze_row_sentiment(row_json)` - Row-level sentiment analysis
- `extract_sentiment_label(json)` - Extract label from sentiment JSON
- `extract_confidence(json)` - Extract confidence score
- `categorize_text(text)` - Categorize text into topics
- `summarize_text(text)` - Generate summaries
- `extract_entities(text)` - Named entity recognition
- `answer_question(question)` - Question answering

### Related Documentation

- [AGStream Manager README](../README.md)
- [Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/udfs/python_udfs/)
- [Agentics Documentation](../../../README.md)

### Example Files

- `udfs/semantic_operators.py` - Complete UDF implementations
- `udfs/test_all_udfs.py` - Comprehensive UDF tests
- `scripts/install_udfs.sh` - UDF installation script
- `scripts/install_agentics_in_flink.sh` - Agentics installation script

---

## Quick Reference

### Essential Commands

```bash
# Start services
./manage_services_full.sh start

# Restart (smart, with cleanup)
./manage_services_full.sh restart

# Clean restart (full reinstall)
./manage_services_full.sh clean-restart

# Clean Flink resources only
./manage_services_full.sh clean-flink

# Stop services
./manage_services_full.sh stop

# Open Flink SQL
./manage_services_full.sh flink-sql

# Manual UDF install
cd scripts && ./install_udfs.sh

# Manual Agentics install
cd scripts && ./install_agentics_in_flink.sh
```

### Essential SQL

```sql
-- Register sentiment UDFs
CREATE TEMPORARY SYSTEM FUNCTION analyze_sentiment
AS 'semantic_operators.analyze_sentiment' LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION extract_sentiment_label
AS 'semantic_operators.extract_sentiment_label' LANGUAGE PYTHON;

-- Simple sentiment analysis
SELECT text, extract_sentiment_label(analyze_sentiment(text)) as sentiment
FROM messages;

-- Row-level sentiment analysis
SELECT id, analyze_row_sentiment(CAST(ROW(*) AS STRING)) as sentiment
FROM messages;
```

---

**Last Updated:** 2026-03-19  
**Version:** 1.0
