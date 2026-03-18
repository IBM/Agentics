# Quick Start: Python UDFs in Browser SQL Terminal

## Step 1: Install UDFs (One-time setup)

Run this command in your terminal (outside the SQL client):

```bash
cd tools/agstream_manager/scripts
./install_udfs.sh
```

Or manually:
```bash
docker cp tools/agstream_manager/udfs/udfs_example.py flink-jobmanager:/opt/flink/udfs_example.py
```

## Step 2: Register UDFs in Browser SQL Terminal

Copy and paste these commands into the PyFlink SQL terminal in your browser:

```sql
CREATE TEMPORARY SYSTEM FUNCTION add_prefix
AS 'udfs_example.add_prefix'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION uppercase_text
AS 'udfs_example.uppercase_text'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION format_with_confidence
AS 'udfs_example.format_with_confidence'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION word_count
AS 'udfs_example.word_count'
LANGUAGE PYTHON;

CREATE TEMPORARY SYSTEM FUNCTION normalize_score
AS 'udfs_example.normalize_score'
LANGUAGE PYTHON;
```

## Step 3: Verify UDFs are Registered

```sql
SHOW FUNCTIONS;
```

You should now see your custom functions in the list!

## Step 4: Use UDFs in Queries

```sql
-- Add prefix to text
SELECT add_prefix(text) as formatted_text FROM Q3;

-- Convert to uppercase
SELECT uppercase_text(category) FROM Q3;

-- Count words
SELECT text, word_count(text) as num_words FROM Q3;

-- Format with confidence (if you have confidence column)
SELECT format_with_confidence(text, 0.95) FROM Q3;

-- Combine multiple UDFs
SELECT
    add_prefix(text) as question,
    uppercase_text(category) as category,
    word_count(text) as words
FROM Q3;
```

## Troubleshooting

### "NoResourceAvailableException"
This means Flink TaskManager isn't running. Fix it:

```bash
# Check if TaskManager is running
docker ps | grep flink-taskmanager

# If not, restart Flink
cd tools/agstream_manager/scripts
./restart_flink.sh
```

### "No such file or directory" when using UDF
The UDF file isn't in the container. Run:

```bash
cd tools/agstream_manager/scripts
./install_udfs.sh
```

### UDF not in SHOW FUNCTIONS list
You need to register it in the current session. Copy the CREATE TEMPORARY SYSTEM FUNCTION commands above.

## Available UDFs

| Function | Input | Output | Example |
|----------|-------|--------|---------|
| `add_prefix` | STRING | STRING | `add_prefix('Hello')` → `'Q: Hello'` |
| `uppercase_text` | STRING | STRING | `uppercase_text('hello')` → `'HELLO'` |
| `word_count` | STRING | INT | `word_count('Hello world')` → `2` |
| `format_with_confidence` | STRING, DOUBLE | STRING | `format_with_confidence('Answer', 0.95)` → `'[HIGH] Answer'` |
| `normalize_score` | DOUBLE | DOUBLE | `normalize_score(1.5)` → `1.0` |

## Tips

1. **UDFs are session-specific** - You need to register them each time you open the SQL terminal
2. **Use LIMIT for testing** - `SELECT * FROM Q3 LIMIT 10;`
3. **Check table schema** - `DESCRIBE Q3;`
4. **Built-in functions are faster** - Use them when possible (see SHOW FUNCTIONS)
