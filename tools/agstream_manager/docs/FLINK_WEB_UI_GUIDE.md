# Using Flink Web UI for SQL Jobs

The Flink Web UI (http://localhost:8085) provides a visual interface for monitoring jobs, but it doesn't have a built-in SQL editor. Here's how to test your sentiment UDF using the browser UI.

## Option 1: Submit SQL Job via Command Line (View in Browser)

### Step 1: Create SQL Job File

Create a file `test_sentiment.sql`:

```sql
-- Create table Q from Kafka topic
CREATE TABLE Q (
    text STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'Q',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'test-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Register the UDF
CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment 
AS 'udfs_example.generate_sentiment' 
LANGUAGE PYTHON;

-- Create output table
CREATE TABLE sentiment_results (
    text STRING,
    sentiment STRING
) WITH (
    'connector' = 'print'
);

-- Run continuous query
INSERT INTO sentiment_results
SELECT text, generate_sentiment(text) as sentiment 
FROM Q;
```

### Step 2: Submit Job

```bash
cd ../agentics911/agentics/tools/agstream_manager
docker exec flink-jobmanager ./bin/sql-client.sh -f /path/to/test_sentiment.sql
```

### Step 3: View in Browser

1. Open http://localhost:8085
2. Click "Running Jobs" to see your job
3. Click on the job to see details
4. View TaskManager logs for output

## Option 2: Use Flink SQL Gateway (Recommended)

Flink SQL Gateway provides a REST API that can be used with web UIs.

### Setup SQL Gateway

1. **Start SQL Gateway** (if not already running):

```bash
docker exec -d flink-jobmanager ./bin/sql-gateway.sh start
```

2. **Access via REST API**:

```bash
# Create session
curl -X POST http://localhost:8083/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{}'

# Execute SQL (use session_handle from above)
curl -X POST http://localhost:8083/v1/sessions/{session_handle}/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "CREATE TABLE Q (text STRING) WITH (...)"
  }'
```

## Option 3: Use Flink Web UI with Pre-registered Tables

If you've already created tables in the SQL client, you can:

1. **Open Flink Web UI**: http://localhost:8085
2. **Go to "Running Jobs"** to see active queries
3. **Click on a job** to see:
   - Job graph
   - TaskManager logs
   - Metrics
   - Checkpoints

## Option 4: Interactive Testing (Easiest)

Use the command-line SQL client but monitor in browser:

### Terminal 1: Run SQL Client

```bash
cd ../agentics911/agentics/tools/agstream_manager
./manage_services_full.sh flink-sql
```

Then execute your SQL commands.

### Terminal 2: View Logs

```bash
docker logs -f flink-taskmanager 2>&1 | grep -E "(DEBUG|sentiment)"
```

### Browser: Monitor Jobs

1. Open http://localhost:8085
2. Watch jobs appear in "Running Jobs"
3. Click on jobs to see execution details

## Viewing Results

### Method 1: Print Connector (Console Output)

Results appear in TaskManager logs:

```bash
docker logs flink-taskmanager 2>&1 | tail -50
```

### Method 2: Kafka Output Topic

Create an output table that writes to Kafka:

```sql
CREATE TABLE sentiment_output (
    text STRING,
    sentiment STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sentiment-results',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

INSERT INTO sentiment_output
SELECT text, generate_sentiment(text) as sentiment 
FROM Q;
```

Then view in Kafka UI: http://localhost:8080

### Method 3: File Sink

Write results to a file:

```sql
CREATE TABLE sentiment_file (
    text STRING,
    sentiment STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/sentiment-results',
    'format' = 'json'
);

INSERT INTO sentiment_file
SELECT text, generate_sentiment(text) as sentiment 
FROM Q;
```

View file in container:

```bash
docker exec flink-taskmanager cat /tmp/sentiment-results/*
```

## Complete Example Workflow

### 1. Start Services

```bash
cd ../agentics911/agentics/tools/agstream_manager
./manage_services_full.sh start
```

### 2. Open Flink SQL Client

```bash
./manage_services_full.sh flink-sql
```

### 3. Create Tables and UDF

```sql
-- Input table
CREATE TABLE Q (
    text STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'Q',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'test-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Register UDF
CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment 
AS 'udfs_example.generate_sentiment' 
LANGUAGE PYTHON;

-- Output to Kafka
CREATE TABLE sentiment_output (
    text STRING,
    sentiment STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sentiment-results',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- Start streaming job
INSERT INTO sentiment_output
SELECT text, generate_sentiment(text) as sentiment 
FROM Q;
```

### 4. Monitor in Browser

1. **Flink Web UI**: http://localhost:8085
   - View running jobs
   - Check TaskManager logs
   - Monitor metrics

2. **Kafka UI**: http://localhost:8080
   - View input topic `Q`
   - View output topic `sentiment-results`

### 5. View Debug Logs

```bash
# In another terminal
docker logs -f flink-taskmanager 2>&1 | grep DEBUG
```

## Troubleshooting

### Job Not Appearing in Web UI

- Check if SQL client is still running
- Verify job was submitted successfully
- Look for errors in logs

### No Output in Kafka Topic

- Check if input topic has data
- Verify UDF is registered correctly
- Check TaskManager logs for errors

### UDF Errors

View detailed logs:

```bash
docker logs flink-taskmanager 2>&1 | grep -A 10 "ERROR"
```

## Useful Links

- **Flink Web UI**: http://localhost:8085
- **Kafka UI**: http://localhost:8080
- **Schema Registry UI**: http://localhost:8000
- **AGStream Manager**: http://localhost:5003

## Tips

1. **Use Kafka output** for easier result viewing in browser
2. **Monitor TaskManager logs** for debug output
3. **Check Flink Web UI** for job status and metrics
4. **Use Kafka UI** to verify data flow
5. **Keep SQL client open** to see query results directly
