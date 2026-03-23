# Troubleshooting Guide

## Common Errors and Solutions

### 1. Kafka Timeout Error

**Error:**
```
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment
```

**Cause**: Flink cannot connect to Kafka

**Solutions:**

#### Check if Kafka is running
```bash
docker ps | grep kafka
```

Should show:
- `kafka` container
- `karapace-schema-registry` container

#### Start services if not running
```bash
cd tools/agstream_manager
./manage_services_full.sh start
```

#### Verify Kafka connectivity
```bash
# From host
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# From Flink container
docker exec flink-jobmanager curl -s http://kafka:29092
```

#### Check table definition
```sql
-- In Flink SQL, show table definition
SHOW CREATE TABLE pr;
```

Verify the bootstrap servers match your setup:
- From host: `localhost:9092`
- From Flink container: `kafka:29092` or `kafka:9092`

#### Recreate the table with correct settings
```sql
-- Drop existing table
DROP TABLE IF EXISTS pr;

-- Recreate with correct Kafka settings
CREATE TABLE pr (
    customer_review STRING,
    product_id STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'product-reviews',
    'properties.bootstrap.servers' = 'kafka:29092',  -- Use kafka:29092 from Flink
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);
```

### 2. UDF Not Found

**Error:**
```
Function 'agmap_table_dynamic' not found
```

**Solutions:**

#### Register the UDF
```bash
cd tools/agstream_manager
./scripts/install_agmap_row.sh
```

#### Or manually in SQL
```sql
CREATE TEMPORARY SYSTEM FUNCTION agmap_table_dynamic
AS 'agmap_row.agmap_table_dynamic' LANGUAGE PYTHON;
```

#### Verify registration
```sql
SHOW FUNCTIONS;
```

Look for `agmap_table_dynamic` in the list.

### 3. Module Not Found

**Error:**
```
ModuleNotFoundError: No module named 'agentics'
```

**Solutions:**

#### Rebuild Flink image
```bash
cd tools/agstream_manager
./scripts/rebuild_flink_image.sh
./manage_services_full.sh restart
```

#### Verify Agentics is installed
```bash
docker exec flink-jobmanager pip list | grep agentics
```

### 4. No API Key Error

**Error:**
```
No API key found. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY
```

**Solutions:**

#### Check .env file
```bash
cat tools/agstream_manager/.env | grep API_KEY
```

#### Add API key if missing
```bash
echo "OPENAI_API_KEY=your_key_here" >> tools/agstream_manager/.env
```

#### Copy to Flink container
```bash
docker cp tools/agstream_manager/.env flink-jobmanager:/opt/flink/.env
```

#### Restart Flink
```bash
cd tools/agstream_manager
./manage_services_full.sh restart
```

### 5. Schema Registry Error

**Error:**
```
Schema not found in registry
```

**Solutions:**

#### Check Schema Registry is running
```bash
docker ps | grep karapace-schema-registry
```

#### Test Schema Registry
```bash
curl http://localhost:8081/subjects
```

#### Verify schema exists
```bash
curl http://localhost:8081/subjects/YourType-value/versions/latest
```

### 6. Table Not Found

**Error:**
```
Table 'pr' not found
```

**Solutions:**

#### List existing tables
```sql
SHOW TABLES;
```

#### Create the table
```sql
CREATE TABLE pr (
    customer_review STRING,
    product_id STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'product-reviews',
    'properties.bootstrap.servers' = 'kafka:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://karapace-schema-registry:8081'
);
```

#### Verify topic exists in Kafka
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep product-reviews
```

## Diagnostic Commands

### Check All Services
```bash
cd tools/agstream_manager
docker-compose ps
```

### View Flink Logs
```bash
docker logs flink-jobmanager -f
```

### View Kafka Logs
```bash
docker logs kafka -f
```

### Check Flink UI
```
http://localhost:8081
```

### Check Schema Registry
```bash
curl http://localhost:8081/subjects
```

### List Kafka Topics
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Test UDF File Exists
```bash
docker exec flink-jobmanager ls -la /opt/flink/udfs/agmap_row.py
```

## Complete Reset

If nothing works, try a complete reset:

```bash
cd tools/agstream_manager

# Stop everything
./manage_services_full.sh down

# Clean Docker
docker system prune -f

# Rebuild image
./scripts/rebuild_flink_image.sh --force

# Start services
./manage_services_full.sh up

# Wait for services to be ready (30 seconds)
sleep 30

# Install UDFs
./scripts/install_agmap_row.sh

# Test
./scripts/sql_shell.sh
```

## Quick Health Check

Run this script to check all components:

```bash
#!/bin/bash
echo "=== Health Check ==="
echo ""

echo "1. Docker containers:"
docker ps --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "2. Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "❌ Kafka not accessible"
echo ""

echo "3. Schema Registry:"
curl -s http://localhost:8081/subjects | jq . 2>/dev/null || echo "❌ Schema Registry not accessible"
echo ""

echo "4. Flink UI:"
curl -s http://localhost:8081 >/dev/null && echo "✅ Flink UI accessible" || echo "❌ Flink UI not accessible"
echo ""

echo "5. UDF file:"
docker exec flink-jobmanager test -f /opt/flink/udfs/agmap_row.py && echo "✅ UDF file exists" || echo "❌ UDF file missing"
echo ""

echo "6. Agentics package:"
docker exec flink-jobmanager pip show agentics >/dev/null 2>&1 && echo "✅ Agentics installed" || echo "❌ Agentics not installed"
```

Save as `health_check.sh` and run:
```bash
chmod +x health_check.sh
./health_check.sh
```

## Getting Help

If issues persist:

1. Check the logs: `docker logs flink-jobmanager -f`
2. Review the error message carefully
3. Try the complete reset procedure
4. Check the documentation:
   - [Quick Start](AGMAP_TABLE_DYNAMIC_QUICKSTART.md)
   - [Performance Guide](AGMAP_PERFORMANCE_GUIDE.md)
   - [Build Optimization](BUILD_OPTIMIZATION.md)

---

**Most Common Issue**: Kafka connection timeout → Check services are running and table uses correct bootstrap servers
