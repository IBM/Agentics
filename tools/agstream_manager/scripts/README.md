# AGStream Manager Scripts

This directory contains essential scripts for managing AGStream services, Flink, and UDFs.

## Core Scripts

### Service Management

- **`manage_agstream.sh`** - Manages AGStream backend service
  - Start/stop/restart the AGStream Manager backend
  - Used by `manage_services_full.sh`

### Flink Management

- **`flink_sql.sh`** - Interactive Flink SQL client
  - Opens Flink SQL CLI for running queries
  - Usage: `./flink_sql.sh`

- **`rebuild_flink_image.sh`** - Rebuilds Flink Docker image
  - Rebuilds image with latest UDFs and dependencies
  - Required after UDF changes
  - Usage: `./rebuild_flink_image.sh`

- **`restart_flink.sh`** - Restarts Flink services
  - Quick restart without full rebuild
  - Usage: `./restart_flink.sh`

- **`clean_and_restart_flink.sh`** - Clean restart of Flink
  - Stops Flink, cleans state, and restarts
  - Usage: `./clean_and_restart_flink.sh`

- **`set_flink_parallelism.sh`** - Sets Flink parallelism
  - Configures parallel task execution
  - Usage: `./set_flink_parallelism.sh <number>`
  - Example: `./set_flink_parallelism.sh 20`

- **`set_column_width.sh`** - Sets Flink SQL column display width
  - Configures max column width for better table display
  - Usage: `./set_column_width.sh <width>`
  - Example: `./set_column_width.sh 100`
  - Useful when query results are truncated

### UDF Management

- **`install_agentics_in_flink.sh`** - Installs Agentics package in Flink
  - Copies Agentics library to Flink containers
  - Called automatically by `manage_services_full.sh`
  - Usage: `./install_agentics_in_flink.sh [--force]`

- **`install_udfs.sh`** - Installs UDFs in Flink
  - Copies UDF files to Flink containers
  - Called automatically by `manage_services_full.sh`
  - Usage: `./install_udfs.sh`

- **`generate_registry_udfs.py`** - Generates registry-based UDFs
  - Auto-generates `agmap_registry.py` and `agreduce_registry.py`
  - Scans Schema Registry for schemas
  - Usage: `python generate_registry_udfs.py`

- **`auto_register_udfs.py`** - Auto-registers UDFs in Flink SQL
  - Registers all UDFs from registry files
  - Usage: `python auto_register_udfs.py`

- **`fix_agreduce.sh`** - Fixes agreduce libgomp dependency
  - Installs libgomp1 in Flink containers
  - Run if you see libgomp ImportError
  - Usage: `./fix_agreduce.sh`

### Kafka/Topic Management

- **`kafka_query.py`** - Query Kafka topics
  - Read messages from Kafka topics
  - Usage: `python kafka_query.py <topic_name>`

- **`clean_sql_topics.py`** - Cleans SQL-related topics
  - Removes old SQL topics
  - Usage: `python clean_sql_topics.py`

- **`clean_sql_topics.sh`** - Shell wrapper for topic cleaning
  - Usage: `./clean_sql_topics.sh`

- **`reset_sql_topics.sh`** - Resets SQL topics
  - Deletes and recreates SQL topics
  - Usage: `./reset_sql_topics.sh`

- **`verify_avro_messages.py`** - Verifies Avro message format
  - Checks Avro serialization in topics
  - Usage: `python verify_avro_messages.py <topic_name>`

### Initialization

- **`init_flink_tables.py`** - Generates Flink table definitions
  - Creates SQL DDL from AGStream Manager schemas
  - Called automatically by `manage_services_full.sh`
  - Usage: `python init_flink_tables.py`

- **`download_flink_avro.sh`** - Downloads Flink Avro connectors
  - Downloads required JAR files for Avro support
  - Usage: `./download_flink_avro.sh`

### Utilities

- **`flink`** - Flink CLI wrapper
  - Wrapper for Flink command-line tools
  - Usage: `./flink <command>`

## Archived Scripts

The `archive/` directory contains scripts that are not actively used by the core system but may be useful for testing, benchmarking, or development:

- Benchmark scripts (`benchmark_*.py`)
- Test scripts (`test_*.py`, `test_*.sh`)
- Development utilities (`generate_*.py`, `setup_*.py`)
- Legacy scripts (`install_python_in_flink.sh`, `rebuild_flink_with_python.sh`)

## Typical Workflows

### After UDF Changes

```bash
# 1. Generate registry UDFs (if schemas changed)
python generate_registry_udfs.py

# 2. Rebuild Flink image
./rebuild_flink_image.sh

# 3. Restart services
cd .. && ./manage_services_full.sh restart_services
```

### Quick Flink Restart

```bash
./restart_flink.sh
```

### Clean Restart

```bash
./clean_and_restart_flink.sh
```

### Interactive SQL

```bash
./flink_sql.sh
```

### Check Kafka Topics

```bash
python kafka_query.py <topic_name>
```

## Dependencies

Most scripts require:
- Docker/Colima running
- Kafka and Flink services running
- Python 3.10+ with required packages

## Notes

- Scripts assume they're run from the `scripts/` directory
- Most scripts are called automatically by `manage_services_full.sh`
- Check individual script headers for detailed usage
