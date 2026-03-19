# AGStream Manager - Quick Start Guide

## 🚀 Getting Started in 3 Steps

### 1. Configure (Optional)

Copy and customize the configuration file:

```bash
cp .env.example .env
# Edit .env if you want to customize UDF folder location
```

**Default settings work out of the box!**

### 2. Start Services

```bash
./manage_services_full.sh start
```

This automatically:
- ✅ Starts Kafka, Flink, Schema Registry, and UIs
- ✅ Installs Agentics package in Flink containers
- ✅ Copies all UDFs from `udfs/` folder to Flink
- ✅ Starts AGStream Manager service

**Wait ~2 minutes for complete startup**

### 3. Use Flink SQL with Agentics

```bash
./manage_services_full.sh flink-sql
```

Then in Flink SQL:

```sql
-- Register UDF
CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment
AS 'udfs_example.generate_sentiment'
LANGUAGE PYTHON;

-- Use it!
SELECT text, generate_sentiment(text) as sentiment
FROM my_topic
LIMIT 10;
```

## 📋 Common Commands

```bash
# Start everything (first time or after stop)
./manage_services_full.sh start
# Note: Installs Agentics + UDFs (~1 minute)

# Restart (keeps containers, faster)
./manage_services_full.sh restart
# Note: Skips installation if already present (~2 seconds)

# Stop everything
./manage_services_full.sh stop

# Clean restart (deletes Kafka data)
./manage_services_full.sh clean-restart

# Check status
./manage_services_full.sh status

# View logs
./manage_services_full.sh logs

# Open Flink SQL
./manage_services_full.sh flink-sql
```

### 💡 Performance Tip

**Use `restart` instead of `stop` + `start` to avoid reinstalling:**

```bash
# ❌ Slow (recreates containers, reinstalls everything)
./manage_services_full.sh stop
./manage_services_full.sh start

# ✅ Fast (keeps containers, skips installation)
./manage_services_full.sh restart
```

The `restart` command keeps containers running and only restarts services, so Agentics stays installed!

## 🌐 Access Points

After starting services:

- **AGStream Manager**: http://localhost:5003
- **Flink Web UI**: http://localhost:8085
- **Kafka UI**: http://localhost:8080
- **Schema Registry UI**: http://localhost:8000

## 🔧 Custom UDF Folder

Want to use UDFs from a different location? Edit `.env`:

```bash
# Use custom folder
UDF_FOLDER=my_custom_udfs

# Use UDFs from another project
UDF_FOLDER=../../../my_project/flink_udfs

# Use absolute path
UDF_FOLDER=/Users/username/my_udfs
```

Then restart:

```bash
./manage_services_full.sh restart
```

## 📝 Creating Your Own UDFs

1. **Create a Python file** in your UDF folder (default: `udfs/`)

2. **Write your UDF**:

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

@udf(result_type=DataTypes.STRING())
def my_function(text):
    # Your logic here
    return text.upper()
```

3. **Restart services** (auto-installs new UDFs):

```bash
./manage_services_full.sh restart
```

4. **Register in Flink SQL**:

```sql
CREATE TEMPORARY SYSTEM FUNCTION my_function
AS 'my_file.my_function'
LANGUAGE PYTHON;
```

## 🔍 Troubleshooting

### UDFs not working after restart?

Check auto-installation is enabled in `.env`:
```bash
AUTO_INSTALL_ON_STARTUP=true
```

### Need to reinstall manually?

```bash
cd scripts
./install_agentics_in_flink.sh  # Install Agentics
./install_udfs.sh                # Install UDFs
```

### Flink crashed?

Restart the cluster:
```bash
./manage_services_full.sh stop
./manage_services_full.sh start
```

## 📚 More Information

- **Full Documentation**: [README.md](README.md)
- **Auto-Installation Details**: [docs/AUTO_INSTALLATION.md](docs/AUTO_INSTALLATION.md)
- **UDF Examples**: [udfs/udfs_example.py](udfs/udfs_example.py)

## 💡 Pro Tips

1. **Cache AG instances** in UDFs for better performance
2. **Use type hints** in Pydantic models for better results
3. **Handle errors** gracefully in UDFs
4. **Monitor resources** - LLM operations can be memory-intensive
5. **Test UDFs locally** before deploying to Flink

## 🎯 Example Workflow

```bash
# 1. Start services
./manage_services_full.sh start

# 2. Open Flink SQL
./manage_services_full.sh flink-sql

# 3. Create a table from Kafka topic
CREATE TABLE my_topic (
    text STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'my_topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

# 4. Register UDF
CREATE TEMPORARY SYSTEM FUNCTION generate_sentiment
AS 'udfs_example.generate_sentiment'
LANGUAGE PYTHON;

# 5. Query with UDF
SELECT
    text,
    generate_sentiment(text) as sentiment
FROM my_topic
LIMIT 10;
```

## 🆘 Need Help?

- Check logs: `./manage_services_full.sh logs`
- View AGStream Manager logs: `tail -f /tmp/agstream_manager.log`
- View Flink logs: `./manage_services_full.sh logs flink-taskmanager`

---

**Made with ❤️ using Agentics**
