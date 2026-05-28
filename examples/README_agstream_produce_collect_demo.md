# AGStream Produce & Collect Demo

A comprehensive demonstration script showing how to produce and collect messages using AGStream with the full AGStream Manager infrastructure.

## Prerequisites

### Required Services

All services must be running before executing the demo:

1. **Kafka Broker** - `localhost:9092`
2. **Schema Registry** - `http://localhost:8081`
3. **AGStream Manager** - `http://localhost:5003`

### Optional Services

These enhance the demo but are not required:

4. **Kafka Connect** - `http://localhost:8083`
5. **Control Center** - `http://localhost:9021`
6. **Flink Cluster** - `http://localhost:8085`

### Starting Services

If you have the AGStream Manager setup, start all services with:

```bash
cd tools/agstream_manager
./scripts/manage_services_full.sh start
```

Or use Docker Compose:

```bash
cd agstream-backends
docker-compose up -d
```

## Running the Demo

```bash
python examples/agstream_produce_collect_demo.py
```

## What the Demo Shows

### Demo 1: Basic Produce & Collect
- Creates an AGStream instance for sensor readings
- Produces 3 sensor readings to Kafka
- Collects them back and displays the data
- Shows message IDs and data validation

### Demo 2: Collection Modes
- Demonstrates different collection strategies:
  - **From Beginning**: Collects all historical messages
  - **Latest Only**: Collects only new messages
- Shows how consumer groups work

### Demo 3: Continuous Listener (5 seconds)
- Sets up input and output streams
- Produces questions to input topic
- Runs a listener that processes questions → answers
- Demonstrates transduction functions
- Collects the generated answers

### Demo 4: Schema Registry Information
- Lists all registered schemas
- Shows schema IDs and versions
- Demonstrates schema registry integration

### Demo 5: Service Health Check
- Checks connectivity to all services
- Reports online/offline status
- Validates the infrastructure is ready

## Expected Output

```
======================================================================
AGStream Produce & Collect Demo
======================================================================

This demo requires the following services to be running:
  - Kafka Broker (localhost:9092)
  - Schema Registry (localhost:8081)
  - AGStream Manager (localhost:5003)

Press Ctrl+C at any time to stop.
======================================================================

======================================================================
DEMO 5: Service Health Check
======================================================================

🏥 Checking service health...
   ✅ AGStream Manager: Online
   ✅ Schema Registry: Online
   ✅ Kafka Connect: Online
   ✅ Control Center: Online
   ✅ Flink JobManager: Online

🧱 Checking Kafka Broker...
   ✅ Kafka Broker: Online (15 topics)

======================================================================
DEMO 1: Basic Produce & Collect
======================================================================

📝 Creating AGStream for sensor readings...
✓ Registered Avro schema 'SensorReading-value' (ID: 123)

📤 Producing sensor readings...
✅ Produced 3 sensor readings
   [1] ID: a1b2c3d4... → sensor-001
   [2] ID: e5f6g7h8... → sensor-002
   [3] ID: i9j0k1l2... → sensor-003

⏳ Waiting for messages to be available...

📥 Collecting sensor readings...
✅ Collected 3 sensor readings:
   - sensor-001: 22.5°C, 45.0% @ warehouse-a
   - sensor-002: 28.3°C, 60.5% @ warehouse-b
   - sensor-003: 19.8°C, 40.2% @ warehouse-c

[... more demos ...]

======================================================================
✅ All demos completed successfully!
======================================================================

💡 Next Steps:
   1. Check Schema Registry UI: http://localhost:8081
   2. Check Control Center: http://localhost:9021
   3. Check AGStream Manager: http://localhost:5003
   4. Query data with Flink SQL (see examples/agstream_sql_example.py)
======================================================================
```

## Key Concepts Demonstrated

### 1. AGStream Creation
```python
stream = AGStream(
    atype=SensorReading,           # Pydantic model
    topic="sensor_readings",        # Kafka topic
    kafka_server="localhost:9092",
    schema_registry_url="http://localhost:8081",
    auto_create_topic=True,         # Create if doesn't exist
    num_partitions=3                # Partition count
)
```

### 2. Producing Messages
```python
readings = [
    SensorReading(sensor_id="001", temperature=22.5, ...),
    SensorReading(sensor_id="002", temperature=28.3, ...),
]
message_ids = stream.produce(readings)  # Returns list of UUIDs
```

### 3. Collecting Messages
```python
# Collect from beginning
collected = stream.consume(limit=10, from_beginning=True)

# Collect only new messages
collected = stream.consume(limit=10, from_beginning=False)
```

### 4. Continuous Listening
```python
def process_message(input_msg):
    # Transform input to output
    return output_msg

stream.listen(
    transduction_fn=process_message,
    output_stream=output_stream,
    max_iterations=10,
    verbose=True
)
```

## Troubleshooting

### Services Not Running
```
❌ Kafka Broker: Error - connection refused
```
**Solution**: Start the services with `manage_services_full.sh start`

### Schema Registry Connection Failed
```
✗ Failed to register Avro schema 'SensorReading-value': Connection refused
```
**Solution**: Verify Schema Registry is running on port 8081

### No Messages Collected
```
✅ Collected 0 sensor readings:
```
**Solution**:
- Check if messages were produced successfully
- Verify topic exists in Kafka
- Try `from_beginning=True` to collect historical messages

### Import Errors
```
ImportError: No module named 'kafka'
```
**Solution**: Install streaming dependencies:
```bash
pip install kafka-python confluent-kafka requests
```

## Next Steps

After running this demo:

1. **Explore Schema Registry UI**: http://localhost:8081
   - View registered schemas
   - Check schema versions
   - Validate compatibility

2. **Use Control Center**: http://localhost:9021
   - Monitor topics
   - View consumer groups
   - Check message throughput

3. **Query with Flink SQL**: See `examples/agstream_sql_example.py`
   - Create SQL tables from topics
   - Run analytical queries
   - Join multiple streams

4. **Build Your Own Pipeline**:
   - Define custom Pydantic models
   - Create transduction functions
   - Set up listeners for real-time processing

## Related Examples

- `agstream_sql_example.py` - Flink SQL queries
- `agpersist_search_example.py` - Vector search integration
- `flink_sql_auto_connector_example.py` - Automatic table registration

## Documentation

- AGStream API: `src/agentics/core/streaming/agstream_sql.py`
- Collection Methods: See "How collect works" documentation
- Schema Registry: `tools/agstream_manager/SCHEMA_REGISTRY_UI_GUIDE.md`
