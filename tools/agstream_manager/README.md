# AGStream Manager

A comprehensive web-based management system for AGStream - the streaming agent framework for Agentics. This unified application consolidates schema management, topic management, transducible function definition, and listener orchestration into a single cohesive interface.

## Overview

AGStream Manager provides a complete solution for managing streaming AI agents with:
- **Schema Type Management**: Define and manage Pydantic schema types with schema registry integration
- **Kafka Topic Management**: Create, view, and delete Kafka topics with automatic type detection
- **Transducible Functions**: Define AI-powered transformation functions between typed data streams
- **Listener Orchestration**: Start, stop, and monitor streaming listeners that process messages in real-time
- **Type Safety**: Automatic type matching ensures listeners only connect compatible topics

## Architecture

The system consists of a single unified service:

### AGStream Manager Service (`agstream_manager_service.py`)
**Port: 5003**

The unified orchestration service that provides:
- Complete API for all AGStream operations
- Schema type registration and management
- Kafka topic creation and deletion
- Transducible function definition and storage
- Listener lifecycle management (start, stop, monitor)
- Integration with Kafka and Schema Registry
- Persistent storage of functions and listeners
- Type-safe topic selection and matching

## Web Interface

### AGStream Manager UI (`agstream_manager.html`)
**Unified Management Console** with three main sections:

1. **📨 Manage Topics**
   - Create Kafka topics with associated types
   - View topic details (partitions, replication, associated type)
   - Delete topics (removes both Kafka topic and schema)
   - Auto-detection of topic types from listeners

2. **⚡ Define Transductions**
   - Create transducible functions with source and target types
   - Specify AI instructions for transformations
   - View and manage existing functions
   - Type-safe function definitions

3. **🎧 Manage Listeners**
   - Start listeners with function and topic selection
   - Type-matched topic filtering (only shows compatible topics)
   - Monitor listener status and logs
   - Stop and restart listeners
   - Persistent listener configurations

## Quick Start

### Prerequisites

1. **Kafka with KRaft** (no Zookeeper needed)
2. **Karapace Schema Registry**
3. **Python 3.8+** with Agentics library
4. **Environment variables** (see `.env_sample`)

### Starting Services

Use the unified management script:

```bash
cd examples/AGStream_Manager
./scripts/manage_agstream.sh start
```

This will:
1. Start Kafka and Karapace Schema Registry (Docker)
2. Start AGStream Manager service (Port 5003)
3. Open the web interface in your browser

### Access Web Interface

- **AGStream Manager**: http://localhost:5003 (or open `frontend/agstream_manager.html`)

### Other Commands

```bash
./scripts/manage_agstream.sh status        # Check service status
./scripts/manage_agstream.sh stop          # Stop all services
./scripts/manage_agstream.sh restart       # Restart services
./scripts/manage_agstream.sh clean-restart # Clear Kafka data and restart
./scripts/manage_agstream.sh logs          # View service logs
```

## Workflow

### 1. Define Schema Types
First, create Pydantic schema types that define your data structures:

```python
# Example: Question type
{
    "name": "Question",
    "fields": {
        "text": {"type": "str", "required": true}
    }
}
```

### 2. Create Topics
Create Kafka topics and associate them with schema types:
- Topic name: `questions`
- Associated type: `Question`
- Partitions: 1
- Replication: 1

### 3. Define Transducible Functions
Create AI-powered transformation functions:
- Function name: `QA`
- Source type: `Question`
- Target type: `Answer`
- Instructions: "Answer the input question concisely"

### 4. Start Listeners
Launch streaming listeners that process messages:
- Select function: `QA`
- Input topic: `questions` (automatically filtered to show only Question-typed topics)
- Output topic: `answers` (automatically filtered to show only Answer-typed topics)
- Lookback: 0 (process only new messages)

## Key Features

### Type Safety
- **Automatic Type Detection**: Topics automatically detect their associated types from listeners
- **Type-Matched Filtering**: Dropdown menus only show topics matching function signatures
- **Null Type Exclusion**: Topics without type assignments are hidden from selection
- **Schema Validation**: All messages are validated against registered schemas

### Persistence
- **Function Storage**: Transducible functions saved to `agstream_functions.json`
- **Listener Configurations**: Listener settings saved to `agstream_listeners.json`
- **Auto-Recovery**: Services reload saved configurations on restart

### Schema Registry Integration
- **Automatic Registration**: Schemas registered when creating topics or listeners
- **Subject Management**: Proper subject naming (`{topic}-value`)
- **Schema Deletion**: Complete cleanup when deleting types or topics
- **Type Auto-Detection**: Queries registry to determine topic types

### Kafka Integration
- **Topic Management**: Full CRUD operations on Kafka topics
- **Auto-Creation Disabled**: Prevents deleted topics from reappearing
- **Message Monitoring**: Debug tools for viewing topic messages
- **Partition Support**: Configurable partition counts

## File Structure

```
AGStream_Manager/
├── README.md                          # This file
├── agstream_functions.json            # Saved functions
├── agstream_listeners.json            # Saved listeners
├── backend/                           # Backend services
│   ├── agstream_manager_service.py    # Main unified service (Port 5003)
│   ├── function_persistence_service.py # Function storage service
│   ├── persistent_function_store.py   # Function store implementation
│   ├── app.py                         # Streamlit app
│   ├── function_store.py              # Function store interface
│   ├── listener_manager.py            # Listener utilities
│   ├── registry_client.py             # Schema registry client
│   └── atypes.py                      # Type definitions
├── frontend/                          # Web interfaces
│   ├── agstream_manager.html          # Main unified interface
│   ├── test_listener_creation.html    # Test interface
│   └── test_ui_debug.html             # Debug interface
├── scripts/                           # Utility scripts
│   ├── manage_agstream.sh             # Service management script
│   ├── force_refresh.sh               # Force refresh script
│   ├── associate_topics_with_schemas.py
│   ├── auto_associate_topics.py
│   ├── chat_client.py
│   ├── debug_topic_messages.py
│   ├── send_test_message.py
│   └── ... (other utility scripts)
├── tests/                             # Test files
│   ├── test_agstream_manager_port5003.py
│   ├── test_listener_persistence.py
│   ├── test_schema_manager.py
│   └── ... (other test files)
└── agstream_manager/                  # Shared JavaScript modules
    └── common.js                      # Common UI utilities
```

## Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Schema Registry
AGSTREAM_BACKENDS_SCHEMA_REGISTRY_URL=http://localhost:8081

# Storage
AGSTREAM_BACKENDS=./agstream-backends

# LLM Configuration (for AI functions)
OPENAI_API_KEY=your-key-here
```

### Service Ports

- AGStream Manager: 5003
- Schema Manager: 5001
- Transduction Manager: 5002
- Function Persistence: 5004

## API Endpoints

### AGStream Manager Service (Port 5003)

#### Topics
- `GET /api/topics` - List all topics
- `GET /api/topics/<name>` - Get topic details with auto-detected type
- `POST /api/topics` - Create topic with optional type association
- `DELETE /api/topics/<name>` - Delete topic and schema

#### Schema Types
- `GET /api/transductions/types` - List all schema types
- `POST /api/transductions/types` - Register new schema type
- `DELETE /api/transductions/types/<name>` - Delete schema type (all subjects)

#### Functions
- `GET /api/transductions/functions` - List all functions
- `POST /api/transductions/functions` - Create function
- `DELETE /api/transductions/functions/<name>` - Delete function

#### Listeners
- `GET /api/transductions/listeners` - List all listeners
- `POST /api/transductions/listeners` - Start listener
- `DELETE /api/transductions/listeners/<id>` - Stop/delete listener

## Troubleshooting

### Topics Show "Associated Type: None"
- Ensure the topic was created with an associated type
- Or start a listener that uses the topic (type will be auto-detected)
- Check schema registry: `curl http://localhost:8081/subjects`

### Listener Won't Start
- Verify input and output topics exist
- Check that topics have compatible types with the function
- Ensure Kafka is running: `docker ps | grep kafka`
- Check service logs for errors

### Schema Registry Errors
- Verify Karapace is running: `curl http://localhost:8081/subjects`
- Check schema registry URL in environment variables
- Ensure schemas are properly formatted JSON

### Type Filtering Not Working
- Refresh the page to reload topic types
- Check browser console for JavaScript errors
- Verify function has source_type and target_type defined

## Development

### Adding New Features

1. **Backend**: Add endpoints to `agstream_manager_service.py`
2. **Frontend**: Update `agstream_manager.html` with new UI elements
3. **Persistence**: Update JSON storage files as needed
4. **Documentation**: Update this README

### Testing

```bash
# Test topic operations
python3 test_topic_deletion.py

# Test listener creation
python3 test_create_listener.py

# Test schema enforcement
python3 test_schema_enforcement.py

# Test UI functionality
python3 test_listener_ui.py
```

## Best Practices

1. **Create Topics First**: Always create topics before defining functions or starting listeners
2. **Use Type Associations**: Associate topics with types for automatic validation
3. **Monitor Listeners**: Check listener logs regularly for errors
4. **Backup Schemas**: Use schema manager backup feature before major changes
5. **Clean Restarts**: Use `./manage_services.sh clean-restart` to reset Kafka data

## Support

For issues, questions, or contributions, please refer to the main Agentics repository.

## License

Part of the Agentics framework. See main repository for license information.
