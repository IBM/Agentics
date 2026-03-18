# AGStream Manager Installation

## Prerequisites

- Python 3.11 or 3.12
- Main agentics-py package installed

## Installation

From the agstream_manager directory:

```bash
cd tools/agstream_manager
uv sync
```

This will:
1. Create a virtual environment in `.venv`
2. Install agentics-py and all required dependencies:
   - flask, flask-cors, flask-sock
   - streamlit
   - psutil
   - requests
   - pydantic

## Running

After installation, activate the virtual environment and run:

```bash
source .venv/bin/activate
./manage_services_full.sh start-agstream-manager
```

Or use the startup script which handles activation automatically:

```bash
./manage_services_full.sh start-agstream-manager
```

## Dependencies

This tool requires:
- **agentics-py**: Core framework (automatically installed from parent project)
- **Flask ecosystem**: Web server and WebSocket support
- **Streamlit**: Alternative UI framework
- **psutil**: Process management
- **requests**: HTTP client for Kafka/Schema Registry APIs

## Notes

- The tool creates its own isolated virtual environment
- It depends on the main agentics-py package being available
- All tool-specific dependencies are managed separately from the main project
