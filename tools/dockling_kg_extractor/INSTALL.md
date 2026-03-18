# Agentics KG Installation

## Prerequisites

- Python 3.11 or 3.12
- Main agentics-py package installed

## Installation

From the agentics_kg directory:

```bash
cd tools/agentics_kg
uv sync
```

This will:
1. Create a virtual environment in `.venv`
2. Install agentics-py and all required dependencies:
   - fastapi, uvicorn
   - python-multipart
   - pandas
   - docling, docling-core

## Running

After installation, activate the virtual environment and run:

```bash
source .venv/bin/activate
python app_simple.py
```

Or use the startup script:

```bash
./start_server.sh
```

The server will start on `http://localhost:5000`

## Dependencies

This tool requires:
- **agentics-py**: Core framework (automatically installed from parent project)
- **FastAPI/Uvicorn**: Web server framework
- **Docling**: Document processing and conversion
- **Pandas**: Data manipulation

## Notes

- The tool creates its own isolated virtual environment
- It depends on the main agentics-py package being available
- All tool-specific dependencies are managed separately from the main project
- Uploaded files are stored in `DOCLING_AGENTICS_UPLOAD_DIR` (default: `/tmp/docling_agentics`)
