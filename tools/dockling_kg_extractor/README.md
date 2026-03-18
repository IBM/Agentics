# Docling-Agentics Knowledge Graph Extractor

A web-based interface for extracting knowledge graphs and structured entities from documents using AI, powered by Docling and Agentics with FastAPI.

## Features

- 📤 **Document Ingestion**: Upload PDF, DOCX, PPTX, images, or load from URL
- 🔄 **Smart Caching**: Reuse existing processed files or force reprocessing
- 📂 **Quick Access**: Dropdown to load previously processed documents
- 🏗️ **Entity Extraction**: Extract structured entities with custom schemas
- 📋 **Quick Templates**: Pre-built schemas for Keywords, Persons, Locations, and Organizations
- 🔗 **Coreference Resolution**: Automatically merge duplicate entities
- 📊 **Infobox Extraction**: Extract structured information about entities using AI
- 💾 **Export Results**: Download extracted data as JSON or CSV files
- 🎯 **Interactive UI**: Modern Bootstrap-based interface with progress tracking

## Prerequisites

- Python 3.11+
- Virtual environment with required packages installed
- API keys for Google/Gemini (set in environment variables)

## Installation

1. **Clone the repository and navigate to the project:**
```bash
cd /path/to/docling-graph
```

2. **Install all dependencies using uv:**
```bash
uv sync
```

This will automatically:
- Create a virtual environment (if not exists)
- Install all required packages including FastAPI, Uvicorn, Agentics, Docling, and Pandas
- Set up development tools

3. **Configure environment variables:**

Copy the example environment file:
```bash
cp agentics_kg/.env.example agentics_kg/.env
```

Edit `.env` to configure your LLM provider and settings:

```bash
# Upload Directory (optional)
DOCLING_AGENTICS_UPLOAD_DIR=/tmp/docling_agentics

# LLM Provider Configuration (choose one or more)

# Option 1: Google Gemini (recommended)
GOOGLE_API_KEY=your-google-api-key-here
# Get your key at: https://makersuite.google.com/app/apikey

# Option 2: OpenAI
OPENAI_API_KEY=your-openai-api-key-here
# Get your key at: https://platform.openai.com/api-keys

# Option 3: Anthropic Claude
ANTHROPIC_API_KEY=your-anthropic-api-key-here
# Get your key at: https://console.anthropic.com/

# Option 4: Azure OpenAI
AZURE_API_KEY=your-azure-api-key
AZURE_API_BASE=https://your-resource.openai.azure.com
AZURE_API_VERSION=2024-02-15-preview

# Default Model (optional, defaults to gemini/gemini-1.5-flash)
# Examples: gpt-4, claude-3-opus-20240229, gemini/gemini-1.5-pro
DEFAULT_MODEL=gemini/gemini-1.5-flash
```

**Note:** The application uses LiteLLM which supports 100+ LLM providers. You only need to configure one provider to get started.

## Running the Application

### Option 1: Using the Startup Script (Recommended)

Simply run the startup script which will automatically kill any existing server and start a new one:

```bash
cd /Users/gliozzo/Code/docling-graph/agentics_kg
./start_server.sh
```

### Option 2: Manual Start

1. Start the FastAPI backend server:
```bash
cd agentics_kg
source ../.venv/bin/activate
python app_simple.py
```

Or using uvicorn directly:
```bash
uvicorn app_simple:app --host 0.0.0.0 --port 5000 --reload
```

The server will start on `http://localhost:5000`

2. Open your web browser and navigate to:
```
http://localhost:5000
```

### Stopping the Server

Press `Ctrl+C` in the terminal to stop the server.

## Configuration

### Environment Variables

- `DOCLING_AGENTICS_UPLOAD_DIR`: Directory for uploaded and processed files (default: `/tmp/docling_agentics`)
- `GOOGLE_API_KEY` or `GEMINI_API_KEY`: Required for AI extraction

## Usage

### 1. Document Ingestion

**Upload Options:**
- **📁 Upload Document**: Drag & drop or browse for PDF, DOCX, PPTX, HTML, or images
- **🌐 Load from URL**: Enter a URL to a document (Docling handles it natively)
- **📂 Quick Access**: Select from previously processed documents in the dropdown

**Processing Options:**
- **Chunking Mode**: Choose between fine-grained paragraphs, full paragraphs, words, or sentences
- **Chunk Size**: Adjust the maximum words per chunk (50-500)
- **Reuse Existing**: Check to reuse previously processed files (faster) or uncheck to reprocess

### 2. Entity Extraction

**Quick Templates:**
Click a template button to quickly load a pre-built schema:
- **🔑 Keyword**: Extract keywords or key terms
- **👤 Person**: Extract people with first_name, last_name, date_of_birth, nationality, job_role
- **📍 Location**: Extract locations with name, type, country
- **🏢 Organization**: Extract organizations with name, type, industry, location

**Custom Schema:**
- Define your own schema with custom fields
- Choose field types: str, int, float, bool, List[str]
- All fields are optional by default
- Add descriptions for better extraction accuracy

**Extraction Process:**
1. Creating dynamic schema
2. Searching relevant content
3. Extracting structured data
4. **Resolving coreferences** (merges duplicate entities)
5. Consolidating results

### 3. Infobox Extraction

- Enter an entity name (e.g., "IBM", "Apple", "Microsoft")
- Adjust the number of documents to use for extraction (1-50)
- Click "Extract Infobox"
- View the Wikipedia-style infobox with extracted triples
- Download results as JSON

## File Structure

```
agentics_kg/
├── app_simple.py              # FastAPI backend API
├── pdf_to_markdown_simple.py  # Document conversion utilities
├── start_server.sh            # Server startup script
├── .env.example               # Example environment configuration
├── static/
│   ├── index_simple.html      # Main HTML page
│   ├── app_simple.js          # JavaScript frontend logic
│   └── styles_bootstrap.css   # Bootstrap styles
└── README.md                  # This file
```

**Note**: Uploaded and processed files are stored in the directory specified by `DOCLING_AGENTICS_UPLOAD_DIR` (default: `/tmp/docling_agentics`)

## API Endpoints

### GET /api/list-folders
List available preprocessed document folders.

**Response**:
```json
{
  "success": true,
  "folders": [
    {
      "name": "document_name",
      "path": "/path/to/folder",
      "csv_count": 2,
      "csv_files": ["file1.csv", "file2.csv"]
    }
  ]
}
```

### POST /api/upload
Upload and process a document.

**Parameters**:
- `file`: Document file (optional)
- `url`: Document URL (optional)
- `csv_path`: Direct path to CSV file (optional)
- `chunk_size`: Maximum words per chunk (default: 1024)
- `chunking_mode`: words|sentences|paragraphs|fine_paragraphs (default: words)
- `save_location`: Optional path to save processed CSV
- `is_preprocessed_csv`: Boolean, skip Docling processing (default: false)
- `reuse_existing`: Boolean, reuse existing processed file (default: true)

**Response**:
```json
{
  "success": true,
  "message": "File uploaded successfully. Loaded 100 rows.",
  "row_count": 100,
  "columns": ["text"],
  "filename": "document.pdf",
  "cached": false
}
```

### POST /api/search
Search for passages using semantic search.

**Request**:
```json
{
  "entity": "IBM",
  "top_n": 5
}
```

### GET /api/extract-stream
Extract infobox with Server-Sent Events for progress updates.

**Parameters**:
- `entity`: Entity name
- `num_documents`: Number of documents to use (default: 10)

### GET /api/schema/extract-stream
Extract structured entities using custom schema with progress updates.

**Parameters**:
- `schema_name`: Name of the schema
- `fields`: JSON string of field definitions
- `instructions`: Optional extraction instructions
- `as_list`: Boolean, extract multiple instances per document

### GET /api/status
Get current application status.

## Troubleshooting

### Port Already in Use
If port 5000 is already in use, you can change it in `app_simple.py`:
```python
uvicorn.run(app, host="0.0.0.0", port=5001)  # Change to any available port
```

### Upload Directory Issues
If you encounter permission issues with the upload directory:
```bash
# Set a custom upload directory
export DOCLING_AGENTICS_UPLOAD_DIR=~/docling_uploads
mkdir -p ~/docling_uploads
```

### API Key Issues
Make sure your `GOOGLE_API_KEY` or `GEMINI_API_KEY` environment variables are set:
```bash
export GOOGLE_API_KEY="your-api-key-here"
```

### Module Not Found Errors
Make sure all required packages are installed:
```bash
uv pip install fastapi uvicorn[standard] python-multipart agentics-py pandas docling
```

## Notes

- The application stores uploaded files in the directory specified by `DOCLING_AGENTICS_UPLOAD_DIR`
- Processed CSV files are cached for faster reloading
- Previously processed documents appear in the dropdown for quick access
- Large files may take longer to process
- The extraction process uses AI and may take several seconds to complete
- Coreference resolution automatically merges duplicate entities
- Results are stored in memory and will be lost when the server restarts

## License

MIT License
