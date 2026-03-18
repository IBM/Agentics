import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pdf_to_markdown_simple import document_to_csv
from pydantic import BaseModel, Field, create_model

from agentics import AG

app = FastAPI(title="Docling-Agentics Knowledge Graph Extractor")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Configuration - Get upload folder from environment variable or use default
UPLOAD_FOLDER = os.getenv("DOCLING_AGENTICS_UPLOAD_DIR", "/tmp/docling_agentics")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
print(f"Upload directory: {UPLOAD_FOLDER}")

# Simple in-memory state
current_data = {"df": None, "csv_path": None, "ag_corpus": None, "filename": None}


@app.get("/api/list-folders")
async def list_folders():
    """List available preprocessed document folders in the upload directory"""
    try:
        folders = []
        if os.path.exists(UPLOAD_FOLDER):
            for item in os.listdir(UPLOAD_FOLDER):
                item_path = os.path.join(UPLOAD_FOLDER, item)
                if os.path.isdir(item_path):
                    # Check if folder contains CSV files
                    csv_files = [f for f in os.listdir(item_path) if f.endswith(".csv")]
                    if csv_files:
                        folders.append(
                            {
                                "name": item,
                                "path": item_path,
                                "csv_count": len(csv_files),
                                "csv_files": csv_files,
                            }
                        )
        return {"success": True, "folders": folders}
    except Exception as e:
        return {"success": False, "error": str(e), "folders": []}


# Pydantic models
class SearchRequest(BaseModel):
    entity: str
    top_n: int = 5


class ExtractRequest(BaseModel):
    entity: str
    max_rows: Optional[int] = None
    num_documents: Optional[int] = 10


class Relation(BaseModel):
    """A semantic triple representing a relationship between entities."""

    subject: Optional[str] = Field(
        default=None,
        description="The subject entity in the relationship (e.g., 'IBM', 'John Smith', 'Python')",
    )
    relation: Optional[str] = Field(
        default=None,
        description="The type of relationship or predicate connecting subject and object (e.g., 'founded_by', 'located_in', 'has_property')",
    )
    object: Optional[str] = Field(
        default=None,
        description="The object entity or value in the relationship (e.g., 'Thomas Watson', 'New York', '1911')",
    )


class Relations(BaseModel):
    """A collection of semantic relations extracted from text."""

    relations: Optional[List[Relation]] = Field(
        default=None,
        description="List of semantic triples (subject-relation-object) representing knowledge extracted from the source text",
    )


class Infobox(BaseModel):
    """A structured summary of key information about an entity, similar to Wikipedia infoboxes.

    Example infobox structure:
    {
        "entity": "IBM",
        "infobox": {
            "founded": "1911",
            "founder": ["Charles Ranlett Flint", "Thomas J. Watson"],
            "headquarters": "Armonk, New York, United States",
            "industry": "Information technology",
            "products": ["Cloud computing", "Artificial intelligence", "Quantum computing"],
            "revenue": "$60.5 billion (2023)",
            "employees": "282,100 (2023)",
            "website": "ibm.com"
        }
    }
    """

    entity: Optional[str] = Field(
        default=None,
        description="The main entity this infobox describes (e.g., 'IBM', 'Albert Einstein', 'Python programming language')",
    )
    infobox: Optional[Dict[str, Union[str, int, float, List[Any]]]] = Field(
        default=None,
        description=(
            "Infobox data as a dictionary where keys are attribute names and values can be:\n"
            "- Single values (str, int, float) for simple attributes\n"
            "- Lists for multi-valued attributes\n"
            "Example: {'founded': '1911', 'founders': ['Person A', 'Person B'], 'revenue': 60.5}"
        ),
    )


def create_dynamic_pydantic_model(
    schema_name: str, fields: List[Dict[str, Any]]
) -> type[BaseModel]:
    """
    Dynamically create a Pydantic model from field definitions.

    Args:
        schema_name: Name of the schema/model
        fields: List of field definitions with name, type, description, required

    Returns:
        A dynamically created Pydantic BaseModel class
    """
    field_definitions = {}

    # Type mapping from string to Python types
    type_map = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "List[str]": List[str],
        "List[int]": List[int],
        "Optional[str]": Optional[str],
        "Optional[int]": Optional[int],
    }

    for field in fields:
        field_name = field["name"]
        field_type_str = field["type"]
        description = field.get("description", "")
        required = field.get("required", True)

        # Get Python type
        python_type = type_map.get(field_type_str, str)

        # Handle optional fields
        if not required and not field_type_str.startswith("Optional"):
            python_type = Optional[python_type]
            field_definitions[field_name] = (
                python_type,
                Field(default=None, description=description),
            )
        else:
            if field_type_str.startswith("Optional"):
                field_definitions[field_name] = (
                    python_type,
                    Field(default=None, description=description),
                )
            else:
                field_definitions[field_name] = (
                    python_type,
                    Field(description=description),
                )

    # Create and return the dynamic model
    return create_model(schema_name, **field_definitions)


async def extract_with_custom_schema(
    corpus: AG,
    schema_model: type[BaseModel],
    instructions: Optional[str] = None,
    max_results: int = 10,
    as_list: bool = False,
) -> AsyncGenerator[dict, None]:
    """
    Extract structured data from corpus using a custom schema with progress updates.

    Args:
        corpus: AG corpus to extract from
        schema_model: Dynamically created Pydantic model
        instructions: Optional extraction instructions
        max_results: Maximum number of results to extract
        as_list: If True, wraps schema in List[schema_model] to extract multiple instances per document

    Yields:
        Progress updates and final results
    """
    yield {
        "status": "progress",
        "message": "Creating extraction schema...",
        "progress": 10,
    }

    # Determine safe k value for search (minimum 2 for vector search)
    corpus_size = len(corpus.states) if hasattr(corpus, "states") else 10
    k = min(max_results, max(2, corpus_size))

    if corpus_size < 2:
        yield {
            "status": "error",
            "message": f"Corpus too small for schema extraction (size: {corpus_size}). Need at least 2 documents.",
        }
        return

    yield {
        "status": "progress",
        "message": "Searching relevant content...",
        "progress": 35,
    }

    try:
        # Search for relevant content (use all corpus for schema extraction)
        # We'll extract from the entire corpus rather than searching
        selected_sources = corpus

    except Exception as e:
        yield {"status": "error", "message": f"Search failed: {str(e)}"}
        return

    yield {
        "status": "progress",
        "message": "Extracting structured data...",
        "progress": 60,
    }

    # If as_list is True, create a wrapper model with a list field
    if as_list:
        # Create a wrapper model that contains a list of the schema
        wrapper_name = f"{schema_model.__name__}List"
        wrapper_model = create_model(
            wrapper_name,
            items=(
                List[schema_model],
                Field(description=f"List of {schema_model.__name__} instances"),
            ),
        )
        extraction_type = wrapper_model
        type_description = f"List[{schema_model.__name__}]"
        default_instructions = f"Extract multiple {schema_model.__name__} instances from the document as a list"
    else:
        extraction_type = schema_model
        type_description = schema_model.__name__
        default_instructions = (
            f"Extract {schema_model.__name__} information from the document"
        )

    # Create AG with custom schema
    extractor = AG(
        atype=extraction_type,
        instructions=instructions or default_instructions,
        transduction_type="amap",  # Use amap to extract multiple instances
    )

    # Apply extraction
    results = await (extractor << selected_sources)

    yield {"status": "progress", "message": "Resolving coreferences...", "progress": 75}

    # Create a coreference resolution model to aggregate duplicate entities
    # Use the proper pattern for creating a model with a list field
    coref_model_name = f"{schema_model.__name__}Aggregated"
    coref_model = create_model(
        coref_model_name,
        entities=(
            Optional[list[schema_model]] | None,
            Field(
                None,
                description=f"Aggregated list of unique {schema_model.__name__} entities with coreferent entities merged",
            ),
        ),
    )

    # Create AG for coreference resolution
    coref_resolver = AG(
        atype=coref_model,
        instructions=f"Aggregate and merge coreferent {schema_model.__name__} entities. Identify entities that refer to the same real-world entity (e.g., same keyword, person, location, or organization mentioned multiple times) and merge them into single entities. Combine their attributes, keeping the most complete and accurate information. Remove duplicates.Sort by most frequently merged. Always return a list entities of the given type",
        transduction_type="areduce",  # Use areduce to aggregate all results
        areduce_batch_size=50,
    )

    # Apply coreference resolution
    resolved_results = await (coref_resolver << results)

    yield {"status": "progress", "message": "Consolidating results...", "progress": 90}

    # Convert results to list of dicts
    extracted_data = []
    if hasattr(resolved_results, "states") and len(resolved_results.states) > 0:
        # Get the aggregated result for all batches and concatenate them
        aggregated_state = coref_model()
        aggregated_state.entities = []
        for state in resolved_results:
            if state.entities is not None:
                aggregated_state.entities += state.entities
        try:
            state_dict = aggregated_state.model_dump()
            if "entities" in state_dict and isinstance(state_dict["entities"], list):
                extracted_data = state_dict["entities"]
            else:
                # Fallback: if no entities field, use the whole state
                extracted_data = [state_dict]
        except Exception as e:
            print(f"Error converting aggregated state to dict: {e}")
            # Fallback to original results without coreference resolution
            if hasattr(results, "states"):
                for state in results.states[:max_results]:
                    try:
                        state_dict = state.model_dump()
                        if as_list and "items" in state_dict:
                            items = state_dict["items"]
                            if isinstance(items, list):
                                extracted_data.extend(items)
                            else:
                                extracted_data.append(state_dict)
                        else:
                            extracted_data.append(state_dict)
                    except Exception as e2:
                        print(f"Error in fallback conversion: {e2}")
                        continue

    yield {
        "status": "complete",
        "message": f"Extracted {len(extracted_data)} records",
        "progress": 100,
        "results": extracted_data,
    }


async def extract_infobox_with_progress(
    source: AG, entity: str, num_documents: int = 10
) -> AsyncGenerator[dict, None]:
    """Extract infobox using Agentics transduction with progress updates.

    Args:
        source: AG corpus to search
        entity: Entity to extract infobox for
        num_documents: Number of documents to use for extraction (default: 10)

    Yields:
        Progress updates and final result with infobox and extracted triples
    """
    import sys

    # Determine safe k value based on corpus size and user preference (minimum 2 for vector search)
    corpus_size = len(source.states) if hasattr(source, "states") else 10
    k = min(
        num_documents, max(2, corpus_size)
    )  # Use requested amount, at least 2, but not more than corpus size

    if corpus_size < 2:
        yield {
            "status": "error",
            "message": f"Corpus too small for extraction (size: {corpus_size}). Need at least 2 documents.",
        }
        return

    yield {"status": "progress", "message": "Search in progress...", "progress": 25}

    try:
        selected_sources = source.search(entity, k=k)
        print(
            f"✓ Found {len(selected_sources.states) if hasattr(selected_sources, 'states') else k} relevant documents",
            flush=True,
        )

    except RuntimeError as e:
        if "Cannot return the results in a contiguous 2D array" in str(e):
            k = max(1, corpus_size // 2)
            selected_sources = source.search(entity, k=k)
        else:
            yield {"status": "error", "message": f"Search failed: {str(e)}"}
            return

    # Extract search results for display
    search_passages = []
    if hasattr(selected_sources, "states"):
        for i, state in enumerate(selected_sources.states):
            search_passages.append({"index": i, "text": str(state)})

    yield {"status": "progress", "message": "Extracting relations...", "progress": 50}

    relations = AG(
        atype=Relations, instructions="Extract semantic relations from the source text"
    )
    relations = await (relations << selected_sources)
    print(
        f"✓ Extracted relations from {len(relations.states) if hasattr(relations, 'states') else 'N/A'} documents",
        flush=True,
    )

    yield {"status": "progress", "message": "Aggregating infobox...", "progress": 75}

    infobox = AG(
        atype=Infobox,
        instructions=f"Extract infobox about {entity} from the source text. The infobox should be a dictionary in which fields names are relations and their values are other related entities or lists of entities",
        transduction_type="areduce",
    )
    infobox = await (infobox << relations)

    result = infobox[0]

    # Extract all triples from relations
    all_triples = []
    if hasattr(relations, "states"):
        for rel_state in relations.states:
            if hasattr(rel_state, "relations") and rel_state.relations:
                for relation in rel_state.relations:
                    all_triples.append(relation.model_dump())

    yield {
        "status": "complete",
        "message": "Extraction complete",
        "progress": 100,
        "result": result.model_dump(),
        "triples": all_triples,
        "num_triples": len(all_triples),
        "search_results": search_passages,
        "num_documents_used": len(search_passages),
    }


@app.get("/")
async def root():
    """Serve the main HTML page."""
    return FileResponse("static/index_simple.html")


# Global variable to track upload progress
upload_progress = {"stage": "idle", "progress": 0, "message": "", "file_size_mb": 0}


@app.get("/api/upload-progress")
async def get_upload_progress():
    """Stream upload progress via Server-Sent Events."""

    async def event_generator():
        last_progress = -1
        while True:
            current_progress = upload_progress["progress"]

            # Only send update if progress changed
            if current_progress != last_progress:
                data = {
                    "stage": upload_progress["stage"],
                    "progress": current_progress,
                    "message": upload_progress["message"],
                }
                yield f"data: {json.dumps(data)}\n\n"
                last_progress = current_progress

                # Stop streaming when complete
                if current_progress >= 100:
                    break

            await asyncio.sleep(0.1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/upload")
async def upload_file(
    file: Optional[UploadFile] = File(None),
    url: Optional[str] = Form(None),
    chunk_size: int = Form(1024),
    chunking_mode: str = Form("words"),
    save_location: Optional[str] = Form(None),
    is_preprocessed_csv: bool = Form(False),
    reuse_existing: bool = Form(True),
    csv_path: Optional[str] = Form(None),
):
    """
    Process a document from file upload, URL, or direct CSV path.

    If csv_path is provided, load that CSV directly.
    If is_preprocessed_csv is True, the file is loaded directly as CSV without Docling processing.
    Otherwise, Docling's DocumentConverter.convert() handles both local files and URLs natively.
    """
    try:
        # Reset progress
        upload_progress["stage"] = "uploading"
        upload_progress["progress"] = 0
        upload_progress["message"] = "Starting..."

        # Handle direct CSV path loading
        if csv_path and os.path.exists(csv_path):
            upload_progress["message"] = "Loading preprocessed CSV..."
            upload_progress["progress"] = 30
            await asyncio.sleep(0.1)

            try:
                df = pd.read_csv(csv_path)
                print(f"Loaded CSV from path: {csv_path} with {len(df)} rows")

                current_data["df"] = df
                current_data["csv_path"] = csv_path

                # Create AG corpus
                upload_progress["stage"] = "indexing"
                upload_progress["progress"] = 70
                upload_progress["message"] = "Creating vector database..."
                await asyncio.sleep(0.1)

                with open(csv_path, "r") as f:
                    current_data["ag_corpus"] = AG.from_csv(f)
                current_data["filename"] = os.path.basename(csv_path)

                upload_progress["stage"] = "complete"
                upload_progress["progress"] = 100
                upload_progress["message"] = f"Complete! Loaded {len(df)} chunks"

                return {
                    "success": True,
                    "message": f"Loaded preprocessed CSV. {len(df)} rows.",
                    "row_count": len(df),
                    "columns": df.columns.tolist(),
                    "filename": os.path.basename(csv_path),
                    "cached": True,
                }
            except Exception as e:
                print(f"Error loading CSV from path: {e}")
                raise HTTPException(
                    status_code=500, detail=f"Error loading CSV: {str(e)}"
                )

        document_source = None
        filename = None

        # Handle file upload - save temporarily for Docling to process
        if file and file.filename:
            upload_progress["message"] = "Uploading file..."
            filename = file.filename
            filepath = os.path.join(UPLOAD_FOLDER, filename)

            # Check if CSV already exists for this file (only if reuse_existing is True)
            csv_path = filepath.rsplit(".", 1)[0] + ".csv"
            if reuse_existing and os.path.exists(csv_path) and not is_preprocessed_csv:
                print(f"Found existing processed CSV: {csv_path}")
                upload_progress["progress"] = 50
                upload_progress["message"] = "Found existing processed version..."
                await asyncio.sleep(0.2)

                try:
                    df = pd.read_csv(csv_path)
                    print(f"Loaded existing CSV with {len(df)} rows")

                    current_data["df"] = df
                    current_data["csv_path"] = csv_path

                    # Create AG corpus
                    upload_progress["stage"] = "indexing"
                    upload_progress["progress"] = 80
                    upload_progress["message"] = "Creating vector database..."
                    await asyncio.sleep(0.2)

                    with open(csv_path, "r") as f:
                        current_data["ag_corpus"] = AG.from_csv(f)
                    current_data["filename"] = filename

                    upload_progress["stage"] = "complete"
                    upload_progress["progress"] = 100
                    upload_progress["message"] = (
                        f"Complete! Loaded {len(df)} chunks from cache"
                    )

                    return {
                        "success": True,
                        "message": f"Loaded existing processed version. {len(df)} rows.",
                        "row_count": len(df),
                        "columns": list(df.columns),
                        "filename": filename,
                        "cached": True,
                    }
                except Exception as e:
                    print(f"Error loading existing CSV, will reprocess: {str(e)}")
                    # Continue to normal processing if cached version fails

            file_size = 0

            with open(filepath, "wb") as f:
                chunk_num = 0
                while chunk := await file.read(1024 * 1024):  # Read 1MB at a time
                    f.write(chunk)
                    chunk_num += 1
                    file_size += len(chunk)
                    upload_progress["progress"] = min(15, chunk_num * 2)
                    upload_progress["message"] = (
                        f"Uploading... {file_size / (1024*1024):.1f} MB"
                    )

            upload_progress["file_size_mb"] = file_size / (1024 * 1024)
            upload_progress["progress"] = 20
            upload_progress["message"] = (
                f'Upload complete ({upload_progress["file_size_mb"]:.1f} MB)'
            )
            document_source = filepath

            # If it's a preprocessed CSV, skip Docling processing
            if is_preprocessed_csv and filename.lower().endswith(".csv"):
                upload_progress["stage"] = "loading"
                upload_progress["progress"] = 50
                upload_progress["message"] = "Loading preprocessed CSV..."
                await asyncio.sleep(0.2)

                try:
                    df = pd.read_csv(filepath)

                    # Validate CSV has 'text' column
                    if "text" not in df.columns:
                        raise HTTPException(
                            status_code=400,
                            detail=f"CSV must have a 'text' column. Found columns: {list(df.columns)}",
                        )

                    print(f"Loaded preprocessed CSV with {len(df)} rows")
                    upload_progress["progress"] = 70
                    upload_progress["message"] = f"Loaded {len(df)} preprocessed chunks"

                    current_data["df"] = df
                    csv_path = filepath
                    current_data["csv_path"] = csv_path

                    # Create AG corpus directly
                    upload_progress["stage"] = "indexing"
                    upload_progress["progress"] = 80
                    upload_progress["message"] = "Creating vector database..."
                    await asyncio.sleep(0.2)

                    with open(csv_path, "r") as f:
                        current_data["ag_corpus"] = AG.from_csv(f)
                    current_data["filename"] = filename

                    upload_progress["stage"] = "complete"
                    upload_progress["progress"] = 100
                    upload_progress["message"] = f"Complete! Loaded {len(df)} chunks"

                    return {
                        "success": True,
                        "message": f"Preprocessed CSV loaded successfully. Loaded {len(df)} rows.",
                        "row_count": len(df),
                        "columns": list(df.columns),
                        "filename": filename,
                        "preprocessed": True,
                    }

                except Exception as csv_error:
                    print(f"Error loading CSV: {str(csv_error)}")
                    import traceback

                    traceback.print_exc()
                    raise HTTPException(
                        status_code=500,
                        detail=f"Error loading preprocessed CSV: {str(csv_error)}",
                    )

        # Handle URL - try Docling native first, fallback to download if needed
        elif url:
            upload_progress["message"] = "Preparing to process URL..."
            upload_progress["progress"] = 10
            filename = url.split("/")[-1] or "document.pdf"

            # For some URLs (like arXiv), we may need to download first
            # We'll try Docling native first, and fallback to download if it fails
            document_source = url
            upload_progress["progress"] = 20
            upload_progress["message"] = "URL ready for processing"
        else:
            raise HTTPException(
                status_code=400, detail="Either file or url must be provided"
            )

        await asyncio.sleep(0.2)

        # Convert document using Docling (handles both local files and URLs)
        upload_progress["stage"] = "converting"
        upload_progress["progress"] = 25
        upload_progress["message"] = "Converting document with Docling..."
        await asyncio.sleep(0.2)

        # Pass source directly to Docling - it handles everything!
        print(f"Processing document from: {document_source}")
        print(f"Chunking mode: {chunking_mode}, chunk_size: {chunk_size}")
        try:
            df = document_to_csv(
                document_source, chunk_size=chunk_size, chunking_mode=chunking_mode
            )
            print(f"Successfully created DataFrame with {len(df)} rows")
        except Exception as conv_error:
            # If URL processing fails, try downloading first
            if document_source and document_source.startswith("http"):
                print(f"Docling couldn't process URL directly, downloading first...")
                upload_progress["message"] = "Downloading from URL..."
                upload_progress["progress"] = 30

                import tempfile
                import urllib.request

                # Download to temp file
                temp_path = os.path.join(UPLOAD_FOLDER, filename)
                try:
                    urllib.request.urlretrieve(document_source, temp_path)
                    file_size = os.path.getsize(temp_path)
                    upload_progress["file_size_mb"] = file_size / (1024 * 1024)
                    print(f"Downloaded {file_size / (1024*1024):.1f} MB to {temp_path}")

                    # Try again with downloaded file
                    upload_progress["message"] = "Converting downloaded document..."
                    upload_progress["progress"] = 40
                    df = document_to_csv(
                        temp_path, chunk_size=chunk_size, chunking_mode=chunking_mode
                    )
                    document_source = temp_path  # Update source for CSV saving
                    print(f"Successfully created DataFrame with {len(df)} rows")
                except Exception as download_error:
                    print(f"Download also failed: {str(download_error)}")
                    import traceback

                    traceback.print_exc()
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to process URL: {str(download_error)}",
                    )
            else:
                print(f"Error in document_to_csv: {str(conv_error)}")
                import traceback

                traceback.print_exc()
                raise

        upload_progress["progress"] = 60
        upload_progress["message"] = f"Chunking complete ({len(df)} chunks created)"
        await asyncio.sleep(0.2)

        current_data["df"] = df

        # Save DataFrame to CSV for AG.from_csv
        upload_progress["progress"] = 70
        upload_progress["message"] = "Saving processed data..."

        # Determine save location
        if save_location:
            csv_path = (
                save_location
                if save_location.endswith(".csv")
                else save_location + ".csv"
            )
        elif document_source and not document_source.startswith("http"):
            # Local file - save next to it
            csv_path = document_source.rsplit(".", 1)[0] + ".csv"
        else:
            # URL or no source - save in uploads folder
            safe_filename = filename.rsplit(".", 1)[0] if filename else "document"
            csv_path = os.path.join(UPLOAD_FOLDER, f"{safe_filename}.csv")

        df.to_csv(csv_path, index=False)
        current_data["csv_path"] = csv_path
        print(f"Saved processed CSV to: {csv_path}")

        # Create AG corpus with VDB
        upload_progress["stage"] = "indexing"
        upload_progress["progress"] = 75
        upload_progress["message"] = "Creating vector database..."
        await asyncio.sleep(0.2)

        print(f"Creating AG corpus from {current_data['csv_path']}")
        with open(current_data["csv_path"], "r") as f:
            current_data["ag_corpus"] = AG.from_csv(f)
        current_data["filename"] = filename

        upload_progress["progress"] = 95
        upload_progress["message"] = "Finalizing..."
        await asyncio.sleep(0.2)

        row_count = len(current_data["df"])
        columns = list(current_data["df"].columns)

        upload_progress["stage"] = "complete"
        upload_progress["progress"] = 100
        upload_progress["message"] = f"Complete! Loaded {row_count} chunks"

        return {
            "success": True,
            "message": f"File uploaded successfully. Loaded {row_count} rows.",
            "row_count": row_count,
            "columns": columns,
            "filename": filename,
        }

    except Exception as e:
        upload_progress["stage"] = "error"
        upload_progress["progress"] = 0
        upload_progress["message"] = f"Error: {str(e)}"
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/api/search")
async def search_entity(request: SearchRequest):
    """Search for passages using Agentics AG.search() semantic search."""
    if current_data["ag_corpus"] is None:
        raise HTTPException(
            status_code=400, detail="No data loaded. Please upload a file first."
        )

    if not request.entity:
        raise HTTPException(status_code=400, detail="Entity name is required")

    try:
        corpus = current_data["ag_corpus"]
        df = current_data["df"]

        if corpus is None or df is None:
            raise HTTPException(status_code=400, detail="No data loaded")

        print(f"\n{'='*60}")
        print(f"AG Semantic Search for: '{request.entity}'")
        print(f"Requested top_n: {request.top_n}")

        # Determine safe k value (minimum 2 for vector search to work)
        corpus_size = len(corpus.states) if hasattr(corpus, "states") else len(df)
        k = min(request.top_n, max(2, corpus_size))

        print(f"Corpus size: {corpus_size}")
        print(f"Actual k used for search: {k}")

        if corpus_size < 2:
            return {
                "success": False,
                "error": f"Corpus too small for search (size: {corpus_size}). Need at least 2 documents.",
                "passages": [],
            }

        if k < request.top_n:
            print(
                f"⚠️  WARNING: Requested {request.top_n} results but corpus only has {corpus_size} chunks"
            )

        try:
            # Use AG semantic search
            selected_sources = corpus.search(request.entity, k=k)
            print(f"✓ AG search completed successfully")

            # Extract passages from search results
            passages = []
            if hasattr(selected_sources, "states"):
                for i, state in enumerate(selected_sources.states):
                    # Convert state to dict
                    passage = {"index": i, "text": str(state)}
                    passages.append(passage)
                print(f"Extracted {len(passages)} passages from AG search results")
            else:
                print("Warning: AG search returned no states")

        except RuntimeError as e:
            if "Cannot return the results in a contiguous 2D array" in str(e):
                # Fallback: use smaller k
                k = max(1, corpus_size // 2)
                print(f"⚠ Search failed, retrying with k={k}")
                selected_sources = corpus.search(request.entity, k=k)

                passages = []
                if hasattr(selected_sources, "states"):
                    for i, state in enumerate(selected_sources.states):
                        passage = {"index": i, "text": str(state)}
                        passages.append(passage)
            else:
                raise

        print(f"Returning {len(passages)} passages")
        print(f"{'='*60}\n")

        return {
            "success": True,
            "count": len(passages),
            "requested": request.top_n,
            "total_matches": len(passages),
            "passages": passages,
        }

    except Exception as e:
        import traceback

        print(f"Search error: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error searching: {str(e)}")


@app.get("/api/extract-stream")
async def extract_infobox_stream(entity: str, num_documents: int = 10):
    """Extract infobox with Server-Sent Events for progress updates."""
    print(
        f"\n[SSE] Extract stream called for entity: {entity}, num_documents: {num_documents}",
        flush=True,
    )

    if current_data["csv_path"] is None:
        print("[SSE] Error: No data loaded", flush=True)

        async def error_generator():
            yield f"data: {json.dumps({'status': 'error', 'message': 'No data loaded'})}\n\n"

        return StreamingResponse(error_generator(), media_type="text/event-stream")

    if not entity:
        print("[SSE] Error: No entity provided", flush=True)

        async def error_generator():
            yield f"data: {json.dumps({'status': 'error', 'message': 'Entity name is required'})}\n\n"

        return StreamingResponse(error_generator(), media_type="text/event-stream")

    async def event_generator():
        try:
            print(f"[SSE] Starting event generator for {entity}", flush=True)
            corpus = current_data["ag_corpus"]
            if corpus is None:
                print("[SSE] Error: Corpus not initialized", flush=True)
                yield f"data: {json.dumps({'status': 'error', 'message': 'Corpus not initialized'})}\n\n"
                return

            print(
                f"[SSE] Corpus ready, starting extraction with {num_documents} documents",
                flush=True,
            )
            async for update in extract_infobox_with_progress(
                corpus, entity, num_documents
            ):
                print(f"[SSE] Sending update: {update}", flush=True)
                yield f"data: {json.dumps(update)}\n\n"
                await asyncio.sleep(0.1)  # Small delay to ensure UI updates
            print(f"[SSE] Extraction complete", flush=True)
        except Exception as e:
            import traceback

            tb = traceback.format_exc()
            print(f"[SSE] Error: {e}", flush=True)
            print(f"[SSE] Traceback: {tb}", flush=True)
            yield f"data: {json.dumps({'status': 'error', 'message': str(e), 'traceback': tb})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/schema/extract-stream")
async def schema_extract_stream(
    schema_name: str, fields: str, instructions: str = "", as_list: bool = False
):
    """
    Extract structured data using a custom schema with Server-Sent Events for progress.

    Args:
        schema_name: Name of the schema
        fields: JSON string of field definitions
        instructions: Optional extraction instructions
        as_list: If True, extracts List[DefinedType] to find multiple instances per document
    """
    print(f"\n[Schema SSE] Extract stream called for schema: {schema_name}", flush=True)

    if current_data["ag_corpus"] is None:
        print("[Schema SSE] Error: No data loaded", flush=True)

        async def error_generator():
            yield f"data: {json.dumps({'status': 'error', 'message': 'No data loaded. Please upload a document first.'})}\n\n"

        return StreamingResponse(error_generator(), media_type="text/event-stream")

    if not schema_name:
        print("[Schema SSE] Error: No schema name provided", flush=True)

        async def error_generator():
            yield f"data: {json.dumps({'status': 'error', 'message': 'Schema name is required'})}\n\n"

        return StreamingResponse(error_generator(), media_type="text/event-stream")

    try:
        # Parse fields JSON
        fields_list = json.loads(fields)
        if not fields_list or len(fields_list) == 0:

            async def error_generator():
                yield f"data: {json.dumps({'status': 'error', 'message': 'At least one field is required'})}\n\n"

            return StreamingResponse(error_generator(), media_type="text/event-stream")

    except json.JSONDecodeError as e:
        print(f"[Schema SSE] Error parsing fields JSON: {e}", flush=True)

        async def error_generator():
            yield f"data: {json.dumps({'status': 'error', 'message': f'Invalid fields JSON: {str(e)}'})}\n\n"

        return StreamingResponse(error_generator(), media_type="text/event-stream")

    async def event_generator():
        try:
            print(
                f"[Schema SSE] Starting event generator for schema: {schema_name}",
                flush=True,
            )
            print(f"[Schema SSE] Fields: {fields_list}", flush=True)
            print(f"[Schema SSE] Instructions: {instructions}", flush=True)

            corpus = current_data["ag_corpus"]
            if corpus is None:
                print("[Schema SSE] Error: Corpus not initialized", flush=True)
                yield f"data: {json.dumps({'status': 'error', 'message': 'Corpus not initialized'})}\n\n"
                return

            # Create dynamic Pydantic model
            print(f"[Schema SSE] Creating dynamic model: {schema_name}", flush=True)
            schema_model = create_dynamic_pydantic_model(schema_name, fields_list)
            print(f"[Schema SSE] Model created successfully", flush=True)
            print(f"[Schema SSE] Extract as list: {as_list}", flush=True)

            # Extract with progress updates
            print(f"[Schema SSE] Starting extraction", flush=True)
            async for update in extract_with_custom_schema(
                corpus,
                schema_model,
                instructions if instructions else None,
                max_results=10,
                as_list=as_list,
            ):
                print(
                    f"[Schema SSE] Sending update: {update.get('status')} - {update.get('message')}",
                    flush=True,
                )
                yield f"data: {json.dumps(update)}\n\n"
                await asyncio.sleep(0.1)

            print(f"[Schema SSE] Extraction complete", flush=True)

        except Exception as e:
            import traceback

            tb = traceback.format_exc()
            print(f"[Schema SSE] Error: {e}", flush=True)
            print(f"[Schema SSE] Traceback: {tb}", flush=True)
            yield f"data: {json.dumps({'status': 'error', 'message': str(e), 'traceback': tb})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/status")
async def get_status():
    """Get current data status."""
    return {
        "success": True,
        "data_loaded": current_data["ag_corpus"] is not None,
        "filename": current_data["filename"],
        "row_count": len(current_data["df"]) if current_data["df"] is not None else 0,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)

# Made with Bob
