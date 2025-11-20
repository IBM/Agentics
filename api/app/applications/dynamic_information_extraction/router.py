import io
import asyncio
import pandas as pd
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Body, Query

# Core Agentics Imports
from agentics import AG
from agentics.core.atype import import_pydantic_from_code

# API Internal Imports
from api.app.core.models import SessionID
from api.app.api.v1.sessions import get_session_store
from api.app.core.atype_store import load_atype_code
from api.predefined_types.Answer import Answer  # Global fallback type

APP_ID = "dynamic_information_extraction"

APP_METADATA = {
    "name": "Dynamic Information Extraction",
    "description": "Upload CSV/JSON datasets and extract structured insights using custom Pydantic models.",
    "files_needed": True,
}

router = APIRouter(tags=["App: Dynamic Info Extraction"])

# --- Helpers ---


def get_app_storage(session_id: str, store: dict) -> dict:
    """Helper to get (or create) the app-specific storage within the user's session."""
    session_data = store.get(session_id)
    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found")

    # Ensure the app dictionary exists
    if "apps" not in session_data:
        session_data["apps"] = {}
    if APP_ID not in session_data["apps"]:
        session_data["apps"][APP_ID] = {}

    return session_data["apps"][APP_ID]


# --- Endpoints ---


@router.post("/upload")
async def upload_dataset(
    session_id: SessionID,
    file: UploadFile = File(...),
    store: dict = Depends(get_session_store),
):
    """
    Uploads a CSV or JSON file and initializes an AG instance in the session.
    """
    content = await file.read()
    filename = file.filename.lower()

    try:
        dataset_ag = None

        # 1. Parse File into AG
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
            data_records = df.to_dict(orient="records")
            dataset_ag = AG()
            dataset_ag.states = data_records  # Manually hydrate states

        elif filename.endswith(".json") or filename.endswith(".jsonl"):
            # Assuming JSONL structure
            df = pd.read_json(io.BytesIO(content), lines=True)
            data_records = df.to_dict(orient="records")
            dataset_ag = AG()
            dataset_ag.states = data_records

        else:
            raise HTTPException(400, "Unsupported file type. Use .csv or .json/.jsonl")

        # 2. Store in Session
        app_store = get_app_storage(session_id, store)
        app_store["dataset"] = dataset_ag
        app_store["filename"] = file.filename

        return {
            "status": "success",
            "filename": file.filename,
            "rows_loaded": len(dataset_ag.states) if dataset_ag.states else 0,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")


@router.get("/status")
async def get_status(session_id: SessionID, store: dict = Depends(get_session_store)):
    """Checks if a dataset is currently loaded."""
    app_store = get_app_storage(session_id, store)
    dataset = app_store.get("dataset")

    return {
        "has_dataset": dataset is not None,
        "filename": app_store.get("filename"),
        "rows": len(dataset.states) if dataset and dataset.states else 0,
    }


@router.post("/run")
async def run_analysis(
    session_id: SessionID,
    question: str = Body(..., description="The analytical question to answer"),
    model_name: str = Body(
        ..., description="The name of the Pydantic model to use (without .py)"
    ),
    batch_size: int = Body(10),
    start_index: int = Body(0),
    end_index: int = Body(10),
    store: dict = Depends(get_session_store),
):
    """
    Executes the Map-Reduce (areduce) process on the uploaded dataset.
    """
    # 1. Retrieve Dataset
    app_store = get_app_storage(session_id, store)
    dataset_ag: Optional[AG] = app_store.get("dataset")

    if not dataset_ag:
        raise HTTPException(400, "No dataset found. Please upload a file first.")

    # 2. Load Target Pydantic Model
    # We look in the app's folder first, logic handled by atype_store
    code = load_atype_code(model_name, app_id=APP_ID)
    if not code:
        # Fallback: Check global types if not found in app
        code = load_atype_code(model_name, app_id=None)

    if not code:
        raise HTTPException(
            404, f"Model '{model_name}' not found in app '{APP_ID}' or global types."
        )

    try:
        target_class = import_pydantic_from_code(code)
    except Exception as e:
        raise HTTPException(500, f"Failed to compile model code: {str(e)}")

    # 3. Execute Agentics Logic
    try:
        # Configure the AG pipeline
        # "sentiment" in the original code represents the intermediate AG with results
        pipeline = AG(
            atype=target_class,
            transduction_type="areduce",
            areduce_batch_size=batch_size,
        )

        subset_states = dataset_ag.states[start_index : end_index + 1]

        # Create a temporary AG for the input data slice
        input_ag = AG()
        input_ag.states = subset_states

        # Run async transduction (Map Reduce)
        result_ag = await (pipeline << input_ag)

        # Add results to states for final aggregation (mirroring original logic)
        # "sentiment.states += sentiment.areduce_batches"
        result_ag.states += result_ag.areduce_batches

        # Add the question context
        result_ag = result_ag.add_attribute("question", default_value=question)

        # 4. Final Aggregation (Generate Answer)
        # This aggregates all the structured "MarketSentiments" into one "Answer"
        final_answer_ag = await (
            AG(atype=Answer, transduction_type="areduce") << result_ag
        )

        # 5. Format Output
        return {
            "summary": final_answer_ag[0].model_dump(),
            "intermediate_results": [
                state.model_dump() for state in result_ag.areduce_batches
            ],
        }

    except Exception as e:
        # Log stack trace in real app
        raise HTTPException(500, detail=f"Agent execution failed: {str(e)}")
