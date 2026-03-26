"""
AGPersist Search - Persistent Vector Search Index for AGStream

Provides main functions:
1. build_search_index() - UDAF that builds and persists a vector search index
2. search_persisted_index() - UDTF that searches a persisted index
3. remove_search_index() - UDAF that removes a persisted index
4. list_indexes() - UDAF that lists all persisted indexes

This enables building vector indexes that persist across Flink sessions,
allowing efficient reuse of indexes without rebuilding.

Usage:
    -- Build a persistent index from reviews (default: use existing if present)
    SELECT build_search_index(customer_review, 'reviews_index') as status
    FROM pr;

    -- Build with override=true to rebuild existing index
    SELECT build_search_index(customer_review, 'reviews_index', true) as status
    FROM pr;

    -- Search the persisted index
    SELECT T.text, T.index, T.score
    FROM LATERAL TABLE(search_persisted_index('reviews_index', 'great service', 10))
    AS T(text, index, score);

    -- Remove an index
    SELECT remove_search_index('reviews_index') as status FROM (VALUES (1));

    -- List all indexes
    SELECT list_indexes() as indexes FROM (VALUES (1));
"""

import fcntl
import json
import os
import pickle
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import hnswlib
import numpy as np
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, TableFunction, udaf, udtf
from sentence_transformers import SentenceTransformer

# Load environment variables
for env_path in ["/opt/flink/.env", "../../../.env", ".env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}", file=sys.stderr)
        break


#######################
### Configuration ###
#######################


def get_index_storage_path():
    """Get the base path for storing persistent indexes."""
    # Check environment variable first
    base_path = os.getenv("AGSTREAM_INDEX_PATH")
    if base_path:
        return Path(base_path)

    # Default to /tmp/agstream_indexes or local directory
    if os.path.exists("/tmp"):
        return Path("/tmp/agstream_indexes")
    else:
        return Path("./agstream_indexes")


#######################
### Index Storage Backend ###
#######################


class PersistentIndexMetadata(BaseModel):
    """Metadata for a persistent index."""

    index_name: str
    version: int
    created_at: float
    updated_at: float
    num_items: int
    dimension: int
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    metric: str = "cosine"


class PersistentIndexStorage:
    """
    Storage backend for persistent vector indexes.

    Handles:
    - Saving/loading HNSW indexes
    - Metadata management
    - Versioning
    - Concurrent access with file locking
    """

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or get_index_storage_path()
        self.base_path.mkdir(parents=True, exist_ok=True)
        print(
            f"PersistentIndexStorage initialized at: {self.base_path}", file=sys.stderr
        )

    def _get_index_dir(self, index_name: str) -> Path:
        """Get the directory for a specific index."""
        return self.base_path / index_name

    def _get_lock_file(self, index_name: str) -> Path:
        """Get the lock file path for an index."""
        return self._get_index_dir(index_name) / ".lock"

    def _get_metadata_file(self, index_name: str) -> Path:
        """Get the metadata file path."""
        return self._get_index_dir(index_name) / "metadata.json"

    def _get_index_file(self, index_name: str) -> Path:
        """Get the HNSW index file path."""
        return self._get_index_dir(index_name) / "index.bin"

    def _get_payloads_file(self, index_name: str) -> Path:
        """Get the payloads file path."""
        return self._get_index_dir(index_name) / "payloads.pkl"

    def acquire_lock(self, index_name: str, timeout: float = 30.0):
        """
        Acquire an exclusive lock for an index.

        Returns a file handle that should be closed when done.
        """
        lock_file = self._get_lock_file(index_name)
        lock_file.parent.mkdir(parents=True, exist_ok=True)

        start_time = time.time()
        while True:
            try:
                # Open in append mode to create if doesn't exist
                fh = open(lock_file, "a")
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fh
            except (IOError, OSError):
                if time.time() - start_time > timeout:
                    raise TimeoutError(
                        f"Could not acquire lock for index '{index_name}' within {timeout}s"
                    )
                time.sleep(0.1)

    def release_lock(self, lock_handle):
        """Release a lock."""
        if lock_handle:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                lock_handle.close()
            except:
                pass

    def save_index(
        self,
        index_name: str,
        hnsw_index: hnswlib.Index,
        payloads: Dict[int, Any],
        metadata: PersistentIndexMetadata,
    ):
        """
        Save an index with metadata and payloads.

        Uses file locking to ensure safe concurrent access.
        """
        lock_handle = None
        try:
            # Acquire lock
            lock_handle = self.acquire_lock(index_name)

            # Create directory
            index_dir = self._get_index_dir(index_name)
            index_dir.mkdir(parents=True, exist_ok=True)

            # Save HNSW index
            index_file = self._get_index_file(index_name)
            hnsw_index.save_index(str(index_file))
            print(f"Saved HNSW index to {index_file}", file=sys.stderr)

            # Save payloads
            payloads_file = self._get_payloads_file(index_name)
            with open(payloads_file, "wb") as f:
                pickle.dump(payloads, f)
            print(f"Saved payloads to {payloads_file}", file=sys.stderr)

            # Save metadata
            metadata_file = self._get_metadata_file(index_name)
            with open(metadata_file, "w") as f:
                json.dump(metadata.model_dump(), f, indent=2)
            print(f"Saved metadata to {metadata_file}", file=sys.stderr)

            print(
                f"✓ Successfully saved index '{index_name}' with {metadata.num_items} items",
                file=sys.stderr,
            )

        finally:
            self.release_lock(lock_handle)

    def load_index(self, index_name: str) -> Optional[tuple]:
        """
        Load an index with its metadata and payloads.

        Returns:
            Tuple of (hnsw_index, payloads, metadata) or None if not found
        """
        lock_handle = None
        try:
            # Check if index exists
            index_dir = self._get_index_dir(index_name)
            if not index_dir.exists():
                print(f"Index '{index_name}' not found at {index_dir}", file=sys.stderr)
                return None

            # Acquire lock for reading
            lock_handle = self.acquire_lock(index_name)

            # Load metadata
            metadata_file = self._get_metadata_file(index_name)
            if not metadata_file.exists():
                print(
                    f"Metadata file not found for index '{index_name}'", file=sys.stderr
                )
                return None

            with open(metadata_file, "r") as f:
                metadata_dict = json.load(f)
                metadata = PersistentIndexMetadata(**metadata_dict)

            # Load HNSW index
            index_file = self._get_index_file(index_name)
            if not index_file.exists():
                print(f"Index file not found for index '{index_name}'", file=sys.stderr)
                return None

            # Create HNSW index and load
            space = "cosine" if metadata.metric == "cosine" else "l2"
            hnsw_index = hnswlib.Index(space=space, dim=metadata.dimension)
            hnsw_index.load_index(str(index_file), max_elements=metadata.num_items)
            hnsw_index.set_ef(200)  # Set search quality

            # Load payloads
            payloads_file = self._get_payloads_file(index_name)
            if not payloads_file.exists():
                print(
                    f"Payloads file not found for index '{index_name}'", file=sys.stderr
                )
                return None

            with open(payloads_file, "rb") as f:
                payloads = pickle.load(f)

            print(
                f"✓ Successfully loaded index '{index_name}' with {metadata.num_items} items",
                file=sys.stderr,
            )

            return (hnsw_index, payloads, metadata)

        finally:
            self.release_lock(lock_handle)

    def index_exists(self, index_name: str) -> bool:
        """Check if an index exists."""
        return self._get_index_dir(index_name).exists()

    def delete_index(self, index_name: str) -> bool:
        """
        Delete a persisted index and all its files.

        Args:
            index_name: Name of the index to delete

        Returns:
            True if deleted successfully, False if index doesn't exist
        """
        lock_handle = None
        try:
            # Check if index exists
            index_dir = self._get_index_dir(index_name)
            if not index_dir.exists():
                print(f"Index '{index_name}' does not exist", file=sys.stderr)
                return False

            # Acquire lock
            lock_handle = self.acquire_lock(index_name)

            # Delete all files in the index directory
            import shutil

            shutil.rmtree(index_dir)

            print(f"✓ Successfully deleted index '{index_name}'", file=sys.stderr)
            return True

        except Exception as e:
            print(f"Error deleting index '{index_name}': {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return False

        finally:
            self.release_lock(lock_handle)

    def list_indexes(self) -> List[str]:
        """List all available indexes."""
        if not self.base_path.exists():
            return []
        return [d.name for d in self.base_path.iterdir() if d.is_dir()]


#######################
### Thread-Local Storage ###
#######################

_thread_local = threading.local()


def get_embedder():
    """Get or create sentence transformer model using thread-local storage."""
    if not hasattr(_thread_local, "embedder"):
        model_name = "sentence-transformers/all-MiniLM-L6-v2"
        print(f"Loading embedding model: {model_name}", file=sys.stderr)
        _thread_local.embedder = SentenceTransformer(model_name)
        print(f"✓ Embedding model loaded", file=sys.stderr)
    return _thread_local.embedder


def get_storage():
    """Get or create storage backend using thread-local storage."""
    if not hasattr(_thread_local, "storage"):
        _thread_local.storage = PersistentIndexStorage()
    return _thread_local.storage


def cleanup_resources():
    """Clean up thread-local resources to prevent memory leaks."""
    if hasattr(_thread_local, "embedder"):
        try:
            # Clear the model from memory
            del _thread_local.embedder
            print("✓ Cleaned up embedder model", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Failed to cleanup embedder: {e}", file=sys.stderr)

    if hasattr(_thread_local, "storage"):
        try:
            del _thread_local.storage
            print("✓ Cleaned up storage", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Failed to cleanup storage: {e}", file=sys.stderr)

    # Force garbage collection
    import gc

    gc.collect()


#######################
### Build Index UDAF ###
#######################


class BuildIndexAccumulator:
    """Accumulator for building a persistent index."""

    def __init__(self):
        self.texts: List[str] = []
        self.metadata_list: List[Dict] = []
        self.index_name: str = None
        self.override: bool = False


class BuildSearchIndexFunction(AggregateFunction):
    """
    UDAF that builds a persistent vector search index.

    Accumulates all rows, builds a vector index, and persists it to disk.

    Usage:
        SELECT build_search_index(ROW(text_column, id_column), 'my_index') as status
        FROM my_table;
    """

    def create_accumulator(self):
        """Create a new accumulator."""
        return BuildIndexAccumulator()

    def accumulate(
        self,
        acc: BuildIndexAccumulator,
        row_data: str,
        index_name: str,
        override: bool = False,
    ):
        """
        Accumulate a row.

        Args:
            acc: The accumulator
            row_data: ROW data as string (will be parsed)
            index_name: Name for the persistent index
            override: If True, overwrite existing index; if False, use existing (default: False)
        """
        if row_data:
            # Parse the ROW data - expecting format like "text|metadata_json"
            # or just "text" if no metadata
            parts = row_data.split("|", 1)
            text = parts[0]
            metadata = json.loads(parts[1]) if len(parts) > 1 else {}

            acc.texts.append(text)
            acc.metadata_list.append(metadata)

        if acc.index_name is None:
            acc.index_name = index_name
            acc.override = override

    def get_value(self, acc: BuildIndexAccumulator) -> str:
        """
        Build and persist the index.

        Returns:
            JSON string with status information
        """
        if not acc.texts or not acc.index_name:
            return json.dumps({"status": "error", "message": "no_data"})

        try:
            # Get embedder and storage
            embedder = get_embedder()
            storage = get_storage()

            # Check if index already exists
            if storage.index_exists(acc.index_name):
                if not acc.override:
                    # Index exists and override is False - use existing index
                    print(
                        f"ℹ️  Index '{acc.index_name}' already exists. Using existing index (override=False).",
                        file=sys.stderr,
                    )
                    return json.dumps(
                        {
                            "status": "exists",
                            "message": f"Index '{acc.index_name}' already exists. Using existing index.",
                            "index_name": acc.index_name,
                        }
                    )
                else:
                    # Index exists but override is True - rebuild it
                    print(
                        f"⚠️  Index '{acc.index_name}' already exists. Overriding with new data (override=True).",
                        file=sys.stderr,
                    )

            print(
                f"Building persistent index '{acc.index_name}' with {len(acc.texts)} items",
                file=sys.stderr,
            )

            # Generate embeddings
            print(f"Generating embeddings...", file=sys.stderr)
            embeddings = embedder.encode(
                acc.texts, convert_to_numpy=True, normalize_embeddings=False
            )
            embeddings = embeddings.astype(np.float32)
            dimension = embeddings.shape[1]

            print(
                f"Generated {len(embeddings)} embeddings of dimension {dimension}",
                file=sys.stderr,
            )

            # Create HNSW index
            hnsw_index = hnswlib.Index(space="cosine", dim=dimension)
            hnsw_index.init_index(
                max_elements=len(acc.texts), ef_construction=200, M=16
            )
            hnsw_index.set_ef(200)

            # Add items to index
            payloads = {}
            for i, (text, metadata, embedding) in enumerate(
                zip(acc.texts, acc.metadata_list, embeddings)
            ):
                hnsw_index.add_items(embedding.reshape(1, -1), np.array([i]))
                payloads[i] = {"text": text, "index": i, "metadata": metadata}

            print(f"Added {len(payloads)} items to HNSW index", file=sys.stderr)

            # Create metadata
            metadata = PersistentIndexMetadata(
                index_name=acc.index_name,
                version=1,
                created_at=time.time(),
                updated_at=time.time(),
                num_items=len(acc.texts),
                dimension=dimension,
                model_name="sentence-transformers/all-MiniLM-L6-v2",
                metric="cosine",
            )

            # Save index
            storage.save_index(acc.index_name, hnsw_index, payloads, metadata)

            return json.dumps(
                {
                    "status": "success",
                    "index_name": acc.index_name,
                    "num_items": len(acc.texts),
                    "dimension": dimension,
                }
            )

        except Exception as e:
            print(f"Error building index: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return json.dumps({"status": "error", "message": str(e)})
        finally:
            # Clean up resources after building index
            cleanup_resources()

    def merge(self, acc1: BuildIndexAccumulator, acc2: BuildIndexAccumulator):
        """Merge two accumulators."""
        acc1.texts.extend(acc2.texts)
        acc1.metadata_list.extend(acc2.metadata_list)
        if acc1.index_name is None:
            acc1.index_name = acc2.index_name
            acc1.override = acc2.override

    def get_result_type(self):
        """Return the result type."""
        return DataTypes.STRING()

    def get_accumulator_type(self):
        """Return the accumulator type."""
        return DataTypes.ROW(
            [
                DataTypes.FIELD("texts", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD("metadata_list", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD("index_name", DataTypes.STRING()),
                DataTypes.FIELD("override", DataTypes.BOOLEAN()),
            ]
        )


# Register the UDAF
build_search_index = udaf(
    f=BuildSearchIndexFunction(),
    result_type=DataTypes.STRING(),
    accumulator_type=DataTypes.ROW(
        [
            DataTypes.FIELD("texts", DataTypes.ARRAY(DataTypes.STRING())),
            DataTypes.FIELD("metadata_list", DataTypes.ARRAY(DataTypes.STRING())),
            DataTypes.FIELD("index_name", DataTypes.STRING()),
            DataTypes.FIELD("override", DataTypes.BOOLEAN()),
        ]
    ),
    name="build_search_index",
)


#######################
### Search Index UDTF ###
#######################


@udtf(result_types=[DataTypes.STRING(), DataTypes.INT(), DataTypes.DOUBLE()])
def search_persisted_index(index_name, query, k=10):
    """
    UDTF that searches a persisted vector index.

    Loads the index from disk and performs vector search.

    Args:
        index_name: Name of the persisted index
        query: Search query string
        k: Number of results to return (default: 10)

    Yields:
        Tuple of (text, index, score) for each result

    Usage:
        SELECT T.text, T.index, T.score
        FROM LATERAL TABLE(search_persisted_index('my_index', 'search query', 10))
        AS T(text, index, score);
    """
    if not index_name or not query:
        return

    try:
        # Get storage and embedder
        storage = get_storage()
        embedder = get_embedder()

        # Load index
        print(f"Loading index '{index_name}'...", file=sys.stderr)
        result = storage.load_index(index_name)

        if result is None:
            print(f"Index '{index_name}' not found", file=sys.stderr)
            yield ("error: index not found", -1, 0.0)
            return

        hnsw_index, payloads, metadata = result

        # Generate query embedding
        query_embedding = embedder.encode(
            [query], convert_to_numpy=True, normalize_embeddings=False
        )
        query_embedding = query_embedding.astype(np.float32)

        # Search
        k = min(k, metadata.num_items)  # Don't request more than available
        labels, distances = hnsw_index.knn_query(query_embedding, k=k)

        # Convert distances to similarity scores (for cosine: similarity = 1 - distance)
        similarities = 1.0 - distances[0]

        # Yield results
        for i, (label, similarity) in enumerate(zip(labels[0], similarities)):
            payload = payloads[int(label)]
            yield (payload["text"], payload["index"], float(similarity))

        print(f"✓ Search completed, returned {len(labels[0])} results", file=sys.stderr)

    except Exception as e:
        print(f"Error searching index: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        yield ("error: " + str(e), -1, 0.0)
    finally:
        # Clean up resources after searching
        cleanup_resources()


#######################
### JSON-Returning Search Function (Compatible with explode_search_results) ###
#######################


class SearchIndexJsonAccumulator:
    """Accumulator for search_index_json function."""

    def __init__(self):
        self.index_name = None
        self.query = None
        self.k = 10


class SearchIndexJsonFunction(AggregateFunction):
    """
    UDAF that searches a persisted index and returns JSON (compatible with explode_search_results).

    This is a workaround for UDTF issues - returns same format as agsearch.

    Usage:
        SELECT text, idx
        FROM (
            SELECT search_index_json('my_index', 'query', 10) as results FROM (VALUES (1))
        ),
        LATERAL TABLE(explode_search_results(results)) AS T(text, idx);
    """

    def create_accumulator(self):
        return SearchIndexJsonAccumulator()

    def accumulate(self, acc, index_name, query, k=10):
        if acc.index_name is None:
            acc.index_name = index_name
            acc.query = query
            acc.k = k if k and k > 0 else 10

    def get_value(self, acc):
        if not acc.index_name or not acc.query:
            return json.dumps([])

        try:
            # Get storage and embedder
            storage = get_storage()
            embedder = get_embedder()

            # Load index
            print(f"Loading index '{acc.index_name}'...", file=sys.stderr)
            result = storage.load_index(acc.index_name)

            if result is None:
                print(f"Index '{acc.index_name}' not found", file=sys.stderr)
                return json.dumps([{"text": "error: index not found", "index": -1}])

            hnsw_index, payloads, metadata = result

            # Generate query embedding
            query_embedding = embedder.encode(
                [acc.query], convert_to_numpy=True, normalize_embeddings=False
            )
            query_embedding = query_embedding.astype(np.float32)

            # Search
            k = min(acc.k, metadata.num_items)
            labels, distances = hnsw_index.knn_query(query_embedding, k=k)

            # Format results as JSON (same format as agsearch)
            results = []
            for label in labels[0]:
                payload = payloads[int(label)]
                results.append({"text": payload["text"], "index": payload["index"]})

            print(
                f"✓ Search completed, returned {len(results)} results", file=sys.stderr
            )
            return json.dumps(results)

        except Exception as e:
            print(f"Error searching index: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return json.dumps([{"text": f"error: {str(e)}", "index": -1}])
        finally:
            # Clean up resources after searching
            cleanup_resources()

    def merge(self, acc1, acc2):
        if acc1.index_name is None:
            acc1.index_name = acc2.index_name
            acc1.query = acc2.query
            acc1.k = acc2.k

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [
                DataTypes.FIELD("index_name", DataTypes.STRING()),
                DataTypes.FIELD("query", DataTypes.STRING()),
                DataTypes.FIELD("k", DataTypes.INT()),
            ]
        )


# Register the UDAF
search_index_json = udaf(
    f=SearchIndexJsonFunction(),
    result_type=DataTypes.STRING(),
    accumulator_type=DataTypes.ROW(
        [
            DataTypes.FIELD("index_name", DataTypes.STRING()),
            DataTypes.FIELD("query", DataTypes.STRING()),
            DataTypes.FIELD("k", DataTypes.INT()),
        ]
    ),
    name="search_index_json",
)


#######################
### List Indexes UDTF ###
#######################


@udtf(result_types=[DataTypes.STRING()])
def list_search_indexes():
    """
    UDTF that lists all persisted vector search indexes.

    Returns a single column with index names.

    Yields:
        String containing the index name

    Usage:
        SELECT T.indexes
        FROM LATERAL TABLE(list_search_indexes())
        AS T(indexes);

    Example:
        -- List all available indexes
        SELECT indexes
        FROM LATERAL TABLE(list_search_indexes())
        AS T(indexes)
        ORDER BY indexes;
    """
    try:
        # Get storage backend
        storage = get_storage()

        # List all indexes
        index_names = storage.list_indexes()

        print(f"Found {len(index_names)} persisted indexes", file=sys.stderr)

        # Yield each index name
        for index_name in index_names:
            yield (index_name,)

        print(f"✓ Listed {len(index_names)} indexes", file=sys.stderr)

    except Exception as e:
        print(f"Error listing indexes: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
    finally:
        # Clean up resources after listing indexes
        cleanup_resources()


#######################
### List Indexes UDAF (Simpler Syntax) ###
#######################


class ListIndexesAccumulator:
    """Accumulator for list_indexes function."""

    def __init__(self):
        self.called = False


class ListIndexesFunction(AggregateFunction):
    """
    UDAF that returns all persisted indexes as a comma-separated string.

    Simpler syntax than the UDTF version - no need for LATERAL TABLE.

    Usage:
        SELECT list_indexes() as indexes FROM (VALUES (1));

    Returns:
        Comma-separated string of index names, e.g., "index1,index2,index3"
        Returns empty string if no indexes exist.
    """

    def create_accumulator(self):
        return ListIndexesAccumulator()

    def accumulate(self, acc):
        acc.called = True

    def get_value(self, acc):
        if not acc.called:
            return ""

        try:
            # Get storage backend
            storage = get_storage()

            # List all indexes
            index_names = storage.list_indexes()

            print(f"Found {len(index_names)} persisted indexes", file=sys.stderr)

            # Return as comma-separated string
            result = ",".join(sorted(index_names))

            print(f"✓ Returning indexes: {result}", file=sys.stderr)
            return result

        except Exception as e:
            print(f"Error listing indexes: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return f"error: {str(e)}"
        finally:
            # Clean up resources after listing indexes
            cleanup_resources()

    def merge(self, acc1, acc2):
        if not acc1.called:
            acc1.called = acc2.called

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ROW([DataTypes.FIELD("called", DataTypes.BOOLEAN())])


# Register the UDAF
list_indexes = udaf(
    f=ListIndexesFunction(),
    result_type=DataTypes.STRING(),
    accumulator_type=DataTypes.ROW([DataTypes.FIELD("called", DataTypes.BOOLEAN())]),
    name="list_indexes",
)


#######################
### Remove Index UDAF ###
#######################


class RemoveIndexAccumulator:
    """Accumulator for remove_search_index function."""

    def __init__(self):
        self.index_name = None


class RemoveSearchIndexFunction(AggregateFunction):
    """
    UDAF that removes a persisted vector search index.

    Deletes the index and all associated files from disk.

    Usage:
        SELECT remove_search_index('my_index') as status FROM (VALUES (1));

    Returns:
        JSON string with status information:
        - {"status": "success", "index_name": "...", "message": "..."}
        - {"status": "not_found", "index_name": "...", "message": "..."}
        - {"status": "error", "message": "..."}
    """

    def create_accumulator(self):
        return RemoveIndexAccumulator()

    def accumulate(self, acc, index_name):
        """
        Accumulate function for remove_search_index.

        Args:
            acc: Accumulator
            index_name: Name of the index to remove
        """
        if acc.index_name is None:
            acc.index_name = index_name

    def get_value(self, acc):
        if not acc.index_name:
            return json.dumps({"status": "error", "message": "No index name provided"})

        try:
            # Get storage backend
            storage = get_storage()

            # Check if index exists
            if not storage.index_exists(acc.index_name):
                print(f"Index '{acc.index_name}' does not exist", file=sys.stderr)
                return json.dumps(
                    {
                        "status": "not_found",
                        "index_name": acc.index_name,
                        "message": f"Index '{acc.index_name}' does not exist",
                    }
                )

            # Delete the index
            print(f"Deleting index '{acc.index_name}'...", file=sys.stderr)
            success = storage.delete_index(acc.index_name)

            if success:
                return json.dumps(
                    {
                        "status": "success",
                        "index_name": acc.index_name,
                        "message": f"Index '{acc.index_name}' deleted successfully",
                    }
                )
            else:
                return json.dumps(
                    {
                        "status": "error",
                        "index_name": acc.index_name,
                        "message": f"Failed to delete index '{acc.index_name}'",
                    }
                )

        except Exception as e:
            print(f"Error removing index: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return json.dumps({"status": "error", "message": str(e)})
        finally:
            # Clean up resources after removing index
            cleanup_resources()

    def merge(self, acc1, acc2):
        if acc1.index_name is None:
            acc1.index_name = acc2.index_name

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ROW([DataTypes.FIELD("index_name", DataTypes.STRING())])


# Register the UDAF
remove_search_index = udaf(
    f=RemoveSearchIndexFunction(),
    result_type=DataTypes.STRING(),
    accumulator_type=DataTypes.ROW([DataTypes.FIELD("index_name", DataTypes.STRING())]),
    name="remove_search_index",
)


# Made with Bob
