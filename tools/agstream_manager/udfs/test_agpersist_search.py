"""
Tests for AGPersist Search - Persistent Vector Search Index

Tests cover:
- Index building and persistence
- Index loading and searching
- Concurrent access
- Error handling
- Storage backend operations
"""

import json
import os
import shutil

# Import the module to test
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(__file__))

from agpersist_search import (
    BuildIndexAccumulator,
    BuildSearchIndexFunction,
    PersistentIndexMetadata,
    PersistentIndexStorage,
    get_embedder,
    get_storage,
)


class TestPersistentIndexStorage:
    """Test the storage backend."""

    @pytest.fixture
    def temp_storage(self):
        """Create a temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        storage = PersistentIndexStorage(Path(temp_dir))
        yield storage
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_storage_initialization(self, temp_storage):
        """Test storage backend initialization."""
        assert temp_storage.base_path.exists()
        assert temp_storage.base_path.is_dir()

    def test_index_directory_creation(self, temp_storage):
        """Test index directory structure."""
        index_name = "test_index"
        index_dir = temp_storage._get_index_dir(index_name)

        assert index_dir == temp_storage.base_path / index_name
        assert not index_dir.exists()  # Not created yet

    def test_file_paths(self, temp_storage):
        """Test file path generation."""
        index_name = "test_index"

        lock_file = temp_storage._get_lock_file(index_name)
        metadata_file = temp_storage._get_metadata_file(index_name)
        index_file = temp_storage._get_index_file(index_name)
        payloads_file = temp_storage._get_payloads_file(index_name)

        assert lock_file.name == ".lock"
        assert metadata_file.name == "metadata.json"
        assert index_file.name == "index.bin"
        assert payloads_file.name == "payloads.pkl"

    def test_lock_acquisition_and_release(self, temp_storage):
        """Test file locking mechanism."""
        index_name = "test_index"

        # Acquire lock
        lock_handle = temp_storage.acquire_lock(index_name, timeout=5.0)
        assert lock_handle is not None

        # Release lock
        temp_storage.release_lock(lock_handle)

    def test_concurrent_lock_timeout(self, temp_storage):
        """Test lock timeout with concurrent access."""
        index_name = "test_index"

        # Acquire first lock
        lock1 = temp_storage.acquire_lock(index_name, timeout=5.0)

        # Try to acquire second lock (should timeout)
        with pytest.raises(TimeoutError):
            temp_storage.acquire_lock(index_name, timeout=0.5)

        # Release first lock
        temp_storage.release_lock(lock1)

        # Now second lock should succeed
        lock2 = temp_storage.acquire_lock(index_name, timeout=5.0)
        temp_storage.release_lock(lock2)

    def test_index_exists(self, temp_storage):
        """Test index existence check."""
        index_name = "test_index"

        # Initially doesn't exist
        assert not temp_storage.index_exists(index_name)

        # Create directory
        index_dir = temp_storage._get_index_dir(index_name)
        index_dir.mkdir(parents=True)

        # Now exists
        assert temp_storage.index_exists(index_name)

    def test_list_indexes(self, temp_storage):
        """Test listing available indexes."""
        # Initially empty
        assert temp_storage.list_indexes() == []

        # Create some indexes
        for name in ["index1", "index2", "index3"]:
            index_dir = temp_storage._get_index_dir(name)
            index_dir.mkdir(parents=True)

        # Should list all
        indexes = temp_storage.list_indexes()
        assert len(indexes) == 3
        assert set(indexes) == {"index1", "index2", "index3"}


class TestPersistentIndexMetadata:
    """Test metadata model."""

    def test_metadata_creation(self):
        """Test creating metadata."""
        metadata = PersistentIndexMetadata(
            index_name="test_index",
            version=1,
            created_at=time.time(),
            updated_at=time.time(),
            num_items=100,
            dimension=384,
        )

        assert metadata.index_name == "test_index"
        assert metadata.version == 1
        assert metadata.num_items == 100
        assert metadata.dimension == 384
        assert metadata.model_name == "sentence-transformers/all-MiniLM-L6-v2"
        assert metadata.metric == "cosine"

    def test_metadata_serialization(self):
        """Test metadata JSON serialization."""
        metadata = PersistentIndexMetadata(
            index_name="test_index",
            version=1,
            created_at=1234567890.0,
            updated_at=1234567890.0,
            num_items=100,
            dimension=384,
        )

        # Serialize
        metadata_dict = metadata.model_dump()
        json_str = json.dumps(metadata_dict)

        # Deserialize
        loaded_dict = json.loads(json_str)
        loaded_metadata = PersistentIndexMetadata(**loaded_dict)

        assert loaded_metadata.index_name == metadata.index_name
        assert loaded_metadata.num_items == metadata.num_items


class TestBuildIndexAccumulator:
    """Test the accumulator for building indexes."""

    def test_accumulator_initialization(self):
        """Test accumulator initialization."""
        acc = BuildIndexAccumulator()

        assert acc.texts == []
        assert acc.metadata_list == []
        assert acc.index_name is None

    def test_accumulator_data_storage(self):
        """Test storing data in accumulator."""
        acc = BuildIndexAccumulator()

        acc.texts.append("text1")
        acc.texts.append("text2")
        acc.metadata_list.append({"id": 1})
        acc.metadata_list.append({"id": 2})
        acc.index_name = "test_index"

        assert len(acc.texts) == 2
        assert len(acc.metadata_list) == 2
        assert acc.index_name == "test_index"


class TestBuildSearchIndexFunction:
    """Test the UDAF for building indexes."""

    @pytest.fixture
    def build_function(self):
        """Create a build function instance."""
        return BuildSearchIndexFunction()

    def test_create_accumulator(self, build_function):
        """Test accumulator creation."""
        acc = build_function.create_accumulator()

        assert isinstance(acc, BuildIndexAccumulator)
        assert acc.texts == []
        assert acc.metadata_list == []

    def test_accumulate_simple_text(self, build_function):
        """Test accumulating simple text."""
        acc = build_function.create_accumulator()

        build_function.accumulate(acc, "Hello world", "test_index")

        assert len(acc.texts) == 1
        assert acc.texts[0] == "Hello world"
        assert acc.index_name == "test_index"

    def test_accumulate_with_metadata(self, build_function):
        """Test accumulating text with metadata."""
        acc = build_function.create_accumulator()

        # Format: text|metadata_json
        data = 'Hello world|{"id": 123, "category": "test"}'
        build_function.accumulate(acc, data, "test_index")

        assert len(acc.texts) == 1
        assert acc.texts[0] == "Hello world"
        assert acc.metadata_list[0] == {"id": 123, "category": "test"}

    def test_accumulate_multiple_rows(self, build_function):
        """Test accumulating multiple rows."""
        acc = build_function.create_accumulator()

        build_function.accumulate(acc, "Text 1", "test_index")
        build_function.accumulate(acc, "Text 2", "test_index")
        build_function.accumulate(acc, "Text 3", "test_index")

        assert len(acc.texts) == 3
        assert acc.index_name == "test_index"

    def test_merge_accumulators(self, build_function):
        """Test merging two accumulators."""
        acc1 = build_function.create_accumulator()
        acc2 = build_function.create_accumulator()

        build_function.accumulate(acc1, "Text 1", "test_index")
        build_function.accumulate(acc2, "Text 2", "test_index")

        build_function.merge(acc1, acc2)

        assert len(acc1.texts) == 2
        assert "Text 1" in acc1.texts
        assert "Text 2" in acc1.texts

    def test_get_value_empty_accumulator(self, build_function):
        """Test get_value with empty accumulator."""
        acc = build_function.create_accumulator()

        result = build_function.get_value(acc)
        result_dict = json.loads(result)

        assert result_dict["status"] == "error"
        assert result_dict["message"] == "no_data"

    @patch("agpersist_search.get_embedder")
    @patch("agpersist_search.get_storage")
    def test_get_value_success(
        self, mock_get_storage, mock_get_embedder, build_function
    ):
        """Test successful index building."""
        # Mock embedder
        mock_embedder = MagicMock()
        mock_embedder.encode.return_value = np.random.rand(3, 384).astype(np.float32)
        mock_get_embedder.return_value = mock_embedder

        # Mock storage
        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        # Create accumulator with data
        acc = build_function.create_accumulator()
        build_function.accumulate(acc, "Text 1", "test_index")
        build_function.accumulate(acc, "Text 2", "test_index")
        build_function.accumulate(acc, "Text 3", "test_index")

        # Get value (build index)
        result = build_function.get_value(acc)
        result_dict = json.loads(result)

        # Verify result
        assert result_dict["status"] == "success"
        assert result_dict["index_name"] == "test_index"
        assert result_dict["num_items"] == 3
        assert result_dict["dimension"] == 384

        # Verify storage was called
        mock_storage.save_index.assert_called_once()


class TestSearchFunctions:
    """Test search functionality."""

    @pytest.fixture
    def temp_storage_with_index(self):
        """Create storage with a test index."""
        temp_dir = tempfile.mkdtemp()
        storage = PersistentIndexStorage(Path(temp_dir))

        # Create a simple test index
        import hnswlib
        from sentence_transformers import SentenceTransformer

        # Generate test data
        texts = ["Hello world", "Good morning", "Nice day"]
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        embeddings = model.encode(
            texts, convert_to_numpy=True, normalize_embeddings=False
        )
        embeddings = embeddings.astype(np.float32)

        # Create HNSW index
        dimension = embeddings.shape[1]
        hnsw_index = hnswlib.Index(space="cosine", dim=dimension)
        hnsw_index.init_index(max_elements=len(texts), ef_construction=200, M=16)
        hnsw_index.set_ef(200)

        # Add items
        payloads = {}
        for i, (text, embedding) in enumerate(zip(texts, embeddings)):
            hnsw_index.add_items(embedding.reshape(1, -1), np.array([i]))
            payloads[i] = {"text": text, "index": i, "metadata": {}}

        # Create metadata
        metadata = PersistentIndexMetadata(
            index_name="test_index",
            version=1,
            created_at=time.time(),
            updated_at=time.time(),
            num_items=len(texts),
            dimension=dimension,
        )

        # Save index
        storage.save_index("test_index", hnsw_index, payloads, metadata)

        yield storage, temp_dir

        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_load_index(self, temp_storage_with_index):
        """Test loading a saved index."""
        storage, _ = temp_storage_with_index

        result = storage.load_index("test_index")

        assert result is not None
        hnsw_index, payloads, metadata = result

        assert metadata.index_name == "test_index"
        assert metadata.num_items == 3
        assert len(payloads) == 3

    def test_load_nonexistent_index(self, temp_storage_with_index):
        """Test loading an index that doesn't exist."""
        storage, _ = temp_storage_with_index

        result = storage.load_index("nonexistent_index")

        assert result is None

    @patch("agpersist_search.get_storage")
    @patch("agpersist_search.get_embedder")
    def test_search_persisted_index(
        self, mock_get_embedder, mock_get_storage, temp_storage_with_index
    ):
        """Test searching a persisted index."""
        from agpersist_search import search_persisted_index

        storage, _ = temp_storage_with_index
        mock_get_storage.return_value = storage

        # Mock embedder
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        mock_get_embedder.return_value = model

        # Search
        results = list(search_persisted_index("test_index", "hello", 2))

        assert len(results) > 0
        # Each result should be (text, index, score)
        for text, idx, score in results:
            assert isinstance(text, str)
            assert isinstance(idx, int)
            assert isinstance(score, float)
            assert 0.0 <= score <= 1.0


class TestIntegration:
    """Integration tests for the full workflow."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_full_workflow(self, temp_dir):
        """Test complete build and search workflow."""
        # Set storage path
        os.environ["AGSTREAM_INDEX_PATH"] = temp_dir

        # Build index
        build_func = BuildSearchIndexFunction()
        acc = build_func.create_accumulator()

        # Add test data
        test_texts = [
            "The quick brown fox jumps over the lazy dog",
            "Python is a great programming language",
            "Machine learning is fascinating",
            "Natural language processing is useful",
            "Vector search enables semantic similarity",
        ]

        for text in test_texts:
            build_func.accumulate(acc, text, "integration_test_index")

        # Build the index
        result = build_func.get_value(acc)
        result_dict = json.loads(result)

        assert result_dict["status"] == "success"
        assert result_dict["num_items"] == len(test_texts)

        # Verify index was saved
        storage = PersistentIndexStorage(Path(temp_dir))
        assert storage.index_exists("integration_test_index")

        # Load and search
        loaded = storage.load_index("integration_test_index")
        assert loaded is not None


def test_thread_local_storage():
    """Test thread-local storage for embedder and storage."""
    # Get embedder (should create new instance)
    embedder1 = get_embedder()
    assert embedder1 is not None

    # Get again (should return same instance)
    embedder2 = get_embedder()
    assert embedder1 is embedder2

    # Get storage
    storage1 = get_storage()
    assert storage1 is not None

    storage2 = get_storage()
    assert storage1 is storage2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

# Made with Bob
