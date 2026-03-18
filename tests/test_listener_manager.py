"""
Test Suite for ListenerManager

This module tests the listener management functionality of AGStream,
including starting, stopping, querying, and managing listener threads.
"""

import queue
import threading
import time
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from agentics.core.streaming.listener_manager import ListenerInfo, ListenerManager

# ---------------------------------------------------------------------------
# Mock AGStream for testing
# ---------------------------------------------------------------------------


class MockAGStream:
    """Mock AGStream for testing without Kafka infrastructure."""

    def __init__(
        self,
        input_topic: str = "test-input",
        output_topic: str = "test-output",
        source_atype_name: Optional[str] = None,
        target_atype_name: Optional[str] = None,
        instructions: str = "",
        **kwargs,
    ):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.source_atype_name = source_atype_name
        self.target_atype_name = target_atype_name
        self.instructions = instructions
        self.kwargs = kwargs

    def model_copy(self, update: Dict[str, Any]) -> "MockAGStream":
        """Mock model_copy for Pydantic-like behavior."""
        new_instance = MockAGStream(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
            source_atype_name=self.source_atype_name,
            target_atype_name=self.target_atype_name,
            instructions=self.instructions,
            **self.kwargs,
        )
        for key, value in update.items():
            setattr(new_instance, key, value)
        return new_instance

    def transducible_function_listener(
        self,
        fn: Any,
        group_id: str,
        validate_schema: bool = True,
        produce_results: bool = True,
        verbose: bool = True,
        stop_event: Optional[threading.Event] = None,
        log_queue: Optional[queue.Queue] = None,
    ) -> None:
        """Mock transducible function listener."""
        if log_queue:
            log_queue.put_nowait("Mock transducible listener started\n")

        # Simulate processing
        while stop_event and not stop_event.is_set():
            time.sleep(0.1)

        if log_queue:
            log_queue.put_nowait("Mock transducible listener stopped\n")

    def listen(
        self,
        source_atype_name: Optional[str] = None,
        group_id: str = "test-group",
        validate_schema: bool = True,
        produce_results: bool = True,
        verbose: bool = True,
        stop_event: Optional[threading.Event] = None,
        log_queue: Optional[queue.Queue] = None,
    ) -> None:
        """Mock simple listener."""
        if log_queue:
            log_queue.put_nowait("Mock simple listener started\n")

        # Simulate processing
        while stop_event and not stop_event.is_set():
            time.sleep(0.1)

        if log_queue:
            log_queue.put_nowait("Mock simple listener stopped\n")


def mock_agstream_factory(
    input_topic: str,
    output_topic: str,
    source_atype_name: Optional[str] = None,
    target_atype_name: Optional[str] = None,
    **kwargs,
) -> MockAGStream:
    """Factory function for creating mock AGStream instances."""
    return MockAGStream(
        input_topic=input_topic,
        output_topic=output_topic,
        source_atype_name=source_atype_name,
        target_atype_name=target_atype_name,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Test Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def listener_manager():
    """Create a ListenerManager with mock factory."""
    return ListenerManager(agstream_factory=mock_agstream_factory)


@pytest.fixture
def sample_function():
    """Sample transducible function for testing."""

    def process_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Sample processing function."""
        return {"processed": True, **data}

    return process_data


# ---------------------------------------------------------------------------
# Test ListenerInfo
# ---------------------------------------------------------------------------


def test_listener_info_creation():
    """Test ListenerInfo dataclass creation."""
    info = ListenerInfo(
        listener_id="test-1",
        name="Test Listener",
        input_topic="input",
        output_topic="output",
        source_atype_name="Source-value",
        target_atype_name="Target-value",
        fn_name="test_fn",
    )

    assert info.listener_id == "test-1"
    assert info.name == "Test Listener"
    assert info.input_topic == "input"
    assert info.output_topic == "output"
    assert info.source_atype_name == "Source-value"
    assert info.target_atype_name == "Target-value"
    assert info.fn_name == "test_fn"
    assert not info.stopped
    assert info.error is None
    assert not info.is_alive


def test_listener_info_status():
    """Test ListenerInfo status property."""
    info = ListenerInfo(
        listener_id="test-1",
        name="Test",
        input_topic="in",
        output_topic="out",
        source_atype_name=None,
        target_atype_name=None,
        fn_name="fn",
    )

    # Initial status
    assert info.status == "idle"

    # Stopped status
    info.stopped = True
    assert info.status == "stopped"

    # Error status
    info.error = "Test error"
    assert info.status == "error: Test error"


# ---------------------------------------------------------------------------
# Test ListenerManager Initialization
# ---------------------------------------------------------------------------


def test_listener_manager_init_with_factory():
    """Test ListenerManager initialization with factory."""
    mgr = ListenerManager(agstream_factory=mock_agstream_factory)
    assert mgr._factory is not None
    assert len(mgr._listeners) == 0
    assert mgr._counter == 0


def test_listener_manager_init_without_factory():
    """Test ListenerManager initialization without factory."""
    mgr = ListenerManager()
    assert mgr._factory is None
    assert len(mgr._listeners) == 0


def test_listener_manager_repr(listener_manager):
    """Test ListenerManager string representation."""
    repr_str = repr(listener_manager)
    assert "ListenerManager" in repr_str
    assert "running=0/0" in repr_str


# ---------------------------------------------------------------------------
# Test Starting Listeners
# ---------------------------------------------------------------------------


def test_start_transducible_listener(listener_manager, sample_function):
    """Test starting a transducible function listener."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
        source_atype_name="TestInput-value",
        target_atype_name="TestOutput-value",
    )

    assert listener_id is not None
    assert listener_id.startswith("listener-")

    # Check listener info
    info = listener_manager.get_info(listener_id)
    assert info is not None
    assert info.input_topic == "test-input"
    assert info.output_topic == "test-output"
    assert info.source_atype_name == "TestInput-value"
    assert info.target_atype_name == "TestOutput-value"
    assert info.fn_name == "process_data"

    # Wait for thread to start
    time.sleep(0.2)
    assert info.is_alive

    # Clean up
    listener_manager.stop(listener_id)


def test_start_simple_listener(listener_manager):
    """Test starting a simple LLM-based listener."""
    listener_id = listener_manager.start_simple_listener(
        input_topic="test-input",
        output_topic="test-output",
        source_atype_name="TestInput-value",
        target_atype_name="TestOutput-value",
        instructions="Process the data",
    )

    assert listener_id is not None
    assert listener_id.startswith("listener-")

    # Check listener info
    info = listener_manager.get_info(listener_id)
    assert info is not None
    assert info.input_topic == "test-input"
    assert info.output_topic == "test-output"
    assert info.fn_name == "listen"

    # Wait for thread to start
    time.sleep(0.2)
    assert info.is_alive

    # Clean up
    listener_manager.stop(listener_id)


def test_start_with_custom_name(listener_manager, sample_function):
    """Test starting a listener with custom name."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
        name="My Custom Listener",
    )

    info = listener_manager.get_info(listener_id)
    assert info.name == "My Custom Listener"

    listener_manager.stop(listener_id)


def test_start_with_agstream_instance(sample_function):
    """Test starting a listener with pre-built AGStream instance."""
    mgr = ListenerManager()  # No factory
    mock_ag = MockAGStream(input_topic="test-in", output_topic="test-out")

    listener_id = mgr.start(
        fn=sample_function,
        input_topic="test-in",
        output_topic="test-out",
        agstream=mock_ag,
    )

    assert listener_id is not None
    info = mgr.get_info(listener_id)
    assert info is not None

    mgr.stop(listener_id)


def test_start_without_factory_or_agstream(sample_function):
    """Test that starting without factory or agstream raises error."""
    mgr = ListenerManager()  # No factory

    with pytest.raises(ValueError, match="No AGStream instance provided"):
        mgr.start(
            fn=sample_function,
            input_topic="test-in",
            output_topic="test-out",
        )


# ---------------------------------------------------------------------------
# Test Stopping Listeners
# ---------------------------------------------------------------------------


def test_stop_listener(listener_manager, sample_function):
    """Test stopping a running listener."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Wait for thread to start
    time.sleep(0.2)
    info = listener_manager.get_info(listener_id)
    assert info.is_alive

    # Stop the listener
    success = listener_manager.stop(listener_id, timeout=2.0)
    assert success

    # Check it's stopped
    time.sleep(0.2)
    assert not info.is_alive
    assert info.stopped


def test_stop_nonexistent_listener(listener_manager):
    """Test stopping a listener that doesn't exist."""
    success = listener_manager.stop("nonexistent-id")
    assert not success


def test_stop_all_listeners(listener_manager, sample_function):
    """Test stopping all running listeners."""
    # Start multiple listeners
    id1 = listener_manager.start(
        fn=sample_function,
        input_topic="topic1",
        output_topic="output1",
    )
    id2 = listener_manager.start(
        fn=sample_function,
        input_topic="topic2",
        output_topic="output2",
    )

    # Wait for threads to start
    time.sleep(0.2)
    assert len(listener_manager.running_ids()) == 2

    # Stop all
    listener_manager.stop_all(timeout=2.0)

    # Check all stopped
    time.sleep(0.2)
    assert len(listener_manager.running_ids()) == 0


# ---------------------------------------------------------------------------
# Test Log Draining
# ---------------------------------------------------------------------------


def test_drain_logs(listener_manager, sample_function):
    """Test draining logs from a listener."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
        verbose=True,
    )

    # Wait for some logs
    time.sleep(0.3)

    # Drain logs
    logs = listener_manager.drain_logs(listener_id)
    assert isinstance(logs, list)
    assert len(logs) > 0
    assert any("started" in log.lower() for log in logs)

    # Clean up
    listener_manager.stop(listener_id)


def test_drain_logs_nonexistent_listener(listener_manager):
    """Test draining logs from nonexistent listener."""
    logs = listener_manager.drain_logs("nonexistent-id")
    assert logs == []


def test_drain_logs_max_lines(listener_manager, sample_function):
    """Test draining logs with max_lines limit."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Add many log entries
    info = listener_manager.get_info(listener_id)
    for i in range(50):
        info.log_queue.put_nowait(f"Log line {i}\n")

    # Drain with limit
    logs = listener_manager.drain_logs(listener_id, max_lines=10)
    assert len(logs) <= 10

    # Clean up
    listener_manager.stop(listener_id)


# ---------------------------------------------------------------------------
# Test Status Queries
# ---------------------------------------------------------------------------


def test_get_info(listener_manager, sample_function):
    """Test getting listener info."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    info = listener_manager.get_info(listener_id)
    assert info is not None
    assert info.listener_id == listener_id
    assert isinstance(info, ListenerInfo)

    listener_manager.stop(listener_id)


def test_get_info_nonexistent(listener_manager):
    """Test getting info for nonexistent listener."""
    info = listener_manager.get_info("nonexistent-id")
    assert info is None


def test_list_listeners(listener_manager, sample_function):
    """Test listing all listeners."""
    # Start multiple listeners
    id1 = listener_manager.start(
        fn=sample_function,
        input_topic="topic1",
        output_topic="output1",
    )
    id2 = listener_manager.start(
        fn=sample_function,
        input_topic="topic2",
        output_topic="output2",
    )

    listeners = listener_manager.list_listeners()
    assert len(listeners) == 2
    assert all(isinstance(info, ListenerInfo) for info in listeners)

    # Clean up
    listener_manager.stop_all()


def test_running_ids(listener_manager, sample_function):
    """Test getting running listener IDs."""
    # Start listeners
    id1 = listener_manager.start(
        fn=sample_function,
        input_topic="topic1",
        output_topic="output1",
    )
    id2 = listener_manager.start(
        fn=sample_function,
        input_topic="topic2",
        output_topic="output2",
    )

    # Wait for threads to start
    time.sleep(0.2)

    running = listener_manager.running_ids()
    assert len(running) == 2
    assert id1 in running
    assert id2 in running

    # Stop one
    listener_manager.stop(id1)
    time.sleep(0.2)

    running = listener_manager.running_ids()
    assert len(running) == 1
    assert id2 in running

    # Clean up
    listener_manager.stop_all()


def test_has_listener_for(listener_manager, sample_function):
    """Test checking if listener exists for topic."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-topic",
        output_topic="output-topic",
    )

    # Wait for thread to start
    time.sleep(0.2)

    assert listener_manager.has_listener_for("test-topic")
    assert not listener_manager.has_listener_for("other-topic")

    # Clean up
    listener_manager.stop(listener_id)


# ---------------------------------------------------------------------------
# Test Remove
# ---------------------------------------------------------------------------


def test_remove_stopped_listener(listener_manager, sample_function):
    """Test removing a stopped listener."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Stop the listener
    listener_manager.stop(listener_id)
    time.sleep(0.2)

    # Remove it
    success = listener_manager.remove(listener_id)
    assert success

    # Verify it's gone
    info = listener_manager.get_info(listener_id)
    assert info is None


def test_remove_running_listener(listener_manager, sample_function):
    """Test that removing a running listener fails."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Wait for thread to start
    time.sleep(0.2)

    # Try to remove while running
    success = listener_manager.remove(listener_id)
    assert not success

    # Clean up
    listener_manager.stop(listener_id)


def test_remove_nonexistent_listener(listener_manager):
    """Test removing a nonexistent listener."""
    success = listener_manager.remove("nonexistent-id")
    assert not success


# ---------------------------------------------------------------------------
# Test Restart
# ---------------------------------------------------------------------------


def test_restart_transducible_listener(listener_manager, sample_function):
    """Test restarting a transducible listener."""
    # Start and stop a listener
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
        name="Test Listener",
    )
    time.sleep(0.2)
    listener_manager.stop(listener_id)
    time.sleep(0.2)

    # Restart it
    new_id = listener_manager.restart(listener_id)
    assert new_id is not None
    assert new_id != listener_id  # Should get a new ID

    # Verify new listener is running
    time.sleep(0.2)
    info = listener_manager.get_info(new_id)
    assert info is not None
    assert info.is_alive
    assert info.input_topic == "test-input"

    # Clean up
    listener_manager.stop(new_id)


def test_restart_simple_listener(listener_manager):
    """Test restarting a simple listener."""
    # Start and stop a simple listener
    listener_id = listener_manager.start_simple_listener(
        input_topic="test-input",
        output_topic="test-output",
        instructions="Process data",
    )
    time.sleep(0.2)
    listener_manager.stop(listener_id)
    time.sleep(0.2)

    # Restart it
    new_id = listener_manager.restart(listener_id)
    assert new_id is not None

    # Verify new listener is running
    time.sleep(0.2)
    info = listener_manager.get_info(new_id)
    assert info is not None
    assert info.is_alive

    # Clean up
    listener_manager.stop(new_id)


def test_restart_running_listener(listener_manager, sample_function):
    """Test that restarting a running listener fails."""
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Wait for thread to start
    time.sleep(0.2)

    # Try to restart while running
    new_id = listener_manager.restart(listener_id)
    assert new_id is None

    # Clean up
    listener_manager.stop(listener_id)


def test_restart_nonexistent_listener(listener_manager):
    """Test restarting a nonexistent listener."""
    new_id = listener_manager.restart("nonexistent-id")
    assert new_id is None


# ---------------------------------------------------------------------------
# Test Thread Safety
# ---------------------------------------------------------------------------


def test_concurrent_starts(listener_manager, sample_function):
    """Test starting multiple listeners concurrently."""
    import concurrent.futures

    def start_listener(i):
        return listener_manager.start(
            fn=sample_function,
            input_topic=f"topic-{i}",
            output_topic=f"output-{i}",
        )

    # Start 10 listeners concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(start_listener, i) for i in range(10)]
        listener_ids = [f.result() for f in futures]

    # Verify all started
    assert len(listener_ids) == 10
    assert len(set(listener_ids)) == 10  # All unique

    # Clean up
    listener_manager.stop_all()


# ---------------------------------------------------------------------------
# Test Error Handling
# ---------------------------------------------------------------------------


def test_listener_error_handling(listener_manager):
    """Test that listener errors are captured."""

    def failing_function(data):
        raise ValueError("Test error")

    # Create a mock AGStream that will raise an error
    def error_factory(*args, **kwargs):
        mock_ag = MockAGStream(*args, **kwargs)

        def error_listener(*args, **kwargs):
            log_queue = kwargs.get("log_queue")
            if log_queue:
                log_queue.put_nowait("Starting...\n")
            raise ValueError("Simulated error")

        mock_ag.transducible_function_listener = error_listener
        return mock_ag

    mgr = ListenerManager(agstream_factory=error_factory)

    listener_id = mgr.start(
        fn=failing_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Wait for error to occur
    time.sleep(0.3)

    info = mgr.get_info(listener_id)
    assert info is not None
    assert info.error is not None
    assert "Simulated error" in info.error
    assert info.stopped


# ---------------------------------------------------------------------------
# Test ID Generation
# ---------------------------------------------------------------------------


def test_listener_id_generation(listener_manager, sample_function):
    """Test that listener IDs are generated sequentially."""
    id1 = listener_manager.start(
        fn=sample_function,
        input_topic="topic1",
        output_topic="output1",
    )
    id2 = listener_manager.start(
        fn=sample_function,
        input_topic="topic2",
        output_topic="output2",
    )

    assert id1 == "listener-1"
    assert id2 == "listener-2"

    # Clean up
    listener_manager.stop_all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

# Made with Bob
