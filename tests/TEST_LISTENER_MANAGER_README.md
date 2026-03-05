# ListenerManager Test Suite

## Overview

This test suite provides comprehensive coverage of the `ListenerManager` class, which manages Kafka listener threads for AGStream pipelines. The tests verify all listener management functions without requiring actual Kafka infrastructure by using mock objects.

## Test File

- **Location**: `tests/test_listener_manager.py`
- **Total Tests**: 31
- **Test Duration**: ~28 seconds
- **Status**: ✅ All tests passing

## Test Coverage

### 1. ListenerInfo Tests (2 tests)
- `test_listener_info_creation`: Verifies ListenerInfo dataclass creation and attributes
- `test_listener_info_status`: Tests the status property (idle, running, stopped, error)

### 2. Initialization Tests (3 tests)
- `test_listener_manager_init_with_factory`: Tests initialization with AGStream factory
- `test_listener_manager_init_without_factory`: Tests initialization without factory
- `test_listener_manager_repr`: Tests string representation

### 3. Starting Listeners (5 tests)
- `test_start_transducible_listener`: Tests starting a transducible function listener
- `test_start_simple_listener`: Tests starting a simple LLM-based listener
- `test_start_with_custom_name`: Tests starting with custom display name
- `test_start_with_agstream_instance`: Tests starting with pre-built AGStream instance
- `test_start_without_factory_or_agstream`: Tests error handling when neither factory nor instance provided

### 4. Stopping Listeners (3 tests)
- `test_stop_listener`: Tests stopping a running listener
- `test_stop_nonexistent_listener`: Tests stopping non-existent listener
- `test_stop_all_listeners`: Tests stopping all running listeners at once

### 5. Log Draining (3 tests)
- `test_drain_logs`: Tests draining logs from a listener's queue
- `test_drain_logs_nonexistent_listener`: Tests draining logs from non-existent listener
- `test_drain_logs_max_lines`: Tests log draining with max_lines limit

### 6. Status Queries (5 tests)
- `test_get_info`: Tests retrieving listener information
- `test_get_info_nonexistent`: Tests getting info for non-existent listener
- `test_list_listeners`: Tests listing all registered listeners
- `test_running_ids`: Tests getting IDs of running listeners
- `test_has_listener_for`: Tests checking if listener exists for a topic

### 7. Remove Listeners (3 tests)
- `test_remove_stopped_listener`: Tests removing a stopped listener
- `test_remove_running_listener`: Tests that removing running listener fails
- `test_remove_nonexistent_listener`: Tests removing non-existent listener

### 8. Restart Listeners (4 tests)
- `test_restart_transducible_listener`: Tests restarting a transducible listener
- `test_restart_simple_listener`: Tests restarting a simple listener
- `test_restart_running_listener`: Tests that restarting running listener fails
- `test_restart_nonexistent_listener`: Tests restarting non-existent listener

### 9. Thread Safety (1 test)
- `test_concurrent_starts`: Tests starting multiple listeners concurrently

### 10. Error Handling (1 test)
- `test_listener_error_handling`: Tests that listener errors are captured properly

### 11. ID Generation (1 test)
- `test_listener_id_generation`: Tests sequential listener ID generation

## Mock Infrastructure

The test suite uses mock objects to simulate AGStream behavior without requiring Kafka:

### MockAGStream
A lightweight mock that simulates AGStream's listener methods:
- `transducible_function_listener()`: Simulates processing with a transducible function
- `listen()`: Simulates simple LLM-based listening
- `model_copy()`: Simulates Pydantic model copying

### mock_agstream_factory
Factory function that creates MockAGStream instances, matching the signature expected by ListenerManager.

## Running the Tests

### Run all ListenerManager tests:
```bash
uv run pytest tests/test_listener_manager.py -v
```

### Run specific test:
```bash
uv run pytest tests/test_listener_manager.py::test_start_transducible_listener -v
```

### Run with coverage:
```bash
uv run pytest tests/test_listener_manager.py --cov=agentics.core.listener_manager --cov-report=html
```

## Key Features Tested

### ✅ Listener Lifecycle
- Starting transducible and simple listeners
- Stopping individual and all listeners
- Restarting stopped listeners
- Removing stopped listeners from registry

### ✅ Thread Management
- Thread creation and daemon mode
- Stop event signaling
- Thread join with timeout
- Concurrent listener starts

### ✅ Log Management
- Log queue creation and population
- Log draining with limits
- Log persistence across listener lifecycle

### ✅ Status Tracking
- Listener state (idle, running, stopped, error)
- Thread alive status
- Error capture and reporting
- Started timestamp tracking

### ✅ Query Operations
- Get listener info by ID
- List all listeners
- Get running listener IDs
- Check if listener exists for topic

### ✅ Error Handling
- Missing factory or AGStream instance
- Listener exceptions during execution
- Invalid operations (restart running, remove running)
- Non-existent listener operations

## Test Patterns

### Fixture Usage
```python
@pytest.fixture
def listener_manager():
    """Create a ListenerManager with mock factory."""
    return ListenerManager(agstream_factory=mock_agstream_factory)

@pytest.fixture
def sample_function():
    """Sample transducible function for testing."""
    def process_data(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"processed": True, **data}
    return process_data
```

### Typical Test Structure
```python
def test_start_transducible_listener(listener_manager, sample_function):
    # Start listener
    listener_id = listener_manager.start(
        fn=sample_function,
        input_topic="test-input",
        output_topic="test-output",
    )

    # Verify creation
    assert listener_id is not None

    # Wait for thread to start
    time.sleep(0.2)

    # Verify running
    info = listener_manager.get_info(listener_id)
    assert info.is_alive

    # Clean up
    listener_manager.stop(listener_id)
```

## Dependencies

The tests require:
- `pytest`: Test framework
- `unittest.mock`: For mocking (standard library)
- `threading`: For thread management (standard library)
- `queue`: For log queues (standard library)
- `time`: For timing operations (standard library)

No Kafka infrastructure is required to run these tests.

## Future Enhancements

Potential areas for additional testing:
1. Integration tests with actual Kafka infrastructure
2. Performance tests with many concurrent listeners
3. Memory leak tests for long-running listeners
4. Network failure simulation tests
5. Schema validation tests with real schema registry

## Maintenance Notes

- Tests use `time.sleep()` for thread synchronization - adjust timing if tests become flaky
- Mock objects should be kept in sync with actual AGStream interface changes
- Consider adding property-based tests for edge cases
- Monitor test execution time and optimize if it grows significantly

---

**Created**: 2026-03-04
**Last Updated**: 2026-03-04
**Test Coverage**: 100% of ListenerManager public API
