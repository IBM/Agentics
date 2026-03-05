# Flink Execution Modes in Agentics

## Overview

Agentics supports two execution modes for running listeners:

### 1. Local PyFlink (Default)
- **Method**: `AGStream.transducible_function_listener()`
- **Execution**: PyFlink runs embedded in the Python process
- **Parallelism**: Local task slots (configurable via `parallelism` parameter)
- **Use Case**: Development, testing, single-machine deployments
- **Pros**: Simple setup, no external dependencies
- **Cons**: Limited to single machine resources

### 2. Flink Cluster (Production)
- **Method**: `FlinkListenerManager` (via REST API submission)
- **Execution**: Jobs submitted to external Flink JobManager
- **Parallelism**: Distributed across Flink TaskManagers
- **Use Case**: Production, scalable deployments
- **Pros**: Distributed execution, fault tolerance, scalability
- **Cons**: Requires Flink cluster infrastructure

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AGstream Manager                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Execution Mode Selection                             │   │
│  │  ┌────────────┐              ┌──────────────────┐    │   │
│  │  │   Local    │              │     Cluster      │    │   │
│  │  │  PyFlink   │              │  FlinkListener   │    │   │
│  │  │            │              │     Manager      │    │   │
│  │  └─────┬──────┘              └────────┬─────────┘    │   │
│  └────────┼─────────────────────────────┼──────────────┘   │
│           │                             │                   │
│           ▼                             ▼                   │
│  ┌────────────────┐          ┌──────────────────────┐      │
│  │  AGStream      │          │  Flink REST API      │      │
│  │  .transduc...  │          │  Job Submission      │      │
│  │  _listener()   │          │                      │      │
│  └────────┬───────┘          └──────────┬───────────┘      │
│           │                             │                   │
└───────────┼─────────────────────────────┼───────────────────┘
            │                             │
            ▼                             ▼
   ┌────────────────┐          ┌──────────────────────┐
   │  Local PyFlink │          │  Flink JobManager    │
   │  Embedded      │          │  (Port 8085)         │
   │  Execution     │          │                      │
   └────────┬───────┘          └──────────┬───────────┘
            │                             │
            │                             ▼
            │                   ┌──────────────────────┐
            │                   │  Flink TaskManagers  │
            │                   │  (Distributed)       │
            │                   └──────────┬───────────┘
            │                             │
            └─────────────┬───────────────┘
                          ▼
                 ┌────────────────┐
                 │  Kafka Topics  │
                 │  (Input/Output)│
                 └────────────────┘
```

## Current Implementation Status

### ✅ Completed
1. **Local PyFlink Execution**
   - `AGStream.transducible_function_listener()` fully functional
   - Runs PyFlink embedded in process
   - Parallel processing via local task slots

2. **FlinkListenerManager**
   - Class created in `src/agentics/core/flink_listener_manager.py`
   - Generates standalone PyFlink job scripts
   - Submits jobs via Flink CLI (`flink run`)
   - Monitors job status via REST API

3. **Full Stack Infrastructure**
   - `docker-compose-karapace-flink.yml` with Flink cluster
   - `manage_services_full.sh` for managing all services
   - Flink Web UI on port 8085

### ⚠️ Pending Integration
- AGstream Manager service needs to be updated to use FlinkListenerManager for cluster mode
- UI needs execution mode selector
- Configuration management for switching modes

## Usage Examples

### Local Mode (Current Default)
```python
from agentics.core.streaming import AGStream
from agentics.core.transducible_functions import make_transducible_function

# Create transducible function
fn = make_transducible_function(
    InputModel=Question,
    OutputModel=Answer,
    instructions="Answer the question"
)

# Create AGStream
ag = AGStream(
    input_topic="questions",
    output_topic="answers",
    target_atype_name="Answer"
)

# Run listener (local PyFlink)
ag.transducible_function_listener(
    fn=fn,
    parallelism=10,
    verbose=True
)
```

### Cluster Mode (Using FlinkListenerManager)
```python
from agentics.core import FlinkListenerManager

# Create listener manager
mgr = FlinkListenerManager(
    agstream_factory=make_ag,
    flink_jobmanager_url="http://localhost:8085"
)

# Submit job to Flink cluster
listener_id = mgr.start(
    fn=fn,
    input_topic="questions",
    output_topic="answers",
    source_atype_name="Question-value",
    target_atype_name="Answer-value"
)

# Check status
status = mgr.get_job_status(listener_id)  # RUNNING, FINISHED, FAILED, etc.

# Stop job
mgr.stop(listener_id)
```

## Next Steps for Full Integration

1. **Update AGstream Manager Service**
   - Add execution mode configuration
   - Integrate FlinkListenerManager for cluster mode
   - Update listener creation endpoint to support both modes

2. **Update UI**
   - Add execution mode selector
   - Show different status indicators for local vs cluster
   - Display Flink job IDs for cluster mode

3. **Configuration**
   - Environment variable: `LISTENER_EXECUTION_MODE=local|cluster`
   - Per-listener execution mode override
   - Flink cluster connection settings

## Technical Notes

### Why Not Direct Remote Submission in AGStream?

PyFlink's `StreamExecutionEnvironment` does not support direct remote submission like Java Flink does. The `create_remote_environment()` method doesn't exist in PyFlink. Instead, we must:

1. Package the PyFlink job as a script
2. Submit via Flink CLI: `flink run -py script.py`
3. Or submit via REST API with proper job packaging

This is why FlinkListenerManager exists as a separate component that handles the packaging and submission process.

### Flink Cluster Requirements

For cluster mode to work:
- Flink JobManager must be accessible (default: localhost:8085)
- Flink TaskManagers must be running
- Kafka connector JAR must be available in `flink-lib/`
- Python environment must be consistent across cluster nodes

## Made with Bob
