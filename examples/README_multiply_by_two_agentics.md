# Multiply by Two with Agentics (AG) - Parallel Processing

This example demonstrates how to use Agentics (AG) tables with transducible functions to process lists of integers in parallel, multiplying each by 2.

## Overview

The [`multiply_by_two_agentics.py`](multiply_by_two_agentics.py) script shows how to:

1. Create an AG table where each row contains a single integer
2. Use transducible functions with custom code to process all rows in parallel
3. Handle large batches efficiently (100+ items)
4. Add statistics and metadata to results

## Key Concepts

### AG (Agentics) Tables

An `AG` object is like a typed dataframe where:
- Each row is a Pydantic model instance
- All rows share the same type (schema)
- Operations can be performed on all rows in parallel

### Creating AG Tables

```python
from agentics import AG
from pydantic import BaseModel

class Number(BaseModel):
    value: int

# Create AG from list of states
numbers_ag = AG.from_states([
    Number(value=1),
    Number(value=2),
    Number(value=3),
])
```

### Parallel Processing with Transducible Functions

When you pass a list of states to a transducible function, it processes them all in parallel:

```python
# Process all states in parallel
results = await multiplier(numbers_ag.states)
```

## Examples Explained

### Example 1: Basic Multiplication with AG

Creates an AG table with 8 numbers and processes them all in parallel:

```python
# Input AG table (8 rows)
numbers_ag = AG.from_states([
    NumberInput(value=1),
    NumberInput(value=2),
    # ... more numbers
])

# Process all in parallel
results = await multiplier(numbers_ag.states)
```

**Output:**
```
Input AG table (8 rows):
  Row 1: value=1
  Row 2: value=2
  ...

Results (processed in parallel):
  Row 1: 1 × 2 = 2
  Row 2: 2 × 2 = 4
  ...
```

**Performance:** Processes 8 states at ~230,000 states/second

### Example 2: Simplified AG Transduction

A more streamlined approach:

```python
numbers = [3, 6, 9, 12, 15, 18]
numbers_ag = AG.from_states([Number(value=n) for n in numbers])

results = await doubler(numbers_ag.states)
```

**Output:**
```
Input: [3, 6, 9, 12, 15, 18]

Results:
  3 → 6
  6 → 12
  9 → 18
  12 → 24
  15 → 30
  18 → 36
```

### Example 3: Large Batch Processing

Demonstrates efficient processing of 100 numbers in parallel:

```python
# Create 100 numbers
numbers = list(range(1, 101))
numbers_ag = AG.from_states([SingleNumber(n=num) for num in numbers])

# Process all at once
results = await processor(numbers_ag.states)
```

**Output:**
```
Processing 100 numbers in parallel...

First 10 results:
  1 × 2 = 2
  2 × 2 = 4
  ...

Last 10 results:
  91 × 2 = 182
  ...
  100 × 2 = 200

✅ All 100 results are correct: True
```

**Performance:** Processes in batches of 10, achieving ~250,000-350,000 states/second per batch

### Example 4: Processing with Statistics

Add metadata and statistics to each result:

```python
class OutputWithStats(BaseModel):
    original: int
    doubled: int
    is_even: bool
    is_positive: bool

# Process with statistics
results = await processor(numbers_ag.states)
```

**Output:**
```
Input numbers: [-5, -2, 0, 3, 7, 10, 15, 20]

Results with statistics:
   -5 × 2 = -10 | Even: False | Positive: False
   -2 × 2 =  -4 | Even: True | Positive: False
    0 × 2 =   0 | Even: True | Positive: False
    3 × 2 =   6 | Even: False | Positive: True
    ...

Summary:
  Total numbers: 8
  Even numbers: 4
  Positive numbers: 5
```

## Performance Characteristics

The script demonstrates excellent parallel processing performance:

- **Small batches (8 items):** ~230,000 states/second
- **Medium batches (10 items):** ~250,000-350,000 states/second
- **Large datasets (100 items):** Automatically batched for optimal performance

## Key Advantages of AG Tables

1. **Type Safety:** All rows share the same Pydantic schema
2. **Parallel Processing:** Automatic parallelization of operations
3. **Batch Management:** Automatic batching for large datasets
4. **Progress Tracking:** Built-in progress bars for long operations
5. **Memory Efficient:** Processes data in configurable batch sizes

## Running the Example

```bash
# Make sure you have set up your .env file with API keys
python examples/multiply_by_two_agentics.py
```

## Comparison with Other Approaches

| Approach | Use Case | Parallel | Batch Size |
|----------|----------|----------|------------|
| [`multiply_by_two.py`](multiply_by_two.py) | Simple list processing | Manual | N/A |
| [`multiply_by_two_agentics.py`](multiply_by_two_agentics.py) | AG table processing | Automatic | Configurable |

## Configuration Options

You can configure batch size when creating AG objects:

```python
numbers_ag = AG.from_states(
    states=[...],
    amap_batch_size=20  # Process 20 items per batch
)
```

## Related Examples

- [`multiply_by_two.py`](multiply_by_two.py) - Basic transducible functions without AG
- [`hello_world.py`](hello_world.py) - Introduction to AG tables
- [`transduction_with_code.py`](transduction_with_code.py) - More transduction patterns

## Learn More

See the documentation for:
- [Agentics Core Concepts](../docs/core_concepts.md) - Understanding AG tables
- [Transducible Functions](../docs/transducible_functions.md) - Function patterns
- [Map-Reduce Operations](../docs/map_reduce.md) - Parallel processing patterns
