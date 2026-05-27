# Multiply by Two - Transducible Functions Example

This example demonstrates how to use transducible functions with custom Python code to process lists of integers and multiply them by 2.

## Overview

The [`multiply_by_two.py`](multiply_by_two.py) script shows four different approaches to using transducible functions with the `function_code` parameter:

1. **Full-featured multiplication** - Complete implementation with original values, multiplied values, and count
2. **Simple multiplication** - Streamlined version that just returns the multiplied values
3. **Batch processing** - Process multiple lists of integers at once
4. **Validation and statistics** - Add input validation and calculate statistics

## Key Concepts

### Using `function_code` Parameter

The `function_code` parameter allows you to define custom Python logic that executes **before** or **instead of** LLM transduction. This is useful when you want to:

- Perform deterministic computations (like multiplication)
- Add pre/post-processing logic
- Validate inputs
- Calculate statistics or metadata

### Basic Structure

```python
function_code = """
from typing import Optional
from pydantic import BaseModel, Field

class InputModel(BaseModel):
    numbers: Optional[list[int]] = None

class OutputModel(BaseModel):
    result: Optional[list[int]] = None

async def process_function(state: InputModel) -> OutputModel:
    '''Your custom logic here'''
    if state.numbers:
        result = [n * 2 for n in state.numbers]
        return OutputModel(result=result)
    return OutputModel(result=[])
"""

multiplier = make_transducible_function(
    function_code=function_code,
    instructions="Multiply each number by 2",
)
```

## Examples Explained

### Example 1: Full-Featured Multiplication

This example shows how to return multiple pieces of information:
- Original list
- Multiplied list
- Count of items processed

```python
class MultipliedList(BaseModel):
    original: Optional[list[int]] = None
    multiplied: Optional[list[int]] = None
    count: Optional[int] = None
```

**Output:**
```json
{
  "original": [1, 2, 3, 4, 5],
  "multiplied": [2, 4, 6, 8, 10],
  "count": 5
}
```

### Example 2: Simple Multiplication

A streamlined version that processes multiple test cases:

```python
test_cases = [
    [1, 2, 3, 4, 5],
    [10, 20, 30],
    [7, 14, 21, 28],
]
```

**Output:**
```
Input:  [1, 2, 3, 4, 5]
Output: [2, 4, 6, 8, 10]

Input:  [10, 20, 30]
Output: [20, 40, 60]

Input:  [7, 14, 21, 28]
Output: [14, 28, 42, 56]
```

### Example 3: Batch Processing

Process multiple lists simultaneously using the agentics parallel processing capabilities:

```python
input_lists = [
    NumberList(numbers=[1, 2, 3]),
    NumberList(numbers=[5, 10, 15]),
    NumberList(numbers=[100, 200, 300]),
]

results = await multiplier(input_lists)  # Processes all at once
```

### Example 4: Validation and Statistics

Add input validation using Pydantic validators and calculate statistics:

```python
class ValidatedInput(BaseModel):
    numbers: Optional[list[int]] = None

    @field_validator('numbers')
    @classmethod
    def validate_positive(cls, v):
        if v and any(n < 0 for n in v):
            raise ValueError("All numbers must be positive")
        return v
```

**Output:**
```json
{
  "original": [5, 10, 15, 20],
  "doubled": [10, 20, 30, 40],
  "sum_original": 50,
  "sum_doubled": 100
}
```

## Running the Example

```bash
# Make sure you have set up your .env file with API keys
python examples/multiply_by_two.py
```

## Key Takeaways

1. **Pure Python Logic**: When you define custom logic in `function_code`, it executes as pure Python without LLM involvement (unless you explicitly call `Transduce()`)

2. **Type Safety**: Using Pydantic models ensures type safety and validation

3. **Batch Processing**: Pass a list of inputs to process multiple items efficiently

4. **Flexibility**: You can mix deterministic code (multiplication) with LLM-based transformations when needed

## Related Examples

- [`transduction_with_code.py`](transduction_with_code.py) - More examples of using `function_code` with LLM transduction
- [`hello_world.py`](hello_world.py) - Basic transducible functions without custom code

## Learn More

See the [Transducible Functions documentation](../docs/transducible_functions.md) for more details on:
- The `Transduce()` function for LLM-based transformations
- Combining custom code with LLM processing
- Advanced patterns and best practices
