# Transduction with Python Code Examples

This example demonstrates how to run transductions using Python code as an argument in the Agentics framework.

## Overview

The `transduction_with_code.py` script shows different ways to create and use transducible functions:

1. **Simple transduction** - Using just InputModel and OutputModel
2. **Custom function code** - Passing Python code as a string with custom logic
3. **Summarization** - Practical example of content summarization
4. **Batch processing** - Processing multiple inputs at once

## Setup

Before running the examples, ensure you have:

1. **Installed Agentics**:
   ```bash
   pip install -e .
   ```

2. **Set up your LLM provider** by creating a `.env` file in the project root:
   ```bash
   # For OpenAI
   OPENAI_API_KEY=your_openai_api_key_here

   # OR for Anthropic
   ANTHROPIC_API_KEY=your_anthropic_api_key_here
   ```

## Running the Examples

```bash
python examples/transduction_with_code.py
```

## Key Concepts

### Method 1: Simple Transduction (No Custom Code)

```python
from agentics.core.transducible_functions import make_transducible_function

# Just specify models and instructions
email_writer = make_transducible_function(
    InputModel=GenericInput,
    OutputModel=Email,
    instructions="Write a professional email based on the content.",
)

result = await email_writer(GenericInput(content="Meeting tomorrow"))
```

### Method 2: With Custom Function Code

```python
# Define complete function with models
function_code = """
from typing import Optional
from pydantic import BaseModel
from agentics.core.transducible_functions import Transduce

class TextInput(BaseModel):
    text: Optional[str] = None

class TextOutput(BaseModel):
    result: Optional[str] = None

async def process_text(state: TextInput) -> TextOutput:
    # Custom pre-processing
    if state.text:
        state.text = state.text.upper()

    # Transduce triggers LLM transformation
    return Transduce(state)
"""

processor = make_transducible_function(
    function_code=function_code,
    instructions="Process the text...",
)

# Get models from the function
TextInput = processor.input_model
result = await processor(TextInput(text="hello"))
```

## How It Works

The [`make_transducible_function()`](../src/agentics/core/transducible_functions.py:533) function:

1. **With `function_code`**:
   - Parses and executes your Python code
   - Extracts the function and model definitions
   - Wraps it with LLM capabilities

2. **With `InputModel`/`OutputModel`**:
   - Creates a pure LLM-based transformation
   - No custom code needed

3. **Returns a `TransducibleFunction`** that:
   - Can process single inputs or lists
   - Supports async execution
   - Integrates with the Agentics framework

## Additional Parameters

You can customize behavior with additional parameters:

```python
make_transducible_function(
    InputModel=MyInput,
    OutputModel=MyOutput,
    instructions="...",
    tools=[web_search],           # Add tools for the LLM
    reasoning=True,                # Enable planning
    max_iter=20,                   # Max tool iterations
    provide_explanation=True,      # Get explanations
    batch_size=10,                 # Parallel processing size
    timeout=300,                   # Timeout in seconds
)
```

## Related Files

- [`src/agentics/core/transducible_functions.py`](../src/agentics/core/transducible_functions.py) - Core implementation
- [`src/agentics/core/utils.py`](../src/agentics/core/utils.py) - Helper functions including `import_last_function_from_code()`
- [`tutorials/transducible_functions.ipynb`](../tutorials/transducible_functions.ipynb) - Interactive tutorial
- [`examples/hello_world.py`](hello_world.py) - Simplest example

## Troubleshooting

**Error: "Provided llm object must be a crew ai llm"**
- Make sure your `.env` file has a valid API key
- The framework needs a CrewAI-compatible LLM provider

**Error: "No functions found" or "NameError"**
- Ensure your `function_code` includes all necessary imports and model definitions
- Models must be defined in the code string, not just referenced

**Error: "Function accepts only X, Transduce, or list"**
- Make sure you're using the models from the created function
- When using `function_code`, get models via: `InputModel = func.input_model`
