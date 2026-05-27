# Fix for WatsonX Transduction Issue

## Problem

You're seeing this error:
```
TypeError: Provided llm object must be a crew ai llm (BaseLLM instance)
```

And the diagnostic shows:
```
✓ LLM Provider detected: NoneType
  Details: None
```

Even though WatsonX is configured and the system says it's using it.

## Root Cause

The issue is that `AG.get_llm_provider()` returns `None` when called at module import time, before the LLMs are fully initialized. The `transducible` decorator's default `llm` parameter calls this function too early.

## Solution

**Do NOT pass the `llm` parameter** to `make_transducible_function()` or `@transducible()`. Let the system use its internal default, which resolves the LLM at runtime.

### ❌ WRONG (causes the error):
```python
from agentics import AG



func = make_transducible_function(
    InputModel=Input,
    OutputModel=Output,
)
```

### ✅ CORRECT:
```python
# Don't pass llm parameter at all
func = make_transducible_function(
    InputModel=Input,
    OutputModel=Output,
    # llm parameter omitted - system will auto-detect at runtime
)
```

## Working Example

```python
import asyncio
from typing import Optional
from pydantic import BaseModel
from agentics.core.transducible_functions import make_transducible_function

class Input(BaseModel):
    text: Optional[str] = None

class Output(BaseModel):
    result: Optional[str] = None

async def main():
    # Create function WITHOUT llm parameter
    transform = make_transducible_function(
        InputModel=Input,
        OutputModel=Output,
        instructions="Echo the input text",
    )

    result = await transform(Input(text="Hello"))
    print(result.model_dump_json(indent=2))

if __name__ == "__main__":
    asyncio.run(main())
```

## For Decorator Usage

### ❌ WRONG:
```python
from agentics import AG


@transducible()  # ❌ llm is None!
async def my_func(state: Input) -> Output:
    return Transduce(state)
```

### ✅ CORRECT:
```python
@transducible()  # ✅ No llm parameter
async def my_func(state: Input) -> Output:
    return Transduce(state)
```

## Why This Happens

1. At module import time, the LLM registry is not yet populated
2. `AG.get_llm_provider()` returns `None`
3. This `None` gets passed to the decorator/function
4. Later, when you try to use the function, it fails because `llm=None`

The fix is to let the system resolve the LLM at runtime by not passing the parameter.

## Verify Your Fix

Run this to test:
```bash
python examples/hello_world.py
```

It should now work correctly with your WatsonX configuration.
