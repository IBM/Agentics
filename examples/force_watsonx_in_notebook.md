# How to Force WatsonX in Jupyter Notebooks

## Problem

When running in Jupyter notebooks, you see:
```
🤖 Using LLM (default): gemini (model: gemini-2.0-flash)
```

Even though your `.env` file has `SELECTED_LLM="watsonx"`.

## Why This Happens

Jupyter notebooks may not properly load the `SELECTED_LLM` environment variable, so the system falls back to the first available LLM alphabetically (Gemini comes before WatsonX).

## Solution: Explicitly Specify WatsonX

### Method 1: Get WatsonX LLM and Pass It

```python
from agentics.core.llm_connections import get_llm_provider
from agentics.core.transducible_functions import make_transducible_function

# Explicitly get WatsonX LLM
watsonx_llm = get_llm_provider("watsonx")

# Use it in your transducible function
write_tweet = make_transducible_function(
    InputModel=Movie,
    OutputModel=Tweet,
    llm=watsonx_llm,  # ✅ Explicitly use WatsonX
)
```

### Method 2: Set Environment Variable in Notebook

```python
import os
os.environ["SELECTED_LLM"] = "watsonx"

# Then reload the module to pick up the change
import importlib
import agentics.core.llm_connections
importlib.reload(agentics.core.llm_connections)

# Now create your function
from agentics.core.transducible_functions import make_transducible_function

write_tweet = make_transducible_function(
    InputModel=Movie,
    OutputModel=Tweet,
    # Will now use WatsonX
)
```

### Method 3: Use << Operator with Explicit LLM

For the `<<` operator, you need to use `With()` to specify the LLM:

```python
from agentics.core.transducible_functions import With
from agentics.core.llm_connections import get_llm_provider

# Get WatsonX LLM
watsonx_llm = get_llm_provider("watsonx")

# Use With() to specify LLM
write_tweet = Tweet << With(Movie, llm=watsonx_llm)

# Now use it
tweet = await write_tweet(movie_data)
```

## Complete Working Example for Notebooks

```python
# Cell 1: Setup
import os
from typing import Optional
from pydantic import BaseModel, Field

# Force WatsonX selection
os.environ["SELECTED_LLM"] = "watsonx"

# Import after setting env var
from agentics.core.llm_connections import get_llm_provider
from agentics.core.transducible_functions import make_transducible_function, With

# Verify WatsonX is selected
watsonx_llm = get_llm_provider("watsonx")
print(f"Using LLM: {watsonx_llm}")

# Cell 2: Define Models
class Movie(BaseModel):
    movie_name: Optional[str] = Field(None, description="Movie title.")
    description: Optional[str] = Field(None, description="Short plot summary.")
    year: Optional[int] = Field(None, description="Year of release.")

class Tweet(BaseModel):
    content: str

# Cell 3: Create Function with Explicit LLM
# Method A: Using make_transducible_function
write_tweet_v1 = make_transducible_function(
    InputModel=Movie,
    OutputModel=Tweet,
    instructions="Write an engaging tweet about this movie",
    llm=watsonx_llm,  # ✅ Explicitly use WatsonX
)

# Method B: Using << operator with With()
write_tweet_v2 = Tweet << With(Movie, llm=watsonx_llm)

# Cell 4: Use the Function
movie = Movie(
    movie_name="The Godfather",
    description="Crime drama about a mafia family",
    year=1972
)

# This will now use WatsonX
tweet = await write_tweet_v1(movie)
print(tweet.content)
```

## Verify Which LLM is Being Used

Add this at the start of your notebook to see which LLMs are available:

```python
from agentics.core.llm_connections import get_available_llms
import os

print(f"SELECTED_LLM env var: {os.getenv('SELECTED_LLM')}")
print(f"Available LLMs: {list(get_available_llms().keys())}")

# Get and verify WatsonX
watsonx = get_available_llms().get("watsonx")
if watsonx:
    print(f"✓ WatsonX available: {watsonx}")
    print(f"  Model: {watsonx.model if hasattr(watsonx, 'model') else 'N/A'}")
else:
    print("✗ WatsonX not available!")
```

## Why Explicit LLM is Better in Notebooks

1. **Predictable**: You know exactly which LLM is being used
2. **No environment variable issues**: Works regardless of how the notebook was started
3. **Easy to switch**: Just change one variable to test different LLMs
4. **Clear in code**: Anyone reading the notebook knows which LLM is used

## Quick Fix for Your Code

Change this:
```python
write_tweet = Tweet << Movie  # ❌ Uses default (Gemini)
```

To this:
```python
from agentics.core.llm_connections import get_llm_provider
from agentics.core.transducible_functions import With

watsonx_llm = get_llm_provider("watsonx")
write_tweet = Tweet << With(Movie, llm=watsonx_llm)  # ✅ Uses WatsonX
