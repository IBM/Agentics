# Getting Started

## What is agentics?

Agentics is a lightweight, Python-native framework for building structured, agentic workflows over tabular or JSON-based data using Pydantic types and transduction logic. Designed to work seamlessly with large language models (LLMs), Agentics enables users to define input and output schemas as structured types and apply declarative, composable transformations, called transductions across data collections. Inspired by a low-code design philosophy, Agentics is ideal for rapidly prototyping intelligent systems that require structured reasoning and interpretable outputs over both structured and unstructured data. 

## Installation

* Clone the repository

  ```shell
    git clone git@github.com:IBM/agentics.git
    cd agentics
  ```

* Install uv (skip if available) 

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

  Other installation options [here](https://docs.astral.sh/uv/getting-started/installation/)

* Install the dependencies

  ```bash
  uv sync
  # Source the environment (optional, you can skip this and prepend uv run to the later lines)
  source .venv/bin/activate # bash/zsh 🐚
  source .venv/bin/activate.fish # fish 🐟
  ```


### 🎯 Environment Variables

Create a `.env` file in the root directory with your environment variables. See `.env.sample` for an example.

Set up LLM provider, chose one of the following: 

#### OpenAI

- Obtain API key from [OpenAI](https://platform.openai.com/)
- `OPENAI_API_KEY` - Your OpenAI APIKey
- `OPENAI_MODEL_ID` - Selected model, default to **openai/gpt-4**

#### Ollama (local)
- Download and install [Ollama](https://ollama.com/)
- Download a model. You should use a model that support reasoning and fit your GPU. So smaller are preferred. 
```
ollama pull ollama/deepseek-r1:latest
```
- "OLLAMA_MODEL_ID" - ollama/gpt-oss:latest (better quality), ollama/deepseek-r1:latest (smaller)

#### IBM WatsonX:

- `WATSONX_APIKEY` - WatsonX API key

- `MODEL`  - watsonx/meta-llama/llama-3-3-70b-instruct (or alternative supporting function call)


#### Google Gemini (offers free API key)

- `GEMINI_API_KEY` - Your Google Gemini API key (get it from [Google AI Studio](https://aistudio.google.com/))

- `MODEL` - `gemini/gemini-1.5-pro` or `gemini/gemini-1.5-flash` (or other Gemini models supporting function calling)


#### VLLM (Need dedicated GPU server):

- Set up your local instance of VLLM
- `VLLM_URL` - <http://base_url:PORT/v1>
- `VLLM_MODEL_ID` - Your model id (e.g. "hosted_vllm/meta-llama/Llama-3.3-70B-Instruct" )


## Test Installation

Test hello world example (need to set up llm credentials first)

```bash
python python examples/hello_world.py
python examples/self_transduction.py
python examples/agentics_web_search_report.py

```


## Hello World

Transform boring product descriptions into viral tweets in just a few lines:

```python
from pydantic import BaseModel, Field
from agentics.core.transducible_functions import transducible, Transduce

from typing import Optional

class ProductDescription(BaseModel):
    name: Optional[str] = None
    features: Optional[str] = None
    price: Optional[float] = None

class ViralTweet(BaseModel):
    tweet: Optional[str] = Field(None, description="Engaging tweet under 280 characters")
    hashtags: Optional[list[str]] = Field(None, description="3-5 relevant hashtags")
    hook: Optional[str] = Field(None, description="Attention-grabbing opening line")

@transducible()
async def generate_viral_tweet(product: ProductDescription) -> ViralTweet:
    """Transform boring product descriptions into viral social media content."""
    return Transduce(product)

# Transform a product into viral content
product = ProductDescription(
    name="Agentics Framework",
    features="Type-safe AI workflows with LLM-powered transductions",
    price=0.0  # Open source!
)

tweet = await generate_viral_tweet(product)
print(f"🔥 {tweet.tweet}")
print(f"📱 {' '.join(tweet.hashtags)}")
```

**Output:**
```
🔥 Stop wrestling with unstructured LLM outputs! 🎯 Agentics gives you type-safe AI workflows that just work. Build production-ready agents in minutes, not weeks. And it's FREE! 🚀
📱 #AI #OpenSource #Python #LLM #DevTools
```

### Alternative: Using  `<<` Operator

For quick one-off transductions, use `<<` operator:

```python
from pydantic import BaseModel

from typing import Optional

class Product(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class Tweet(BaseModel):
    content: Optional[str] = None

# Create transduction on the fly
make_tweet = Tweet << Product

product = Product(
    name="Agentics",
    description="Type-safe AI framework for Python"
)

tweet = await make_tweet(product)
print(tweet.content)
```

This concise syntax is perfect for exploratory work and rapid prototyping!

### Batch Processing: Multiple Products

Transducible functions automatically support batch processing. Process multiple products at once in parallel:

```python

products = [
    ProductDescription(
        name="Agentics Framework",
        features="Type-safe AI workflows with LLM-powered transductions",
        price=0.0
    ),
    ProductDescription(
        name="Smart Coffee Maker",
        features="AI-powered brewing with perfect temperature control",
        price=299.99
    ),
    ProductDescription(
        name="Wireless Earbuds Pro",
        features="Active noise cancellation and 30-hour battery life",
        price=149.99
    ),
]

# Automatically processes all products in parallel
tweets = await generate_viral_tweet(products)

# Display results
for product, tweet in zip(products, tweets):
    print(f"\n📦 Product: {product.name}")
    print(f"🔥 Tweet: {tweet.tweet}")
    print(f"📱 Tags: {' '.join(tweet.hashtags)}")
```

**Output:**
```
📦 Product: Agentics Framework
🔥 Tweet: Stop wrestling with unstructured LLM outputs! 🎯 Agentics gives you type-safe AI workflows that just work. Build production-ready agents in minutes, not weeks. And it's FREE! 🚀
📱 Tags: #AI #OpenSource #Python #LLM #DevTools

📦 Product: Smart Coffee Maker
🔥 Tweet: Wake up to perfection! ☕ Our AI-powered coffee maker learns your taste and brews the perfect cup every time. Never settle for mediocre coffee again! 🤖
📱 Tags: #SmartHome #Coffee #AI #Tech #MorningRoutine

📦 Product: Wireless Earbuds Pro
🔥 Tweet: Silence the world, amplify your music! 🎧 30 hours of pure audio bliss with active noise cancellation. Your commute just got an upgrade! 🔋
📱 Tags: #Audio #Tech #Wireless #Music #Productivity
```

The same transducible function works seamlessly for both single items and batches—no code changes needed!

### Installation details

#### Poetry

    Install poetry (skip if available)

    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```

    Clone and install agentics

    ```bash
    
    poetry install
    source $(poetry env info --path)/bin/activate 
    ```

#### Python

    > Ensure you have Python 3.11+ 🚨.
    >
    > ```shell
    > python --version
    > ```

    * Create a virtual environment with Python's built in `venv` module. In linux, this 
    package may be required to be installed with the Operating System package manager.
        ```shell
        python -m venv .venv
        ```

    * Activate the virtual environment

    `source .venv/bin/activate`         # Bash/Zsh
    `source .venv/bin/activate.fish`    # Fish

    * Install the package
        ```bash
        python -m pip install ./agentics
        ```
    

#### uv

    * Ensure `uv` is installed.
    ```bash
    command -v uv >/dev/null &&  curl -LsSf https://astral.sh/uv/install.sh | sh
    # It's recommended to restart the shell afterwards
    exec $SHELL
    ```
    * `uv venv --python 3.11`
    * `uv pip install ./agentics` or `uv add ./agentics` (recommended)
  

#### uvx 🏃🏽

    > This is a way to run agentics temporarily or quick tests

    * Ensure `uv` is installed.
    ```bash
    command -v uv >/dev/null &&  curl -LsSf https://astral.sh/uv/install.sh | sh
    # It's recommended to restart the shell afterwards
    exec $SHELL
    ```
    * uvx --verbose --from ./agentics ipython


#### Conda

    1. Create a conda environment:
       ```bash
       conda create -n agentics python=3.11
       ```
       In this example the name of the environment is `agetnics` but you can change
       it to your personal preference.


    2. Activate the environment
        ```bash
        conda activate agentics
        ```
    3. Install `agentics` from a folder or git reference
        ```bash
        pip install ./agentics
        ```

## Next
- 👉 [Core Concepts](core_concepts.md) - Understanding the theoretical foundation
