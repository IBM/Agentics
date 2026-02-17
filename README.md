<h1 align="center">Agentics</h1>
<p align="center"><b>Transduction is all you need</b></p>

<p align="center">
  <img src="https://raw.githubusercontent.com/IBM/Agentics/refs/heads/main/image.png" height="140" alt="Agentics logo">
</p>


<p align="center">
  Agentics is a Python framework for structured, scalable, and semantically grounded <i>agentic computation</i>.<br/>
  Build AI-powered pipelines as <b>typed data transformations</b>—combining Pydantic schemas, LLM-powered transduction, and async execution.
</p>

---

## ✨ Why Agentics

Most "agent frameworks" let untyped text flow through a pipeline. Agentics flips that: **types are the interface**.
Workflows are expressed as transformations between structured states, with predictable schemas and composable operators.

---

## 🚀 Key Features

- **Typed agentic computation**: Define workflows over structured types using standard **Pydantic** models.
- **Logical transduction (`<<`)**: Transform data between types using LLMs (few-shot examples, tools, memory).
- **Async mapping & reduction**: Scale out with `amap` and `areduce` over datasets.
- **Batch execution & retry**: Built-in batching, retries, and graceful fallbacks.
- **Tool support (MCP)**: Integrate external tools via MCP.

---

## 📦 Getting Started

Quickstart:

Install Agentics in your current env, set up your environment variable, and run your first logical transduction:

```bash
uv pip install agentics-py
```
set up your .env using the required parameters for your LLM provider of choice. Use [.env_sample](.env_sample) as a reference.

Find out more
👉 **Getting Started**: [docs/getting_started.md](docs/getting_started.md)

**Examples**

Run scripts in the `examples/` folder (via `uv`):

```bash
uv run python examples/hello_world.py
```


---

## 🧪 Example Usage

```python
from pydantic import BaseModel, Field
from agentics.core.transducible_functions import transducible, Transduce

class ProductDescription(BaseModel):
    name: str
    features: str
    price: float

class ViralTweet(BaseModel):
    tweet: str = Field(..., description="Engaging tweet under 280 characters")
    hashtags: list[str] = Field(..., description="3-5 relevant hashtags")
    hook: str = Field(..., description="Attention-grabbing opening line")

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

---

## 📘 Documentation and Notebooks

Complete documentation available [here](./docs/index.md)

| Notebook | Description |
|---|---|
| [agentics.ipynb](./tutorials/agentics.ipynb) | Core Agentics concepts: typed states, operators, and workflow structure |
| [atypes.ipynb](./tutorials/atypes.ipynb) | Working with ATypes: schema composition, merging, and type-driven design patterns |
| [logical_transduction_algebra.ipynb](./tutorials/logical_transduction_algebra.ipynb) | Logical Transduction Algebra: principles and examples behind `<<` |
| [map_reduce.ipynb](./tutorials/map_reduce.ipynb) | Scale out workflows with `amap` / `areduce` (MapReduce-style execution) |
| [synthetic_data_generation.ipynb](./tutorials/synthetic_data_generation.ipynb) | Generate structured synthetic datasets using typed transductions |
| [transducible_functions.ipynb](./tutorials/transducible_functions.ipynb) | Build reusable `@transducible` functions, explanations, and transduction control |

## ✅ Tests

Run all tests:

```bash
uv run pytest
```


---

## 📄 License

Apache 2.0

---

## 👥 Authors

**Project Lead and Main Contributor**
- Alfio Massimiliano Gliozzo (IBM Research) — gliozzo@us.ibm.com

**Core Contributors**
- Junkyu Lee (IBM) — Junkyu.Lee@ibm.com
- Nahuel Defosse (IBM) — nahuel.defosse@ibm.com
- Naweed Aghmad Khan (IBM) — naweed.khan@ibm.com

**Community Contributors**
- Christodoulos Constantinides (IBM) — Christodoulos.Constantinides@ibm.com
- Nandana Mihindukulasooriya (IBM) — nandana@ibm.com
- Mustafa Eyceoz (Red Hat) — Mustafa.Eyceoz@partner.ibm.com
- Gaetano Rossiello (IBM) — gaetano.rossiello@ibm.com
- Agostino Capponi (Columbia University) — ac3827@columbia.edu
- Chunghyun Han (Columbia University) — ch4005@columbia.edu
- Abhinav Goel (Columbia University) ag5252@columbia.edu
- Chaitya Shan (Columbia University) — cs4621@columbia.edu
- Brian Zi Qi Zhu (Columbia University) — bzz2101@columbia.edu
---


## 🧠 Conceptual Overview

Most “agent frameworks” let untyped text flow through a pipeline. Agentics flips that: **types are the interface**.
Workflows are expressed as transformations between structured states, with predictable schemas and composable operators.

Because every step is a typed transformation, you can **compose** workflows safely (merge and compose types/instances, chain transductions, and reuse `@transducible` functions) without losing semantic structure.

Agentics makes it natural to **scale out**: apply transformations over collections with async `amap`, and aggregate results with `areduce`.

Agentics models workflows as transformations between **typed states**.

Core operations:

- `amap(func)`: apply an async function over each state
- `areduce(func)`: reduce a list of states into a single value
- `<<`: logical transduction from source to target Agentics
- `&`: merge Pydantic types / instances
- `@`: compose Pydantic types / instances



## 📜 Reference

Agentics implements **Logical Transduction Algebra**, described in:

- Alfio Gliozzo, Naweed Khan, Christodoulos Constantinides, Nandana Mihindukulasooriya, Nahuel Defosse, Junkyu Lee.
  *Transduction is All You Need for Structured Data Workflows* (August 2025).
  arXiv:2508.15610 — https://arxiv.org/abs/2508.15610


---

## 🤝 Contributing

Contributions are welcome!
[CONTRIBUTING.md](CONTRIBUTING.md)

 Please ensure your commit messages include:

```text
Signed-off-by: Author Name <authoremail@example.com>
```
