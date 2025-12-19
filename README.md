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

Most “agent frameworks” let untyped text flow through a pipeline. Agentics flips that: **types are the interface**.  
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

Install Agentics, set up your environment, and run your first logical transduction:

👉 **Getting Started**: [docs/getting_started.md](docs/getting_started.md)

---

## 📘 Documentation

- 🧠 **Agentics**: [docs/agentics.md](docs/agentics.md) — wrapping Pydantic models into transduction-ready agents  
- 🔁 **Transduction**: [docs/transduction.md](docs/transduction.md) — how `<<` works and how to control it  


---

## 🧪 Example Usage

```python
from typing import Optional
from pydantic import BaseModel, Field

from agentics.core.transducible_functions import Transduce, transducible


class Movie(BaseModel):
    movie_name: Optional[str] = None
    description: Optional[str] = None
    year: Optional[int] = None


class Genre(BaseModel):
    genre: Optional[str] = Field(None, description="e.g., comedy, drama, action")


@transducible(provide_explanation=True)
async def classify_genre(state: Movie) -> Genre:
    """Classify the genre of the source Movie."""
    return Transduce(state)


genre, explanation = await classify_genre(
    Movie(
        movie_name="The Godfather",
        description=(
            "The aging patriarch of an organized crime dynasty transfers control "
            "of his clandestine empire to his reluctant son."
        ),
        year=1972,
    )
)
```

---

## 🧠 Conceptual Overview

Agentics models workflows as transformations between **typed states**.

An `Agentics` instance typically includes:

- `atype`: a Pydantic model representing the schema
- `states`: a list of objects of that type

Core operations:

- `amap(func)`: apply an async function over each state
- `areduce(func)`: reduce a list of states into a single value
- `<<`: logical transduction from source to target Agentics
- `&`: merge Pydantic types / instances
- `@`: compose Pydantic types / instances

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

## 🗂️ Examples

Run scripts in the `examples/` folder (via `uv`):

```bash
uv run python examples/hello_world.py
```

---

## 📄 License

Apache 2.0

---

## 👥 Authors

**Principal Investigator**  
- Alfio Massimiliano Gliozzo (IBM Research) — gliozzo@us.ibm.com

**Core Contributors**  
- Nahuel Defosse (IBM Research) — nahuel.defosse@ibm.com  
- Junkyu Lee (IBM Research) — Junkyu.Lee@ibm.com  
- Naweed Aghmad Khan (IBM Research) — naweed.khan@ibm.com  
- Christodoulos Constantinides (IBM Watson) — Christodoulos.Constantinides@ibm.com  
- Mustafa Eyceoz (Red Hat) — Mustafa.Eyceoz@partner.ibm.com  

---

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
