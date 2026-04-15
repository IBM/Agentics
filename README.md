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

## 🧠 What is Agentics?

Most "agent frameworks" let untyped text flow through a pipeline. **Agentics flips that: types are the interface.**

Workflows are expressed as **transformations between structured states**, with predictable schemas and composable operators. Because every step is a typed transformation, you can compose workflows safely without losing semantic structure.

**Core Philosophy:**
- 🎯 **Type-driven design**: Pydantic models define your data contracts
- 🔄 **Logical transduction**: Transform between types using LLM-powered reasoning
- 📊 **Structured outputs**: Every operation produces validated, typed results
- 🔗 **Composable operators**: Chain transformations with mathematical precision

**Core Operations:**
- `<<` — Logical transduction from source to target types
- `amap(func)` — Apply async function over each state
- `areduce(func)` — Reduce list of states into single value
- `&` — Merge Pydantic types/instances
- `@` — Compose Pydantic types/instances

---

## 🚀 Key Features

- **Typed agentic computation**: Define workflows over structured types using standard **Pydantic** models
- **Logical transduction (`<<`)**: Transform data between types using LLMs (few-shot examples, tools, memory)
- **Async mapping & reduction**: Scale out with `amap` and `areduce` over datasets
- **Batch execution & retry**: Built-in batching, retries, and graceful fallbacks
- **Tool support (MCP)**: Integrate external tools via Model Context Protocol
- **AGStream**: Real-time streaming with Kafka, Flink SQL, and schema registry
- **PyFlink SQL Auto-Connector**: Automatically discover and query AGStream Manager channels with zero configuration

---

## 📦 Quick Start

### Installation

Agentics offers two installation options using `uv` for fast, reliable dependency management:

> **Note:** `uv` automatically creates and manages a `.venv` virtual environment for you - no need to create one manually!

#### Option 1: Core Framework Only

Install just the core Agentics package for basic agentic computation:

```bash
# From PyPI (using pip)
pip install agentics-py

# Or from source with uv (recommended)
git clone https://github.com/IBM/agentics.git
cd agentics
make install-agentics
# Equivalent to: uv sync --no-dev
# This automatically creates .venv/ and installs dependencies
```

After installation, activate the virtual environment:
```bash
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate     # On Windows
```

This installs the lightweight core framework without heavy dependencies like Kafka, Flink, or streaming components.

#### Option 2: Core + AGStream Manager

Install Agentics with AGStream Manager for real-time streaming capabilities:

```bash
# From source with uv
git clone https://github.com/IBM/agentics.git
cd agentics
make install-agstream
# Equivalent to: uv sync --no-dev --group agstream
# This automatically creates .venv/ and installs all dependencies
```

After installation, activate the virtual environment:
```bash
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate     # On Windows
```

Then start the services (Docker/Colima will be automatically managed):
```bash
make start-full
# This will:
# - Check if Docker is running
# - Automatically restart Colima if needed (macOS)
# - Start Kafka, Flink, and AGStream Manager
```

This installs:
- Core Agentics framework
- AGStream Manager dependencies (Kafka, Flask, WebSocket support)
- All required streaming components

**Quick Installation Reference:**
```bash
make install-agentics    # Core package only (uv sync --no-dev)
make install-agstream    # Core + AGStream Manager (uv sync --no-dev --group agstream)
make install-dev         # Development mode (uv sync --group dev)
```

**Service Management:**
```bash
make start-full          # Start all services (auto-restarts Colima if needed)
make stop                # Stop all services
make status              # Check service status
make open-ui             # Open AGStream Manager UI
make flink-sql           # Start Flink SQL client
```

> **💡 Tip:** `uv` handles virtual environment creation automatically, and `make start-full` handles Docker/Colima automatically on macOS!

#### Additional Tools

Each tool in `tools/` has its own isolated environment:

**Dockling Knowledge Graph Extractor** - Document-to-KG extraction
```bash
cd tools/dockling_kg_extractor
uv sync  # Installs docling, docling-core, fastapi, uvicorn
```

See individual tool documentation:
- [Dockling KG Extractor Installation](tools/dockling_kg_extractor/INSTALL.md)

### Environment Setup

```bash
# Copy sample environment file
cp .env_sample .env

# Edit .env with your LLM provider credentials
# Supported: OpenAI, Anthropic, Google Gemini, Azure OpenAI, IBM WatsonX
```

### Basic Example

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

**Run examples:**
```bash
python examples/hello_world.py
python examples/generate_tweets.py
```

---

## 📘 Documentation

### 📚 Complete Documentation
- **[Full Documentation](./docs/index.md)** - Comprehensive guides and API reference
- **[Getting Started](./docs/introduction/quickstart.md)** - Quick introduction to Agentics
- **[Architecture](./docs/introduction/architecture.md)** - System design and components
- **[Key Concepts](./docs/introduction/key-concepts.md)** - Core principles and terminology
- **[API Reference](./docs/reference/index.md)** - Detailed API documentation

### 📓 Interactive Tutorials

| Notebook | Description |
|---|---|
| [agentics.ipynb](./tutorials/agentics.ipynb) | Core Agentics concepts: typed states, operators, and workflow structure |
| [atypes.ipynb](./tutorials/atypes.ipynb) | Working with ATypes: schema composition, merging, and type-driven design patterns |
| [logical_transduction_algebra.ipynb](./tutorials/logical_transduction_algebra.ipynb) | Logical Transduction Algebra: principles and examples behind `<<` |
| [map_reduce.ipynb](./tutorials/map_reduce.ipynb) | Scale out workflows with `amap` / `areduce` (MapReduce-style execution) |
| [synthetic_data_generation.ipynb](./tutorials/synthetic_data_generation.ipynb) | Generate structured synthetic datasets using typed transductions |
| [transducible_functions.ipynb](./tutorials/transducible_functions.ipynb) | Build reusable `@transducible` functions, explanations, and transduction control |

### 💡 Examples
- **[Examples Directory](./examples/)** - Ready-to-run code examples
- **[Use Cases](./docs/introduction/use-cases.md)** - Real-world applications

---

## 🛠️ Agentics Tools

### 📚 Dockling Knowledge Graph Extractor
**Document-to-knowledge-graph extraction** powered by Docling and Agentics.

**Features:**
- 📄 **Multi-Format Support**: PDFs, DOCX, images, URLs, and markdown
- 🎯 **Custom Schemas**: Define your own entity types with Pydantic
- 🔗 **Entity Extraction**: Automatic entity and relationship detection
- 🧩 **Coreference Resolution**: Merge duplicate entities intelligently
- 📊 **Export Options**: JSON, CSV, or graph formats
- 🌐 **Web UI**: FastAPI-based interface for easy document processing

**Quick Start:**
```bash
cd tools/dockling_kg_extractor
uv sync
./start_server.sh  # Starts web UI on http://localhost:5000
```

**📖 Documentation**: [`tools/dockling_kg_extractor/README.md`](tools/dockling_kg_extractor/README.md)

---

## 🐳 Docker Setup

### Option A: Colima (macOS - Free Alternative to Docker Desktop)

```bash
# Install
brew install colima docker docker-compose

# Start with optimized settings
colima start --arch aarch64 --vm-type=vz --vz-rosetta \
  --mount-type virtiofs --cpu 4 --memory 8

# Verify
docker ps
```

### Option B: Docker Desktop

Download from [docker.com](https://www.docker.com/products/docker-desktop) and verify:
```bash
docker --version && docker compose version
```

---

## ✅ Testing

### Core Framework Tests

```bash
# Run all core tests
uv run pytest tests/test_package.py tests/test_examples.py -v

# Run with coverage
uv run pytest tests/ --cov=src/agentics --cov-report=html

# Run specific test
uv run pytest tests/test_specific_module.py -v
```

**Test Coverage:**
- ✅ Package import and initialization
- ✅ Example scripts (hello_world, transducible_functions, emotion_extractor, generate_tweets)
- ✅ Transduction operations and type transformations
- ✅ Async map/reduce operations

### AGStream Tests

AGStream tests require Kafka and Schema Registry services.
Refer to AGStream documentation for service setup.

```bash
# Run AGStream integration tests
uv run python tests/agstream_tests/test_agstream_integration.py
```

**Test Coverage:**
- ✅ Schema registration with Avro
- ✅ Message production to Kafka topics
- ✅ Message consumption from Kafka topics
- ✅ Transducible function execution
- ✅ Full produce-consume-transform cycle

**Note:** AGStream tests are automatically skipped if Kafka dependencies are not installed.

### Dockling KG Extractor Tests

```bash
cd tools/dockling_kg_extractor
uv sync  # Ensure dependencies are installed
uv run pytest tests/ -v  # If tests exist
```

**Manual Testing:**
```bash
./start_server.sh
# Open http://localhost:5000 and test document upload/processing
```

---

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Development setup with `uv`
- Pre-commit hooks
- Testing guidelines
- Code style requirements

**Quick setup:**
```bash
# Install dependencies
uv sync --all-groups --all-extras

# Install pre-commit hooks
uv tool install pre-commit
pre-commit install

# Run tests
uv run pytest
```

---

## 📜 References

Agentics implements **Logical Transduction Algebra**, described in the following papers:

### Primary Reference
**Agentics 2.0: Logical Transduction Algebra for Agentic Data Workflows**
- Authors: Alfio Massimiliano Gliozzo, Junkyu Lee, Nahuel Defosse
- arXiv:2603.04241 [cs.AI, cs.LG] — https://arxiv.org/abs/2603.04241
- Submitted: March 4, 2026
- Abstract: Presents Agentics 2.0, a lightweight Python-native framework for building high-quality, structured, explainable, and type-safe agentic data workflows with Logical Transduction Algebra at its core.

### Foundational Work
**Transduction is All You Need for Structured Data Workflows**
- Authors: Alfio Gliozzo, Naweed Khan, Christodoulos Constantinides, Nandana Mihindukulasooriya, Nahuel Defosse, Junkyu Lee
- arXiv:2210.13952 [cs.CL, cs.AI, cs.IR]
- Foundational work on transduction-based approaches for structured data processing

### Applications & Use Cases

**An end-to-end agentic pipeline for smart contract translation and quality evaluation**
- Authors: Abhinav Goel, Chaitya Shah, Agostino Capponi, Alfio Gliozzo
- arXiv:2602.13808 [cs.AI, cs.SE] — https://arxiv.org/abs/2602.13808
- Submitted: February 14, 2026
- Application: End-to-end framework for LLM-generated smart contracts from natural-language specifications, with automated quality assessment through compilation and security checks using CrewAI-style agent teams.

**Semantic Trading: Agentic AI for Clustering and Relationship Discovery in Prediction Markets**
- Authors: Agostino Capponi, Alfio Gliozzo, Brian Zhu
- arXiv:2512.02436 [cs.AI] — https://arxiv.org/abs/2512.02436
- Submitted: December 2, 2025
- Application: Agentic AI pipeline for autonomous clustering of prediction markets into coherent topical groups and identification of hidden relationships using natural-language understanding.

**DAO-AI: Evaluating Collective Decision-Making through Agentic AI in Decentralized Governance**
- Authors: Agostino Capponi, Alfio Gliozzo, Chunghyun Han, Junkyu Lee
- arXiv:2510.21117 [cs.AI] — https://arxiv.org/abs/2510.21117
- Submitted: October 23, 2025
- Application: Empirical study of agentic AI as autonomous decision-makers in decentralized governance, analyzing 3K+ proposals from major protocols with realistic financial simulation.

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
- Abhinav Goel (Columbia University) — ag5252@columbia.edu
- Chaitya Shan (Columbia University) — cs4621@columbia.edu
- Brian Zi Qi Zhu (Columbia University) — bzz2101@columbia.edu

---

## 🔗 Additional Resources

- **Documentation**: [`docs/`](./docs/)
- **Tutorials**: [`tutorials/`](./tutorials/)
- **Examples**: [`examples/`](./examples/)
- **API Reference**: [`docs/references.md`](./docs/references.md)
- **Tools**: [`tools/`](./tools/) - Production-ready applications
