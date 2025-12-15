# 🌐 Agentics

Agentics is a lightweight, Python-native framework for building **structured and massively parallel agentic workflows** using Pydantic models and **transducible functions** 💫.

A **transducible function** is an LLM-powered, type-safe transformation between Pydantic models. Agentics lets you:

- Define these transformations **declaratively**  
- Compose them into **pipelines**  
- Execute them at scale using an asynchronous **Map–Reduce** execution engine ⚙️

Under the hood, Agentics is grounded in **Logical Transduction Algebra (LTA)**, a logico-mathematical formalism that guarantees:

- ✅ Composability  
- ✅ Explainability  
- ✅ Stability of LLM-based transformations  

The result is a way to build agentic systems that are:

- **Typed** – every step has explicit input/output schemas 📐  
- **Composable** – pipelines are built from reusable transducible functions 🧩  
- **Traceable** – outputs carry evidence back to input fields 🔍  
- **Scalable** – async `amap` / `areduce` primitives support large workloads 🚀  
- **Minimal** – no heavy orchestrators: just types, functions, and data 🪶  

Agentics code is **simple, predictable, and robust**, and is easy to embed into modern ecosystems (LangFlow, LangChain, CrewAI, MCP, etc.) 🤝.

---

## 🔑 Key Features

### ⚙️ Transducible Functions (Core Abstraction)

Define LLM-powered transformations as first-class functions:

- 🧾 Typed input and output via Pydantic models  
- 🛡️ Automatic schema validation and type-constrained generation  
- 🪜 Composable into higher-level workflows and chains  

---

### 🧱 Typed State Containers - a.k.a. Agentics (AG) 

Wrap data into typed state collections so that every row or document carries a concrete Pydantic type:

- Safe, batch-level operations ✅  
- Clear semantics over datasets and intermediate states 📊  
- Input/output from DBs, CSV and Json
- Ideal to represent tabular/structured data

---

### 🚀 Async Map–Reduce Execution

Run transducible functions over large collections using:

- ⚡ `amap` for massively parallel application  
- 📉 `areduce` for aggregations and global summaries  

Designed to scale on multi-core or distributed execution backends 🖥️🖥️🖥️.

---

### 🧩 Dynamic Type & Function Composition

Create new workflows on the fly:

- 🔄 Merge or refine types dynamically  
- 🧬 Compose transducible functions declaratively  
- 🔀 Build polymorphic or adaptive pipelines driven by data and instructions  

---

### 🔍 Explainable & Traceable Inference

Each generated attribute can be traced back to:

- Specific input fields 🧷  
- The specific transducible function or step that produced it 🧠  

This enables **auditable, debuggable** LLM reasoning across the pipeline.

---

### 🛡️ End-to-End Type Safety

Pydantic models are enforced at every boundary:

- ✅ Validation on input loading  
- ✅ Validation after each transducible function  
- ✅ Predictable runtime behavior and clear failure modes  

---

### 🔌 Tool & Memory Integration

Expose external tools and knowledge to transducible functions:

- 🌐 Web / search tools  
- 🗄️ Databases & vector stores  
- 💻 Code execution backends  
- 🔗 MCP-based tools  

---

### ✨ Minimalistic, Pythonic API

The framework is intentionally small:

- 🚫 No custom DSL to learn  
- 🐍 Just Python functions, Pydantic models, and a few core primitives  
- 🌉 Easy to embed into existing stacks (LangFlow nodes, CrewAI agents, MCPs, etc.)  

---

## 📚 Documentation Overview

This documentation goes from core concepts → advanced patterns → integrations.  

---

### 1️⃣ Foundations

- **[Getting Started](getting_started.md)** 🚀  
  Install Agentics, set up your environment, and run your first transducible function over a small dataset.

- **[Core Concepts](core_concepts.md)** 🧠  
  The mental model: Pydantic types, transducible functions, typed state containers, Logical Transduction Algebra (LTA), and Map–Reduce.

- **[Why Agentics?](why_agentics.md)** ❓  
  Design philosophy, comparison with traditional orchestrators and agent frameworks, and guidance on when to use Agentics in your stack.

---

### 2️⃣ Transducible Functions & Types

- **[Transducible Functions](transducible_functions.md)** ⚙️  
  How to define, configure, and invoke transducible functions; specifying instructions; controlling temperature, retries, and structured decoding.

- **[Type System & State Containers](types_and_states.md)** 🧬  
  Defining Pydantic models for inputs/outputs, working with `AG` containers, loading data from JSON/CSV/DataFrames, and preserving type information across the pipeline.

- **[Composition & Pipelines](composition.md)** 🔁  
  Chaining transducible functions, branching, fan-in/fan-out patterns, and building reusable pipeline components.

---

### 3️⃣ Execution & Scaling

- **[Async Map–Reduce Execution](mapreduce.md)** 🚀  
  Using `amap` and `areduce` for large-scale runs, batching strategies, handling failures, and performance considerations.

- **[Monitoring, Logging & Traces](monitoring.md)** 📈  
  Capturing intermediate states, tracing evidence links, logging LLM calls, and integrating with external observability tools.

---

### 4️⃣ Memory, Tools & Integrations

- **[Memory & Knowledge Integration](memory.md)** 🧠📚  
  Attaching external knowledge sources (documents, vector stores, APIs) to transducible functions and enriching states with retrieved context.

- **[Tools & External Actions](tools.md)** 🛠️  
  Exposing search, code execution, databases, or MCP tools to transducible functions; patterns for safe tool-calling from within a transduction.

- **[Ecosystem Integrations](integrations.md)** 🌉  
  Using Agentics together with:
  - LangFlow components 🎛️  
  - LangChain tools/agents 🔗  
  - CrewAI agents and MCP tools 🤖  
  - Other LLM backends and orchestration layers 🌐  

---

### 5️⃣ Patterns, Examples & Reference

- **[Patterns & Use Cases](use_cases.md)** 📘  
  End-to-end examples: text-to-SQL, data extraction and enrichment, classification, document workflows, evaluation pipelines, and more.

- **[Testing & Validation](testing.md)** ✅  
  Unit-testing transducible functions, golden-set evaluation, and regression testing for LTA-based workflows.

- **[API Reference](api_reference.md)** 📖  
  Detailed public API reference: core classes, helpers for transducible functions, state containers, and configuration objects.
