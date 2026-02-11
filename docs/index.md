# 🌐 Agentics

Agentics is a lightweight, Python-native framework for building **structured and massively parallel agentic workflows** using Pydantic models and **transducible functions** . 

---

## 📚 Documentation Overview

### Core Documentation

- **[Getting Started](getting_started.md)** 🚀
  Install Agentics, set up your environment, and run your first transducible function over a small dataset.

- **[Core Concepts](core_concepts.md)** 🧠
  The mental model: Pydantic types, transducible functions, typed state containers, Logical Transduction Algebra (LTA), and Map–Reduce.

- **[Transducible Functions](transducible_functions.md)** ⚙️
  How to define, configure, and invoke transducible functions; specifying instructions; controlling temperature, retries, and structured decoding.

- **[Map-Reduce Operations](map_reduce.md)** 🔁
  Scaling transducible functions with map and reduce operations, batch processing patterns, and hierarchical reduction strategies.

- **[Agentics (AG)](agentics.md)** 🧬
  Working with `AG` typed state containers, loading data from JSON/CSV/DataFrames, and preserving type information across the pipeline.

### Advanced Topics

- **[Performance Optimization](optimization.md)** ⚡
  Batch size tuning, persisting intermediate results, error handling, retries, and performance benchmarking.

- **[Tool Integration](tool_integration.md)** 🔌
  Using MCP tools, web search, databases, custom tools, and best practices for tool orchestration.

### Tutorials & Examples

- **[Logical Transduction Algebra](../tutorials/logical_transduction_algebra.ipynb)** 🔁
  Interactive tutorial: Chaining transducible functions, branching, fan-in/fan-out patterns, and building reusable pipeline components.

- **[Map-Reduce Tutorial](../tutorials/map_reduce.ipynb)** 🚀
  Interactive tutorial: Using `amap` and `areduce` for large-scale runs, batching strategies, handling failures, and performance considerations.

- **[Examples & Use Cases](../examples)** 📘
  End-to-end examples: text-to-SQL, data extraction and enrichment, classification, document workflows, evaluation pipelines, and more.



---

## Transducible Functions

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

### 🔌 Tool Integration

Agentics is fully compatible with Model Context Protocol (MCP) and expose external tools and knowledge to transducible functions:

- 🌐 Web / search tools  
- 🗄️ Databases & vector stores  
- 💻 Code execution backends  
- 🔗 MCP-based tools  

---

### ✨ Minimalistic, Pythonic API

The framework is intentionally small:

- 🚫 No custom DSL to learn  
- 🐍 Just Python functions, Pydantic models, and a few core primitives  

---

## 📖 Glossary

**AG (Agentics)**  
Short for "Agentics". A typed state container that wraps a list of Pydantic objects, enabling structured transductions. Used as `AG[Type]` or simply `AG(atype=Type)`. The recommended way to work with collections of typed data.

**Agentics**  
The full name of the framework and the class name for typed state containers. In code, typically imported and used as `AG` for brevity.

**Transducible Function**  
A typed, explainable function that maps inputs of type `Source` to outputs of type `Target`. Defined using the `@transducible()` decorator or dynamically with the `<<` operator. Guarantees totality, local evidence, and slot-level provenance.

**Transduction**  
The process of transforming data from one typed structure to another using LLM-powered reasoning. Unlike simple mapping, transduction preserves semantic relationships and provides explainability.

**Logical Transduction Algebra (LTA)**  
The formal mathematical framework underlying Agentics. Treats transductions as morphisms between types, enabling composition, explainability, and stability guarantees.

**`<<` Operator (Left Shift)**  
The transduction operator. `Target << Source` creates a transducible function that maps `Source` to `Target`. Can be used with types, instances, or existing functions for composition.

**`With()` Function**  
A helper that wraps a source type with configuration parameters. Used as `Target << With(Source, instructions="...", tools=[...])` to create configured transducible functions dynamically.

**TransductionResult**  
A wrapper object returned when `provide_explanation=True`. Supports automatic unpacking into `(value, explanation)` tuples or single value assignment.

**AType**  
Short for "Agentics Type". The Pydantic model class that defines the schema for all instances in an AG container. Accessed via `ag.atype`.

**Map-Reduce**  
The execution pattern for scaling transductions. `amap` applies a function to each element in parallel; `areduce` aggregates results into a summary.

**MCP (Model Context Protocol)**  
A standard protocol for exposing tools (web search, databases, APIs) to LLMs. Agentics supports MCP tools via the `tools` parameter.

**Evidence**  
The subset of input fields that contributed to generating a specific output field. Tracked automatically to enable explainability and provenance.

**Slot**  
A field in a Pydantic model. "Slot-level provenance" means tracking which input slots contributed to each output slot.

- 🌉 Easy to embed into existing stacks (LangFlow nodes, CrewAI agents, MCPs, etc.)  
