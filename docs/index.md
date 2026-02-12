# 🌐 Agentics

Agentics is a lightweight, Python-native framework for building **structured and massively parallel agentic workflows** using Pydantic models and **transducible functions**. 

---

## 📚 Documentation Overview

### Core Documentation

- **[Getting Started](getting_started.md)** 🚀
  Install Agentics, set up your environment, and run your first transducible function over a small dataset.

- **[Core Concepts](core_concepts.md)** 🧠
  Pydantic types, transducible functions, typed state containers, Logical Transduction Algebra (LTA), and Map–Reduce.

- **[Transducible Functions](transducible_functions.md)** ⚙️
  How to define, configure, and execute transducible functions.
  Understanding dynamic generation and composition of transducible functions, batch processing, and provenance of generation.

- **[Map-Reduce Operations](map_reduce.md)** 🔁
  Scaling transducible functions with map and reduce operations, batch processing patterns, and best practices.

- **[Semantic Operators](semantic_operators.md)** 🔍
  High-level declarative API for data transformations using natural language. Includes `sem_map`, `sem_filter`, `sem_agg`, and more LOTUS-style operations.

- **[Agentics (AG)](agentics.md)** 🧬
  Working with `AG` typed state containers, loading data from JSON/CSV/DataFrames, and preserving type information across the pipeline.

### Advanced Topics

- **[Performance Optimization](optimization.md)** ⚡
  Batch size tuning, persisting intermediate results, performance optimization strategies, performance benchmarking, error handling, and best practices.

- **[Tool Integration](tool_integration.md)** 🔌
  Using MCP tools, tool usage patterns, custom tools, and best practices.

### Tutorials & Examples

- **[Logical Transduction Algebra](../tutorials/logical_transduction_algebra.ipynb)** 🔁
  Interactive tutorial: Chaining transducible functions, branching, fan-in/fan-out patterns, and building reusable pipeline components.

- **[Map-Reduce Tutorial](../tutorials/map_reduce.ipynb)** 🚀
  Interactive tutorial: Using `amap` and `areduce` for large-scale runs, batching strategies, handling failures, and performance considerations.

- **[Examples & Use Cases](../examples)** 📘
  End-to-end examples: text-to-SQL, data extraction and enrichment, classification, document workflows, evaluation pipelines, and more.




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


## Documentation

This documentation page is written using Mkdocs. 
You can start the server to visualize this interactively.
```bash
mkdocs serve
```
After started, documentation will be available here [http://127.0.0.1:8000/](http://127.0.0.1:8000/)
