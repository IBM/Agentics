
# Agentics

Agentics is a lightweight, Python-native framework for building **structured and massively parallel agentic workflows** using Pydantic models and transducible functions.  Designed for seamless integration with modern LLMs and agentics frameworks, Agentics introduces a principled way to define **typed input/output schemas** and execute **declarative transformations**—called *transducible functions*—that are composable, explainable, and robust at scale, by offering a native **MapReduce** framework.

At its foundation, agentics builds on **Logical Transduction Algebra**, a logico-mathematical formalism that guarantees composability and explainability of LLM based transducible functions. 

This approach enables developers to construct reliable, interpretable, and modular reasoning pipelines without the overhead of heavyweight orchestrators. As a result, Agentics coding is **simple, streamlined, predictable,  robust and scalable**. 

---

## 🔑 Key Features

### ⚙️ **Transducible Functions**
Define LLM-powered functions as structured transformations between Pydantic types, with automatic schema validation and type-constrained generation.

### 🚀 **Async Map–Reduce Scale-Out**
Run large workloads efficiently using fully asynchronous `amap` and `areduce`, enabling high-throughput parallelism and distributed-ready pipelines.

### 🧩 **Dynamic Function & Type Composition**
Compose functions declaratively, merge or synthesize types on the fly, and build adaptive workflows that evolve with your data.

### 🔍 **Explainable & Traceable Inferences**
Every output carries evidence links that map generated attributes back to their originating input fields—transparent, auditable LLM reasoning.

### 🛡️ **Type-Safe & Robust**
Powered by Pydantic models end-to-end, enforcing structural correctness, validation, and predictable runtime behavior across all transductions.

### ✨ **Minimalistic Design Patterns**
A clean, Pythonic API based on simple primitives—types, functions, and transductions—without heavy frameworks or orchestration layers.




## 📚 Documentation Overview

This documentation introduces the core concepts behind Agentics and provides everything you need to start building structured, agentic workflows using its data model and transduction framework.

⸻

👉 [Getting Started](getting_started.md): Learn how to install Agentics, set up your environment, and run your first logical transduction.

📘 [Why Agentics?](background.md): Understand the foundational principles and architecture of the Agentics framework.

🚀 [Use Cases](use_cases.md): Explore real-world scenarios where Agentics enhances data intelligence and reasoning capabilities.

🧠  [Agentics](agentics.md): See how Agentics wraps pydantic models into transduction-ready agents for structured execution.

🔁 [Transduction](transduction.md): Discover how the << operator enables logical transduction between types and how to customize its behavior.

🧬  [Memory](memory.md): Use external knowledge from documents to augment transduction through the built-in memory system.

🛠️ [Tools](tools.md): Integrate with external frameworks like LangChain or CrewAI to provide dynamic access to data sources during transduction.
