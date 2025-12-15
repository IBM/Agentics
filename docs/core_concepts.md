# 🧠 Core Concepts

Agentics is built around a small set of concepts that work together:

- **Pydantic types** – how you describe structured data  
- **Transducible functions** – LLM-powered, type-safe transformations  
- **Typed state containers (AGs)** – collections of typed rows/documents  
- **Logical Transduction Algebra (LTA)** – the formal backbone  
- **Map–Reduce** – the execution pattern for large workloads  

This page gives you the mental model you need before diving into code.

---

## 1. Pydantic Types: Describing Structured Data 📐

At the heart of Agentics is the idea that **everything is a type**.

You describe your data using **Pydantic models**:

```python
from pydantic import BaseModel

class Product(BaseModel):
    id: str | None = None
    title: str | None = None
    description: str | None = None
    price: float | None = None
```

These models serve three roles:

1. **Schema** – they define the fields, types, and optionality  
2. **Validation** – they validate inputs and outputs at runtime  
3. **Contract** – they act as the contract between your code and the LLM  

In Agentics, any LLM-powered transformation is expressed as:

> “Given a `Source` type, produce a `Target` type.”

Instead of prompt engineering around raw strings, you define **transformations between types**.

---

## 2. Transducible Functions: Typed LLM Transformations ⚙️

A **transducible function** is the core abstraction in Agentics.

Informally:

> A transducible function is an LLM-backed function  
> that maps inputs of type `Source` to outputs of type `Target`  
> under a set of instructions and constraints.

Conceptually:

```text
Target << Source
```

Example:

```python
from pydantic import BaseModel

class Review(BaseModel):
    text: str

class ReviewSummary(BaseModel):
    sentiment: str
    summary: str
```

A transducible function might be:

```python
fn: (Review) -> ReviewSummary
```

with instructions like:

> “Given a review, detect its sentiment (positive/negative/neutral) and produce a one-sentence summary.”

Key properties:

- **Typed I/O** – the function is bound to `Source` and `Target` Pydantic models.  
- **Single Source of Truth for Instructions** – instructions live alongside the function definition.  
- **LLM-Agnostic** – the function describes *what* to transform; the underlying model can change.  
- **Composable** – functions can be chained, branched, or merged into larger workflows.

You don’t call the LLM directly; you **call the transducible function**, which manages LLM calls, validation, retries, and evidence tracking.

---

## 3. Typed State Containers (AGs): Working with Collections 🗂️

Transformations rarely happen on a single object. You typically work with **collections** of items (rows, documents, events, etc.).

Agentics introduces **typed state containers** (AG) to:

- Hold a collection of instances of a given Pydantic type  
- Preserve that type information across operations  
- Provide a uniform interface for Map–Reduce, filtering, joining, etc.


Conceptually, you can think of an `AG[Source]` like a type-aware table:
 

```text
AG[Review]
  ├─ row 0: Review(text="…")
  ├─ row 1: Review(text="…")
  └─ row n: Review(text="…")
```

Applying a transducible function `(Review) -> ReviewSummary` over an `AG[Review]` conceptually yields an `AG[ReviewSummary]`.

Typed state containers give you:

- **Clarity** – you always know what type you’re holding.  
- **Safety** – operations can check types and schemas instead of guessing.  
- **Composability** – containers can flow between functions and stages.

You can think of state containers as the **data plane** of Agentics.


Note: The name Agentics is derived as a legacy from the first version of Agentics, in which data models and transformations were blended into the same object. By introducing transducible functions as first class citizens, Agentics 2.0 uses AGs primarily as a data structure, although it is still possible to use them directly for transformations. See agentics v1.0 documentation to learn more. 


---

## 4. Logical Transduction Algebra (LTA): The Formal Backbone 📚

Transducible functions and typed states are not just coding patterns; they are backed by a formal framework called **Logical Transduction Algebra (LTA)**.

You do **not** need to understand the full mathematics to use Agentics, but the intuition is important:

- **Transductions as Morphisms**  
  Each transducible function is treated as a morphism between types:  
  `Source ⟶ Target`.

- **Composability**  
  If you have `f: A ⟶ B` and `g: B ⟶ C`, then you can form a composite transduction `g ∘ f: A ⟶ C`. Agentics gives you a practical way to do this over LLM-based functions.


- **Explainability & Evidence**  
  Because transductions are modeled as structured mappings, Agentics can track **which fields** and **which steps** contributed to the final outputs. This underpins **evidence tracking** and **traceability**.

In short:

> LTA provides the theoretical foundation  
> for why your pipelines are composable and explainable,  
> even though they are powered by probabilistic models.

---

## 5. Map–Reduce: Scaling Transductions 🚀

Once you have:

- Typed collections (`AG[Source]`), and  
- Typed transformations (`Source -> Target`),

you need a way to run these at scale. Agentics uses a familiar pattern: **Map–Reduce**.

### 5.1 Map Phase (`amap`)

The **map** phase applies a transducible function to each element (or batch) of a collection.

Conceptually:

```text
list[Source]  --amap(f)-->  list[Target]
```

Where `f: Source -> Target`.

Properties:

- **Parallelizable** – each element can be processed independently.  
- **Asynchronous** – `amap` is designed for async I/O and concurrent execution.  
- **Typed In/Out** – both input and output containers carry their types.

Typical use cases:

- Extracting structured info from documents  
- Enriching rows with LLM-derived attributes  
- Normalizing or cleaning text fields at scale  

### 5.2 Reduce Phase (`areduce`)

The **reduce** phase aggregates a collection back into a smaller structure (often a single summary or global view).

```text
list[Target]  --areduce(g)-->  GlobalSummary
```

Where `g` is a transducible function or aggregation operation that takes many items and produces fewer (often one).

Examples:

- Summarizing a whole dataset into a report object  
- Producing global statistics or flags  
- Clustering and relation induction 

Map–Reduce in Agentics is a **logical pattern**, not tied to any specific infrastructure:

- `amap` = “apply a typed transformation to many items”  
- `areduce` = “aggregate many results into fewer structured outputs”

Together, they define how **large-scale reasoning workflows** are expressed in Agentics.

---

## 6. How the Concepts Fit Together 🔗

A typical workflow looks like this:

1. **Define your types**  
   Use Pydantic to describe your raw data (`Source`) and desired outputs (`Target`, `Report`, etc.).

2. **Define transducible functions**  
   For each logical step, define a transducible function:  
   extraction → normalization → classification → enrichment → summarization.

3. **Load data into typed state containers (Optional)**  
   Wrap your dataset into a container such as `AG[Source]`. 
   You can also use simple python lists of objects of the intended type. 


4. **Apply Map–Reduce**  
   - Use `amap` to apply transducible functions over the collection. 
   - Use `areduce` to build global summaries or reports.

5. **Rely on LTA properties**  
   Because everything is a typed transduction, you can:  
   - Compose steps cleanly,  
   - Trace outputs back to inputs,  
   - Reason about structure and invariants in your pipeline.

---

## 7. Summary ✅

- **Pydantic types** give you schemas and validation.  
- **Transducible functions** turn LLM calls into typed, reusable transformations.  
- **Typed state containers** hold collections of those types with clear semantics.  
- **Logical Transduction Algebra (LTA)** explains why these transformations compose and remain interpretable.  
- **Map–Reduce** provides the pattern for scaling these transductions to large datasets.

From here, you can explore:

- 👉 [Transducible Functions](transducible_functions.md) for concrete examples of defining and using transducible functions
- 👉 `types_and_states.md` for data modeling patterns  
- 👉 `mapreduce.md` to see how large-scale execution works in practice  
