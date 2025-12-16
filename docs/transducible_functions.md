# ⚙️ Transducible Functions

Transducible functions are the *workhorse* of Agentics.  
They turn “call this LLM with a prompt” into:

> **A typed, explainable transformation**  
> `T: X → Y` with guarantees about how each output field was produced.

This document explains what transducible functions are, how they work in Agentics, and how to use them in practice — including **dynamic generation** and **compositional patterns** using the `<<` operator.

---

## 1. What Is a Transducible Function?

Formally, a **transducible function** \(T : X \to Y\) is an *explainable* function that satisfies:

1. **Totality**  
   For every valid input \(x \in \llbracket X \rrbracket\), the function produces a valid output of type \(Y\).  
   > No silent failures: the function always returns some well-typed `Y`.

2. **Local Evidence**  
   Each output slot \(y_i\) is computed only from its *evidence subset* \(\mathcal{E}_i(x)\).  
   > No field is generated “from nowhere”: if `subject` appears in the output, we know which inputs and instructions it depended on.

3. **Slot-Level Provenance**  
   The mapping between input and output slots is explicit:  
   \[
   \mathcal{T}(y_i) = \mathcal{E}_i
   \]  
   This induces a bipartite graph between **input slots** and **output slots**, which acts as the *explainability trace* of the transduction.

Intuitively:

- An ordinary function only tells you *“here is the output.”*  
- A **transducible** function also tells you *“here is the output, and here is exactly which inputs I used and why”*

Transducible functions extend normal functions with **structural transparency at the slot level**.

---

## 2. Source and Target Types (X and Y) 📐

Agentics uses **Pydantic models** to represent the input type `X` and the output type `Y`.

```python
from pydantic import BaseModel, Field
from typing import Optional

class UserMessage(BaseModel):
    content: Optional[str] = None

class Email(BaseModel):
    """A simple email schema."""
    to: Optional[str] = Field(None, description="Recipient name or email address.")
    subject: Optional[str] = None
    body: Optional[str] = None
```

- `UserMessage` is our **Source** type (`X`).
- `Email` is our **Target** type (`Y`).

> **Recommendation**  
> In transduction scenarios, it is often useful to declare fields as `Optional[...] = None`.  
> This gives the LLM the ability to say *“I don’t have enough evidence for this field”* by leaving it `null`, instead of hallucinating content.

The transducible function we will define next will transform exactly **one** `UserMessage` into **one** `Email` (and later, we’ll see how to scale to lists).

---

## 3. Defining Transducible Functions

In Agentics, transducible functions are `async` Python functions that:

- Accept **exactly one** instance of the source type `X` as input.
- Return **exactly one** instance of the target type `Y`.

They can be defined in two main ways:

1. Using the **`@transducible()` decorator** on an async Python function.
2. **Dynamically generating** them from source and target types (e.g., via builders or the `<<` operator), with instructions and parameters.

This section starts with the decorator pattern and then moves to dynamic generation and composition.

---

## 4. The `@transducible()` Decorator

The decorator turns an ordinary async function into a transducible function. When decorated with `@transducible()`, your function can return either:

- A **concrete instance of the target type** `Y` (pure Python logic), or
- A special **`Transduce`** object wrapping an instance of the source type `X`, which means:

> “Send this source state to the LLM and let the model generate the target type `Y`.”

### 4.1 Example: Hybrid LLM + Programmatic Logic

```python
import re
from typing import Optional
from pydantic import BaseModel
from agentics.core.transducible_functions import transducible, Transduce

class UserMessage(BaseModel):
    content: Optional[str] = None

class Email(BaseModel):
    to: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
```

#### LLM-driven email generation

```python
@transducible()
async def write_email_with_llm(state: UserMessage) -> Email:
    """Write a full email about the provided content.
    The LLM is allowed to elaborate and make up reasonable details."""
    # Optionally mutate or pre-process state here
    return Transduce(state)
```

Here, `Transduce(state)` signals:

- “Use the transduction engine with this `UserMessage` as evidence.”
- The LLM will generate an `Email` instance, respecting the schema.

#### Programmatic email extraction (no LLM)

```python
@transducible()
async def write_email_programmatically(state: UserMessage) -> Email:
    pattern = r"^(Hi|Dear|Hello|Hey)\s+([^,]+),\s*(.+)$"
    match = re.match(pattern, state.content or "")
    if match:
        greeting, name, body = match.groups()
        return Email(to=name, body=body)
    # Not enough evidence → return an empty Email
    return Email()
```

This function is also **transducible**, even if it does not call any LLM:

- It still respects totality: for any `UserMessage` it returns a valid `Email`.
- Local evidence is explicit: `to` and `body` come directly from `content`.
- Slot-level provenance is trivial: each field maps to a substring in `content`.

Because *both* functions are transducible, they can be composed, traced, and plugged into Map–Reduce pipelines in exactly the same way.

---

## 5. Executing Transducible Functions

You call a transducible function just like any other async function:

```python
message = UserMessage(
    content="Hi Lisa, I made great progress with the new release of Agentics 2.0"
)

target1 = await write_email_with_llm(message)
target2 = await write_email_programmatically(message)
```

### 5.1 Example Outputs

`target1` (LLM-based) may return something like:

```json
{
  "to": "Lisa",
  "subject": "Update on Agentics 2.0",
  "body": "Hi Lisa,\n\nI wanted to share some exciting news about the new release of Agentics 2.0. Over the past week, I made great progress on the features we discussed...\n\nBest regards,\n[Your Name]"
}
```

`target2` (programmatic) will deterministically return:

```json
{
  "to": "Lisa",
  "subject": null,
  "body": "I made great progress with the new release of Agentics 2.0"
}
```

A few important observations:

- The LLM output is **stochastic**: repeated calls may differ in style, but must remain logically transducible and semantically aligned with the evidence.
- The programmatic output is **deterministic** and brittle (it depends strictly on the regex).
- In practice, you combine both patterns:
  - Use deterministic logic when the pattern is simple and strict.
  - Use LLM-based transduction when structure is fixed but content is open-ended.

---

## 6. Dynamic Generation & Composition of Transducible Functions

Beyond the decorator, Agentics lets you **generate and compose** transducible functions *dynamically* using the **`<<` operator** and helpers such as `With(...)`.

Conceptually, the operator implements:

> **Typed transduction construction**  
> `Y << X` means: *“Build a transducible function that maps from type `X` to type `Y`.”*  

You can use it with:

- **Types** (`Y << X`),
- **Existing transducible functions** (`Y << f`), and
- **Configuration wrappers** (`Y << With(X, ...)`).

### 6.1 Minimal Setup

```python
from pydantic import BaseModel, Field
from typing import Optional
from agentics.core.transducible_functions import With

class GenericInput(BaseModel):
    content: Optional[str] = None

class Email(BaseModel):
    """Email generated from a generic input."""
    to: Optional[str] = Field(None, description="Recipient of the email.")
    subject: Optional[str] = None
    body: Optional[str] = None
```

---

### 6.2 Dynamic Generation with `<<` (Type → Function)

The simplest form of dynamic generation is:

```python
write_mail = Email << GenericInput
```

This constructs a transducible function:

```text
write_mail: GenericInput → Email
```

Usage:

```python
input_state = GenericInput(
    content="Write a news story on Zoran Mandani winning the election in NYC and send it to Alfio"
)

mail = await write_mail(input_state)
print(mail.model_dump_json(indent=2))
```

Here, `Email << GenericInput` tells Agentics:

- “Create an LLM-backed transducible function that maps a `GenericInput` into an `Email`.”
- The default instructions depend on your configuration and global defaults (or you can refine them via `With`, shown below).

---

### 6.3 Composing Transductions with `<<`

You can build **multi-step pipelines** by composing transducible functions and types using `<<`.

Suppose we want to add a **summary** step on top of the email:

```python
class Summary(BaseModel):
    summary_text: Optional[str] = None
```

#### 6.3.1. Two-step composition

```python
input_state = GenericInput(
    content="Write news story on Zoran Mandani winning the election in NYC and send it to Alfio"
)

write_mail = Email << GenericInput             # GenericInput → Email
summary_from_email = Summary << Email          # Email → Summary

# Composition by function application
mail = await write_mail(input_state)
summary = await summary_from_email(mail)

print(mail.model_dump_json(indent=2))
print(summary.model_dump_json(indent=2))
```

#### 6.3.2. Composition via `<<` on functions

You can also let `<<` perform the composition directly:

```python
# Compose Summary on top of write_mail
summary_composite_1 = Summary << write_mail   # GenericInput → Summary

summary1 = await summary_composite_1(input_state)
print(summary1.model_dump_json(indent=2))
```

Or inline:

```python
summary_composite_2 = Summary << (Email << GenericInput)
summary2 = await summary_composite_2(input_state)
print(summary2.model_dump_json(indent=2))
```

In all cases, the pipeline is:

```text
GenericInput  →  Email  →  Summary
```

but you can choose whether to:

- Write the steps explicitly, or
- Build them into a single composed transducible function.

---

### 6.4 Using `With(...)` for Configured Dynamic Transduction

The `With(...)` helper lets you **attach instructions and options** to dynamic transductions.

Example: first generate an email, then rewrite it into a compact summary:

```python
from agentics.core.transducible_functions import With

class Summary(BaseModel):
    summary_text: Optional[str] = None

# A basic dynamic transduction
write_mail = Email << GenericInput

# A configured transduction: Email → Summary
summarize = Summary << With(
    Email,
    instructions="Rewrite the email into a concise summary.",
    enforce_output_type=True,
    verbose_transduction=False,
)

input_state = GenericInput(
    content="Zoran Mandani won the election in NYC. Draft a message to the press list."
)

mail = await write_mail(input_state)
summary = await summarize(mail)

print(mail.model_dump_json(indent=2))
print(summary.model_dump_json(indent=2))
```

Here:

- `With(Email, ...)` tells Agentics:  
  *“When you see an `Email` as input, apply these instructions and guarantees to produce a `Summary`.”*
- `enforce_output_type=True` strengthens validation so outputs **must** conform to `Summary`.
- `verbose_transduction=False` keeps logs / metadata minimal (implementation-dependent).

Because `Summary << With(Email, ...)` is still a transducible function, you can compose it further, call it on lists, or plug it into Map–Reduce.

---

#### 6.5. Adding explanations with `With(...)`

You can also ask for a structured explanation of the classification:

```python
classify_genre = Genre << With(
    Movie,
    provide_explanation=True,
)

genre, explanation = await classify_genre(movie)
print(genre.model_dump_json(indent=2))
print(explanation.model_dump_json(indent=2))
```

Here, `provide_explanation=True` configures the dynamic transduction so that:

- The first output is the typed `Genre`.
- The second output is an explanation object (typically another Pydantic model),  
  capturing *why* the classifier picked that genre.

This pattern generalizes:

- `With(..., provide_explanation=True)` can be used with other source/target pairs.
- Explanations can be logged, inspected, or surfaced in UI as **transparent justification** for the model’s decision.

---

## 7. Map–Reduce: Scaling Transducible Functions 🚀

When wrapped by `@transducible()` **or** created dynamically with `<<`, transducible functions are overloaded to accept **lists** of `X` as well. When called this way, they return a corresponding list of `Y`:

```python
messages = [
    UserMessage(content="Hi John, I made great progress with Agentics."),
    UserMessage(content="Hi , I fixed the last blocking bug in the pipeline."),
]

emails = await write_email_with_llm(messages)
```

Under the hood, Agentics uses an **asynchronous Map** operation:

- Conceptually:  
  `amap(write_email_with_llm, messages) -> list[Email]`
- Each element is processed independently, enabling concurrency and parallelism.
- This pattern scales to **batch inference, dataset scans, and large evidence extraction tasks**.

Later, you can combine this with **Reduce** operations (e.g., summarizing all emails into a single report), forming full Map–Reduce pipelines over typed states.

---

## 8. Evidence, Provenance, and Explainability

Because transducible functions are defined over explicit types and carry evidence subsets, Agentics can:

- Track which input fields contributed to each output field.
- Represent this as a **bipartite graph** between input and output slots.
- Attach this trace as **metadata** to your states (depending on your Agentics configuration).

For example, in the email examples:

- `Email.to` is mapped to (a span inside) `UserMessage.content`.
- `Email.subject` may depend on the *entire* `content`.
- `Email.body` is mostly grounded in `content`, plus stylistic priors from instructions.

This is critical when you:

- Need **auditable** LLM behavior.  
- Want to debug why a particular field was generated.  
- Need to enforce *“no hallucination from outside these inputs”* policies.

---

## 9. When to Create a New Transducible Function

In a real system, you’ll typically end up with many small, focused transducible functions instead of one giant one.

Good reasons to define a separate transducible function:

- You’re doing a logically distinct step:
  - e.g., *extract entities*, *normalize names*, *classify intent*, *summarize conversation*.
- You want to **test** and **benchmark** that step independently.
- You expect to **reuse** it across pipelines.
- You need different **instructions, constraints, or safety properties** for that stage.

Think of transducible functions as the **operators** of your Logical Transduction Algebra.

---

## 10. Summary ✅

- A **transducible function** is a typed, explainable mapping `T: X → Y` with:
  - **Totality**, **Local Evidence**, and **Slot-Level Provenance**.
- In Agentics:
  - Inputs and outputs are modeled as Pydantic types (`X`, `Y`).
  - You can define transducible functions via:
    - The `@transducible()` decorator,
    - Dynamic builders like `make_transducible_function`, and
    - The `<<` operator (with or without `With(...)`).
  - Functions can be purely programmatic, purely LLM-based, or hybrid.
- Transducible functions:
  - Scale from **single calls** to **batch Map–Reduce** workloads.
  - Expose structured explainability traces for each output field.
  - Compose into robust, interpretable, large-scale reasoning pipelines.

From here you can explore:

- `core_concepts.md` – the broader mental model (types, states, LTA, Map–Reduce).  
- `mapreduce.md` – how Agentics orchestrates large-scale transductions over typed state containers.  
- `types_and_states.md` – how to design good schemas and manage collections of states.
