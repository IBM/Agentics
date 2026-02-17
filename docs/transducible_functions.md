# ⚙️ Transducible Functions

Transducible functions are the *workhorse* of Agentics.  
They turn “call this LLM with a prompt” into:

> **A typed, explainable transformation**  
> `T: X → Y` with explanation about how each output field was produced.

This document explains what transducible functions are, how they work in Agentics, and how to use them in practice — including **dynamic generation** and **compositional patterns** using the `<<` operator.

---

## 1. What Is a Transducible Function?

Formally, a **transducible function** `T: X → Y` is an *explainable* function that satisfies:


1. **Local Evidence**  
   Each output slot **yᵢ*** is computed only from its *evidence subset* **Eᵢ(x)**.  
   > No field is generated “from nowhere”: if `subject` appears in the output, we know which inputs and instructions it depended on.

2. **Slot-Level Provenance** 
   The mapping between input and output slots is explicit: **T(yᵢ) = Eᵢ**

   This induces a bipartite graph between **input slots** and **output slots**, which acts as the *explainability trace* of the transduction.

Intuitively:

- An ordinary function only tells you *“here is the output.”*  
- A **transducible** function also tells you *“here is the output, and here is exactly which inputs I used and why”*

Transducible functions extend normal functions with **structural transparency at the slot level**.

---

## 2. Source and Target Types 📐

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
> This gives an LLM the ability to say *“I don’t have enough evidence for this field”* by leaving it `null`, instead of hallucinating content.

The transducible function we will define next will transform exactly **one** `UserMessage` into **one** `Email` (and later, we’ll see how to scale to lists).

---

## 3. Defining Transducible Functions

In Agentics, transducible functions are `async` Python functions that:

- Accept **exactly one** instance of the source type `X` as input.
- Return **exactly one** instance of the target type `Y`.

They can be defined in two main ways:

1. Using the **`@transducible()` decorator** on an async Python function.
2. **Dynamically generating** them from source and target types (e.g., via builders or the `<<` operator), with instructions and parameters.


---

## 4. The `@transducible()` Decorator

This section starts with the decorator pattern and then moves to dynamic generation and composition.
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
    content="Write a news story on the winner of Super Bowl in 2025 and send it to Alfio."
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
    content="Write a news story on the winner of Super Bowl in 2025 and send it to Alfio."
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
    content="Philadelphia Eagles won Super Bowl 2025. Draft a message to the press list."
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

### 6.5 `With()` Function Reference

The `With()` function creates a `TransductionConfig` object that wraps a source model with configuration parameters. It's used with the `<<` operator to create configured transducible functions dynamically.

**Signature:**
```python
def With(model: Type[BaseModel], **kwargs) -> TransductionConfig
```

**Parameters:**

All parameters from the `@transducible()` decorator are supported:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `instructions` | `str` | `""` | Custom instructions for the LLM on how to perform the transduction |
| `tools` | `list[Any]` | `[]` | List of tools (MCP, CrewAI, or LangChain) available during transduction |
| `enforce_output_type` | `bool` | `False` | If `True`, raises `TypeError` if output doesn't match target type |
| `llm` | `Any` | `AG.get_llm_provider()` | LLM provider to use (OpenAI, WatsonX, Ollama, etc.) |
| `reasoning` | `bool` | `False` | Enable reasoning mode for complex transductions |
| `max_iter` | `int` | `10` | Maximum iterations for agentic reasoning loops |
| `verbose_transduction` | `bool` | `True` | Print detailed transduction logs |
| `verbose_agent` | `bool` | `False` | Print agent-level execution logs |
| `batch_size` | `int` | `10` | Number of items to process in parallel batches |
| `provide_explanation` | `bool` | `False` | Return explanation alongside result (see Section 6.6) |
| `timeout` | `int` | `300` | Timeout in seconds for each transduction |
| `post_processing_function` | `Callable` | `None` | Function to apply to outputs after transduction |
| `persist_output` | `str` | `None` | Path to save intermediate batch results |
| `transduce_fields` | `list[str]` | `None` | Specific fields to use for transduction |
| `prompt_template` | `str` | `None` | Custom prompt template for the LLM |
| `areduce` | `bool` | `False` | Use reduce mode instead of map (for aggregations) |

**Usage Patterns:**

```python
# Basic usage with instructions
classify = Genre << With(Movie, instructions="Classify the movie genre")

# Multiple parameters
enrich = EnrichedData << With(
    RawData,
    instructions="Enrich with external data",
    tools=[web_search_tool],
    batch_size=20,
    timeout=600,
    provide_explanation=True
)

# Comparison: With() vs @transducible()
# These are equivalent:

# Using With()
fn1 = TargetType << With(SourceType, instructions="Transform data")

# Using decorator
@transducible(instructions="Transform data")
async def fn2(state: SourceType) -> TargetType:
    return Transduce(state)
```

**When to use `With()` vs `@transducible()`:**

- Use `With()` for **dynamic, one-off transductions** where you don't need a named function
- Use `@transducible()` for **reusable functions** that you'll call multiple times or compose into larger workflows
- `With()` is ideal for **exploratory work** and **inline transformations**
- `@transducible()` is better for **production code** with clear function names and documentation

---

### 6.6 Result Unpacking with `TransductionResult`

When you set `provide_explanation=True` (either in `@transducible()` or `With()`), the transduction returns a `TransductionResult` object that supports automatic unpacking.

**The `TransductionResult` Class:**

```python
class TransductionResult:
    def __init__(self, value, explanation):
        self.value = value          # The actual transduced output
        self.explanation = explanation  # Explanation of how it was derived
    
    def __iter__(self):
        yield self.value
        yield self.explanation
```

**Automatic Unpacking Behavior:**

The framework automatically detects how you assign the result:

```python
# Single assignment - get only the value
result = await classify_genre(movie)
print(result.genre)  # Access the Genre object directly

# Tuple unpacking - get both value and explanation
genre, explanation = await classify_genre(movie)
print(genre.genre)  # The Genre object
print(explanation.reasoning)  # The explanation object
```

**Example with Decorator:**

```python
@transducible(provide_explanation=True)
async def classify_genre(state: Movie) -> Genre:
    """Classify the genre of the source Movie."""
    return Transduce(state)

movie = Movie(
    movie_name="The Godfather",
    description="Crime family drama",
    year=1972
)

# Get both result and explanation
genre, explanation = await classify_genre(movie)

print(f"Genre: {genre.genre}")
print(f"Reasoning: {explanation.reasoning}")
print(f"Confidence: {explanation.confidence}")
```

**Example with `With()`:**

```python
classify_genre = Genre << With(
    Movie,
    provide_explanation=True,
    instructions="Classify based on plot and themes"
)

# Tuple unpacking works the same way
genre, explanation = await classify_genre(movie)
```

**Batch Processing with Explanations:**

When processing lists, each item gets its own explanation:

```python
movies = [movie1, movie2, movie3]

# Returns list of values and list of explanations
genres, explanations = await classify_genre(movies)

for genre, explanation in zip(genres, explanations):
    print(f"{genre.genre}: {explanation.reasoning}")
```

**Note:** If you don't need explanations, simply omit `provide_explanation=True` and the function returns only the transduced value(s).

---

---

## 7. Batch Processing with Lists

Transducible functions automatically support batch processing. When you pass a list of items, they are processed efficiently:

```python
messages = [
    UserMessage(content="Hi John, I made great progress with Agentics."),
    UserMessage(content="Hi, I fixed the last blocking bug in the pipeline."),
]

# Automatically processes all messages
emails = await write_email_with_llm(messages)
```

For detailed information on Map-Reduce operations, scaling to large datasets, and aggregation patterns, see the dedicated [Map-Reduce](map_reduce.md) documentation.

---

## 8. Evidence, Provenance, and Explainability

Because transducible functions are defined over explicit types and carry evidence subsets, Agentics can:

- Track which input fields contributed to the output.
- Attach this trace as **metadata** to your states (depending on your Agentics configuration).

For example, in the email examples:

- `Email.to` is mapped to (a span inside) `UserMessage.content`.
- `Email.subject` may depend on the *entire* `content`.
- `Email.body` is mostly grounded in `content`, plus stylistic priors from instructions.

This is critical when you:

- Need **auditable** LLM behavior.
- Want to debug why a particular field was generated.
- Need to enforce *"no hallucination from outside these inputs"* policies.

---

## 9. When to Create a New Transducible Function

In a real system, you'll typically end up with many small, focused transducible functions instead of one giant one.

Good reasons to define a separate transducible function:

- You're doing a logically distinct step:
  - e.g., *extract entities*, *normalize names*, *classify intent*, *summarize conversation*.
- You want to **test** and **benchmark** that step independently.
- You expect to **reuse** it across pipelines.
- You need different **instructions, constraints, or safety properties** for that stage.


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

---

## Next
- 👉 **[Transducible Functions Tutorial](../tutorials/transducible_functions.ipynb)** to see how transducible functions works in practice
- 👉 **[Map-Reduce Operations](map_reduce.md)** - Scaling with map and reduce, batch processing patterns

## Go to Index
- 👉 [Index](index.md)
