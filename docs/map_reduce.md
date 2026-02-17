# 🔁 Map-Reduce Operations

Map-Reduce is the execution pattern for scaling transducible functions to large datasets. Agentics provides built-in support for both **map** (parallel transformation) and **reduce** (aggregation) operations over typed collections.

---

## Overview

When you define a transducible function, it automatically supports both single-item and batch processing:

```python
from pydantic import BaseModel

class UserMessage(BaseModel):
    content: str

class Email(BaseModel):
    to: str
    subject: str
    body: str

@transducible()
async def write_email(message: UserMessage) -> Email:
    """Convert a message into a professional email."""
    return Transduce(message)

# Single item
email = await write_email(UserMessage(content="Hi John, great progress!"))

# Batch processing (automatic map)
messages = [
    UserMessage(content="Hi John, I made great progress with Agentics."),
    UserMessage(content="Hi, I fixed the last blocking bug in the pipeline."),
]
emails = await write_email(messages)  # Returns list[Email]
```

---

## The Map Operation

The **map** operation applies a transducible function to each element independently, enabling concurrency and parallelism.

### How Map Works

```python
# Conceptually:
# amap(write_email, messages) -> list[Email]

# Each element is processed independently
# Results maintain the same order as inputs
```

### Map Characteristics

| Aspect | Description |
|--------|-------------|
| **Input** | Single item or list of items |
| **Output** | List of transformed items (one per input) |
| **Operation** | Independent transformation of each element |
| **Parallelization** | Fully parallel - elements processed concurrently |
| **Use Cases** | Enrichment, extraction, classification, normalization |

### Map Examples

**Example 1: Data Enrichment**

```python
class Product(BaseModel):
    name: str
    category: str

class EnrichedProduct(BaseModel):
    name: str
    category: str
    description: str
    keywords: list[str]

@transducible()
async def enrich_product(product: Product) -> EnrichedProduct:
    """Add description and keywords to product."""
    return Transduce(product)

# Process entire catalog
products = load_products()  # list[Product]
enriched = await enrich_product(products)  # Parallel processing
```

**Example 2: Text Classification**

```python
class Document(BaseModel):
    text: str

class ClassifiedDocument(BaseModel):
    text: str
    category: str
    confidence: float
    tags: list[str]

@transducible(batch_size=20)
async def classify_document(doc: Document) -> ClassifiedDocument:
    """Classify document into categories."""
    return Transduce(doc)

documents = load_documents(1000)
classified = await classify_document(documents)  # Processes in batches of 20
```

---

## The Reduce Operation

The **reduce** operation aggregates a collection of items into a single summary or consolidated result.

### Using `transduction_type="areduce"`

Specify the transduction type to create a reduce operation:

```python
from typing import List

class Review(BaseModel):
    text: str
    rating: int

class ReviewSummary(BaseModel):
    overall_sentiment: str
    average_rating: float
    key_themes: List[str]
    total_reviews: int

@transducible(transduction_type="areduce")
async def summarize_reviews(reviews: List[Review]) -> ReviewSummary:
    """Aggregate multiple reviews into a single summary."""
    return Transduce(reviews)

# Use it
reviews = [
    Review(text="Great product!", rating=5),
    Review(text="Good value for money", rating=4),
    Review(text="Not bad, could be better", rating=3),
]

summary = await summarize_reviews(reviews)
print(f"Overall: {summary.overall_sentiment}")
print(f"Average: {summary.average_rating}")
```

### Reduce Characteristics

| Aspect | Description |
|--------|-------------|
| **Input** | List of items |
| **Output** | Single aggregated result |
| **Operation** | Aggregation across all elements |
| **Parallelization** | Sequential or hierarchical |
| **Use Cases** | Summarization, statistics, consolidation, consensus |

### Common Reduce Patterns

**Pattern 1: Summarization**

```python
class Document(BaseModel):
    title: str
    content: str

class ExecutiveSummary(BaseModel):
    main_points: List[str]
    conclusion: str
    word_count: int

@transducible(transduction_type="areduce")
async def create_executive_summary(docs: List[Document]) -> ExecutiveSummary:
    """Summarize multiple documents into key insights."""
    return Transduce(docs)
```

**Pattern 2: Statistical Aggregation**

```python
class DataPoint(BaseModel):
    value: float
    category: str
    timestamp: str

class Statistics(BaseModel):
    mean: float
    median: float
    categories: List[str]
    trend: str  # "increasing", "decreasing", "stable"

@transducible(transduction_type="areduce")
async def analyze_data(points: List[DataPoint]) -> Statistics:
    """Compute statistics and identify trends."""
    return Transduce(points)
```

**Pattern 3: Consensus Building**

```python
class Opinion(BaseModel):
    author: str
    stance: str
    reasoning: str

class Consensus(BaseModel):
    majority_view: str
    key_arguments: List[str]
    dissenting_views: List[str]
    confidence: float

@transducible(transduction_type="areduce")
async def build_consensus(opinions: List[Opinion]) -> Consensus:
    """Find consensus across multiple opinions."""
    return Transduce(opinions)
```

---

## Dynamic Map-Reduce with `<<` Operator

Create map and reduce operations on the fly:

### Dynamic Map

```python
# Create a map function dynamically
enrich = EnrichedProduct << Product

products = [Product(name="Widget", category="Tools"), ...]
enriched = await enrich(products)  # Automatic map
```

### Dynamic Reduce

```python
from agentics import With

# Create a reduce function on the fly
summarize = ReviewSummary << With(
    List[Review],
    transduction_type="areduce",
    instructions="Analyze all reviews and provide comprehensive summary"
)

summary = await summarize(reviews)
```

---

## Combining Map and Reduce

Build complete Map-Reduce pipelines by chaining operations:

```python
# Step 1: Map - Extract insights from each document
class Document(BaseModel):
    text: str

class Insight(BaseModel):
    key_point: str
    importance: int

@transducible()
async def extract_insight(doc: Document) -> Insight:
    """Extract key insight from a document."""
    return Transduce(doc)

# Step 2: Reduce - Consolidate all insights
class Report(BaseModel):
    top_insights: List[str]
    overall_theme: str

@transducible(transduction_type="areduce")
async def consolidate_insights(insights: List[Insight]) -> Report:
    """Consolidate insights into a final report."""
    return Transduce(insights)

# Execute the pipeline
documents = [Document(text="..."), Document(text="..."), ...]
insights = await extract_insight(documents)  # Map phase
report = await consolidate_insights(insights)  # Reduce phase
```

### Multi-Stage Pipeline Example

```python
# Stage 1: Map - Clean and normalize
@transducible()
async def clean_data(raw: RawData) -> CleanData:
    return Transduce(raw)

# Stage 2: Map - Extract features
@transducible()
async def extract_features(clean: CleanData) -> Features:
    return Transduce(clean)

# Stage 3: Reduce - Aggregate statistics
@transducible(transduction_type="areduce")
async def compute_stats(features: List[Features]) -> Statistics:
    return Transduce(features)

# Execute pipeline
raw_data = load_raw_data()
clean = await clean_data(raw_data)
features = await extract_features(clean)
stats = await compute_stats(features)
```

---


## Best Practices

### For Map Operations

1. **Use appropriate batch sizes** - Balance throughput and memory (see [Optimization](optimization.md))
2. **Handle failures gracefully** - Individual items can fail without stopping the batch
3. **Monitor progress** - Use `verbose_transduction=True` for long-running operations
4. **Consider rate limits** - Adjust batch size for API rate limits

### For Reduce Operations

1. **Keep reduce operations focused** - Each reduce should have a clear aggregation goal
2. **Handle empty lists** - Consider what happens when the input list is empty
3. **Use hierarchical reduction** - For very large collections, reduce in stages
4. **Provide clear instructions** - Help the LLM understand the aggregation logic
5. **Consider token limits** - Large collections may exceed context windows
6. **Test with representative data** - Ensure reduce logic works across different input sizes

### General Best Practices

```python
# Good: Clear separation of concerns
@transducible()
async def extract(item: Raw) -> Processed:
    """Map: Extract and normalize."""
    return Transduce(item)

@transducible(transduction_type="areduce")
async def summarize(items: List[Processed]) -> Summary:
    """Reduce: Aggregate results."""
    return Transduce(items)

# Execute
processed = await extract(raw_items)
summary = await summarize(processed)
```

---

## Performance Considerations

### Batch Size Tuning

```python
# Small batches for complex operations
@transducible(batch_size=5)
async def complex_analysis(item: Data) -> Analysis:
    return Transduce(item)

# Large batches for simple operations
@transducible(batch_size=30)
async def simple_extraction(item: Data) -> Extract:
    return Transduce(item)
```

### Parallel Execution

Map operations are automatically parallelized based on `batch_size`. For more control, see [Optimization](optimization.md).

---

## Next
- 👉 [Map-Reduce Tutorial](../tutorials/map_reduce.ipynb) to see how large-scale execution works in practice
- 👉 [Semantic Operators](semantic_operators.md) for performing data transformation tasks using natural language.

## Go to Index
- 👉 [Index](index.md)
