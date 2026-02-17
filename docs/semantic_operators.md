# 🔍 Semantic Operators

Semantic operators provide a high-level, declarative API for performing common data transformation tasks using natural language. Inspired by [LOTUS](https://lotus-data.github.io/)-style semantic operations, these operators enable you to work with structured and unstructured data using LLM-powered transformations.

---

## Overview

Agentics semantic operators bridge the gap between traditional data manipulation (like pandas operations) and LLM-powered semantic understanding. Each operator accepts either an `AG` (Agentics) or a pandas `DataFrame` as input and returns the same type, making them easy to integrate into existing data pipelines.

### Available Operators

| Operator | Description |
|----------|-------------|
| `sem_map` | Map each record using a natural language instruction |
| `sem_filter` | Keep records that match a natural language predicate |
| `sem_agg` | Aggregate across all records (e.g., for summarization) |

---

## `sem_map`

Transform each record in your dataset according to natural language instructions, mapping source data to a target schema.

### Signature

```python
async def sem_map(
    source: AG | pd.DataFrame,
    target_type: Type[BaseModel] | str,
    instructions: str,
    merge_output: bool = True,
    **kwargs,
) -> AG | pd.DataFrame
```

### Parameters

- **`source`** (`AG | pd.DataFrame`): Input data to be mapped
- **`target_type`** (`Type[BaseModel] | str`): Target schema for the output
  - If a Pydantic `BaseModel` subclass: used directly as the target type
  - If a `str`: a Pydantic model is created dynamically with a single string field
- **`instructions`** (`str`): Natural language description of how to transform the data
- **`merge_output`** (`bool`, default=`True`): 
  - `True`: Merge mapped fields back into original source records
  - `False`: Return only the mapped output
- **`**kwargs`**: Additional arguments forwarded to `AG()` constructor (e.g., model configuration, batching)

### Returns

- **`AG | pd.DataFrame`**: `AG` or `DataFrame` that contains the transformed data following `target_type`

### Example: Basic Mapping

```python
import pandas as pd
from agentics.core.semantic_operators import sem_map
from pydantic import BaseModel

# Sample data
df = pd.DataFrame({
    'review': [
        'This product is amazing! Best purchase ever.',
        'Terrible quality, broke after one day.',
        'It works okay, nothing special.'
    ]
})

# Define target schema
class Sentiment(BaseModel):
    sentiment: Optional[str] = Field(None, description="The sentiment of the review (e.g., positive, negative, neutral)")
    confidence: Optional[float] = Field(None, description="Confidence score of the sentiment analysis btw 0 and 1")

# Map reviews to sentiment
result = await sem_map(
    source=df,
    target_type=Sentiment,
    instructions="Analyze the sentiment of the review and provide a confidence score between 0 and 1."
)

# Output includes original 'review' column plus 'sentiment' and 'confidence' columns
                                         review  sentiment  confidence
 0  This product is amazing! Best purchase ever.  positive        0.85
 1        Terrible quality, broke after one day.  negative        0.99
 2               It works okay, nothing special.   neutral        0.85
```

### Example: String-based Target Type

```python
# Using string target type for simpler cases
result = await sem_map(
    source=df,
    target_type="category",
    instructions="Classify the review into one of: positive, negative, neutral"
)
```

---

## `sem_filter`

Filter records based on a natural language predicate, keeping only those that satisfy the condition.

### Signature

```python
async def sem_filter(
    source: AG | pd.DataFrame,
    predicate_template: str,
    **kwargs
) -> AG | pd.DataFrame
```

### Parameters

- **`source`** (`AG | pd.DataFrame`): Input data to be filtered
- **`predicate_template`** (`str`): Natural language condition or LangChain-style template
  - Can use `{field}` placeholders to reference source fields
  - Or provide a plain text predicate
- **`**kwargs`**: Additional arguments forwarded to `AG()` constructor

### Returns

- **`AG | pd.DataFrame`**: Filtered data containing only records that satisfy the predicate

### Example: Simple Predicate

```python
from agentics.core.semantic_operators import sem_filter

df = pd.DataFrame({
    'product': ['Laptop', 'Phone', 'Tablet', 'Monitor'],
    'description': [
        'High-performance gaming laptop with RGB keyboard',
        'Budget smartphone with basic features',
        'Premium tablet with stylus support',
        '4K monitor for professional work'
    ]
})

# Filter for premium/high-end products
result = await sem_filter(
    source=df,
    predicate_template="The product is premium or high-end"
)

print(result)
   product                                       description
0   Laptop  High-performance gaming laptop with RGB keyboard
1   Tablet                Premium tablet with stylus support
2  Monitor                  4K monitor for professional work
```

### Example: Template-based Filtering

```python
# Use field placeholders in the predicate
result = await sem_filter(
    source=df,
    predicate_template="The {product} described as '{description}' is suitable for gaming"
)
```

---

## `sem_agg`

Aggregate data across all records to produce a summary or consolidated output.

### Signature

```python
async def sem_agg(
    source: AG | pd.DataFrame,
    target_type: Type[BaseModel] | str,
    instructions: str = None,
    **kwargs,
) -> AG | pd.DataFrame
```

### Parameters

- **`source`** (`AG | pd.DataFrame`): Input data to be aggregated
- **`target_type`** (`Type[BaseModel] | str`): Schema for the aggregated output
- **`instructions`** (`str`, optional): Natural language description of the aggregation
- **`**kwargs`**: Additional arguments forwarded to `AG()` constructor

### Returns

- **`AG | pd.DataFrame`**: Aggregated result (typically a single record or summary)

### Example: Summarization

```python
from agentics.core.semantic_operators import sem_agg
from pydantic import BaseModel

df = pd.DataFrame({
    'review': [
        'Great product, very satisfied!',
        'Good quality but expensive',
        'Not worth the price',
        'Excellent, would buy again',
        'Decent but has some issues'
    ]
})

class ReviewSummary(BaseModel):
    overall_sentiment: str
    key_themes: list[str]
    recommendation: str

# Aggregate all reviews into a summary
result = await sem_agg(
    source=df,
    target_type=ReviewSummary,
    instructions="Summarize all reviews, identify key themes, and provide an overall recommendation"
)

print(result)
# Returns a single record with aggregated insights
```

### Example: Statistical Summary

```python
class Statistics(BaseModel):
    total_count: int
    positive_count: int
    negative_count: int
    average_sentiment: str

result = await sem_agg(
    source=df,
    target_type=Statistics,
    instructions="Count total reviews, positive reviews, negative reviews, and determine average sentiment"
)
```

---

## Best Practices

### 1. Choose the Right Operator

- **`sem_map`**: Use for 1:1 transformations (each input → one output)
- **`sem_filter`**: Use for selecting subsets based on conditions
- **`sem_agg`**: Use for many:1 transformations (all inputs → one summary)

### 2. Write Clear Instructions

```python
# ❌ Vague
instructions = "Process the data"

# ✅ Clear and specific
instructions = """
Extract the product name, price, and category from each description.
Normalize prices to USD. Categorize products as: Electronics, Clothing, or Home Goods.
"""
```

### 3. Use Appropriate Target Types

```python
# For simple extractions, use string types
sem_map(
    ...
    target_type= "category_name"
    ...
)


# For structured outputs, use Pydantic models
class Product(BaseModel):
    name: str
    price: float
    category: str

sem_map(
    ...
    target_type= Product
    ...
)
```

### 4. Batch Processing

```python
# Configure batch size for large datasets
result = await sem_map(
    source=large_df,
    target_type=MyType,
    instructions="...",
    amap_batch_size=50  # Process 50 records at a time
)
```

### 5. Handle Both AG and DataFrame

```python
# Operators work with both types
df_result = await sem_filter(df, "condition")  # Returns DataFrame
ag_result = await sem_filter(ag, "condition")  # Returns AG
```

---

## Performance Considerations

### Batching

Semantic operators support batching for efficient processing of large datasets:

```python
result = await sem_map(
    source=df,
    target_type=MyType,
    instructions="...",
    amap_batch_size=20  # Default for sem_filter
)
```


## Integration with Agentics Workflows

Semantic operators integrate seamlessly with other Agentics features:

### Chaining Operations

```python
# Filter → Map → Aggregate pipeline
filtered = await sem_filter(df, "High-value customers")
mapped = await sem_map(filtered, CustomerProfile, "Extract profile details")
summary = await sem_agg(mapped, Summary, "Summarize customer segments")
```

---

## Next
- 👉 [Semantic Operators Tutorial](../tutorials/semantic_operators.ipynb) - Code examples
- 👉 [Agentics (AG)](agentics.md) for data modeling patterns and typed state containers

## Go to Index
- 👉 [Index](index.md)
