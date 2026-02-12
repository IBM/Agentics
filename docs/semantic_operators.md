# 🔍 Semantic Operators

Semantic operators provide a high-level, declarative API for performing common data transformation tasks using natural language instructions. Inspired by LOTUS-style semantic operations, these operators enable you to work with structured and unstructured data using LLM-powered transformations.

---

## Overview

Agentics semantic operators bridge the gap between traditional data manipulation (like pandas operations) and LLM-powered semantic understanding. Each operator accepts either an `AG` (Agentics) or a pandas `DataFrame` as input and returns the same type, making them easy to integrate into existing data pipelines.

### Available Operators

| Operator | Description |
|----------|-------------|
| `sem_map` | Map each record using a natural language projection |
| `sem_filter` | Keep records that match a natural language predicate |
| `sem_extract` | Extract one or more attributes from each row |
| `sem_agg` | Aggregate across all records (e.g., for summarization) |
| `sem_topk` | Order records by natural language sorting criteria |
| `sem_join` | Join two datasets based on a natural language predicate |

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

- **`AG | pd.DataFrame`**: Transformed data in the same format as input

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
    sentiment: str
    confidence: float

# Map reviews to sentiment
result = await sem_map(
    source=df,
    target_type=Sentiment,
    instructions="Analyze the sentiment of the review and provide a confidence score (0-1)"
)

print(result)
# Output includes original 'review' column plus 'sentiment' and 'confidence' columns
```

### Example: String-based Target Type

```python
# Using string target type for simpler cases
result = await sem_map(
    source=df,
    target_type="category",
    instructions="Classify the review into one of: positive, negative, neutral"
)

print(result)
# Output includes original columns plus a 'category' column
```

### Example: Extract Without Merge

```python
# Get only the mapped output without original data
result = await sem_map(
    source=df,
    target_type=Sentiment,
    instructions="Analyze sentiment",
    merge_output=False
)

print(result)
# Output contains only 'sentiment' and 'confidence' columns
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
# Returns only Laptop, Tablet, and Monitor
```

### Example: Template-based Filtering

```python
# Use field placeholders in the predicate
result = await sem_filter(
    source=df,
    predicate_template="The {product} described as '{description}' is suitable for gaming"
)

print(result)
# Returns only the gaming laptop
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

### Using with AG

```python
from agentics import AG

# Load data into AG
ag = AG.from_dataframe(df)

# Apply semantic operators
filtered_ag = await sem_filter(ag, "Important records")
mapped_ag = await sem_map(filtered_ag, OutputType, "Transform data")

# Continue with AG operations
result = await (target_ag << mapped_ag)
```

---


## Coming Soon

The following operators are planned for future releases:

- **`sem_topk`**: Order and select top-k records by semantic criteria
- **`sem_join`**: Join datasets based on semantic similarity or conditions

---

## See Also

- [Core Concepts](core_concepts.md) - Understanding Agentics fundamentals
- [Transducible Functions](transducible_functions.md) - Lower-level transformation API
- [Map-Reduce Operations](map_reduce.md) - Scaling transformations
- [Agentics (AG)](agentics.md) - Working with typed state containers
