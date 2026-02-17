# ⚡ Performance Optimization

Efficient batch processing and performance optimization are crucial for large-scale transductions. This guide covers strategies to maximize throughput, manage resources, and handle large datasets effectively.

---

## Understanding Batch Size

The `batch_size` parameter controls how many items are processed concurrently. Choosing the right batch size is critical for balancing throughput, memory usage, and reliability.

```python
# Small batches - lower memory, more overhead
@transducible(batch_size=5)
async def conservative_process(state: Item) -> Result:
    return Transduce(state)

# Large batches - higher throughput, more memory
@transducible(batch_size=25)
async def aggressive_process(state: Item) -> Result:
    return Transduce(state)
```

### Choosing the Right Batch Size

| Scenario | Recommended Batch Size | Reason |
|----------|----------------------|---------|
| Simple transformations (< 1s each) | 20-30 | Maximize throughput |
| Complex reasoning (> 5s each) | 5-10 | Avoid timeout issues |
| Large input/output objects | 10-15 | Manage memory usage |
| Rate-limited APIs | 5-15 | Stay within limits |
| Local LLM (Ollama) | 1-5 | Limited by GPU memory |

---

## Persisting Intermediate Results

Use `persist_output` to save results incrementally, enabling recovery from failures:

```python
@transducible(
    batch_size=20,
    persist_output="./output/processed_batches"
)
async def process_large_dataset(state: DataItem) -> ProcessedItem:
    """Results saved after each batch completes."""
    return Transduce(state)

# Process 10,000 items
large_dataset = load_items(10000)
results = await process_large_dataset(large_dataset)

# If interrupted, previously completed batches are saved
# Resume by loading saved batches and processing remaining items
```

### File Structure

```
output/processed_batches/
├── batch_0000.jsonl  # First 20 items
├── batch_0001.jsonl  # Next 20 items
├── batch_0002.jsonl  # And so on...
└── ...
```

---

## Monitoring Progress

Enable verbose logging to track batch processing:

```python
@transducible(
    batch_size=25,
    verbose_transduction=True,  # Show progress
    verbose_agent=False  # Hide detailed agent logs
)
async def monitored_process(state: Item) -> Result:
    return Transduce(state)

# Output shows:
# Processing batch 1/40 (25 items)...
# Processing batch 2/40 (25 items)...
# ...
```

---

## Performance Optimization Strategies

### 1. Adaptive Batch Sizing

Tune batch size based on item complexity:

```python
# Adaptive batching based on input size
def get_batch_size(items):
    avg_size = sum(len(str(item)) for item in items) / len(items)
    if avg_size < 500:
        return 25  # Small items
    elif avg_size < 2000:
        return 15  # Medium items
    else:
        return 5  # Large items

batch_size = get_batch_size(dataset)
process_fn = Result << With(Item, batch_size=batch_size)
```

### 2. Field-Specific Transduction

Only transduce the fields you need:

```python
@transducible(
    transduce_fields=["summary", "category"],  # Only these fields
    batch_size=30
)
async def focused_transform(state: FullData) -> PartialResult:
    """Faster by ignoring unnecessary fields."""
    return Transduce(state)
```

### 3. Reduce Token Usage with Prompt Templates

```python
# Custom template to reduce token count
compact_template = """
Input: {input_data}
Task: {instructions}
Output format: {output_schema}
"""

@transducible(
    prompt_template=compact_template,
    batch_size=40
)
async def efficient_transform(state: Item) -> Result:
    return Transduce(state)
```

### 4. Parallel Processing with Multiple Workers

For extremely large datasets, consider splitting work across multiple processes:

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor

async def process_chunk(chunk, process_fn):
    """Process a chunk of data."""
    return await process_fn(chunk)

async def parallel_process(dataset, process_fn, num_workers=4):
    """Split dataset across multiple workers."""
    chunk_size = len(dataset) // num_workers
    chunks = [dataset[i:i+chunk_size] for i in range(0, len(dataset), chunk_size)]
    
    tasks = [process_chunk(chunk, process_fn) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    
    # Flatten results
    return [item for chunk_result in results for item in chunk_result]
```

---

## Performance Benchmarking

Measure throughput for your specific use case:

```python
import time

async def benchmark_transduction():
    test_items = generate_test_data(100)
    
    start = time.time()
    results = await process_fn(test_items)
    elapsed = time.time() - start
    
    print(f"Processed {len(results)} items in {elapsed:.2f}s")
    print(f"Throughput: {len(results)/elapsed:.2f} items/sec")
    print(f"Average time per item: {elapsed/len(results):.2f}s")

await benchmark_transduction()
```

### Profiling Memory Usage

```python
import tracemalloc

async def profile_memory():
    tracemalloc.start()
    
    # Your transduction
    results = await process_fn(large_dataset)
    
    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory: {current / 1024 / 1024:.2f} MB")
    print(f"Peak memory: {peak / 1024 / 1024:.2f} MB")
    
    tracemalloc.stop()
```

---

## Error Handling & Retries

### Automatic Retries

Configure retry behavior for transient failures:

```python
@transducible(
    max_retries=3,  # Retry up to 3 times
    retry_delay=2.0,  # Wait 2 seconds between retries
    batch_size=20
)
async def resilient_process(state: Item) -> Result:
    return Transduce(state)
```

### Graceful Degradation

Use optional fields to handle partial failures:

```python
class RobustResult(BaseModel):
    required_field: str
    optional_field: Optional[str] = None  # May be None if extraction fails
    confidence: Optional[float] = None

@transducible(
    batch_size=25,
    allow_partial=True  # Continue even if some fields fail
)
async def robust_transform(state: Item) -> RobustResult:
    return Transduce(state)
```

### Batch-Level Error Handling

```python
async def process_with_error_handling(items):
    results = []
    failed = []
    
    for batch in chunk_items(items, batch_size=20):
        try:
            batch_results = await process_fn(batch)
            results.extend(batch_results)
        except Exception as e:
            print(f"Batch failed: {e}")
            failed.extend(batch)
    
    # Retry failed items with smaller batch size
    if failed:
        print(f"Retrying {len(failed)} failed items...")
        retry_fn = Result << With(Item, batch_size=5)
        retry_results = await retry_fn(failed)
        results.extend(retry_results)
    
    return results
```

---

## Best Practices

1. **Start with conservative batch sizes** - Increase gradually based on benchmarks
2. **Monitor memory usage** - Especially with large input/output objects
3. **Use persist_output for long-running jobs** - Protect against interruptions
4. **Profile before optimizing** - Measure to identify actual bottlenecks
5. **Consider API rate limits** - Adjust batch size and concurrency accordingly
6. **Test with representative data** - Performance varies with input complexity
7. **Use field-specific transduction** - Only process what you need
8. **Enable progress monitoring** - Track long-running operations

---

## Common Performance Issues

### Issue: High Memory Usage

**Symptoms:** Process crashes or slows down with large datasets

**Solutions:**
- Reduce batch size
- Use field-specific transduction
- Process in chunks with persistence
- Stream results instead of loading all at once

### Issue: Slow Throughput

**Symptoms:** Processing takes much longer than expected

**Solutions:**
- Increase batch size (if memory allows)
- Reduce prompt complexity
- Use faster LLM models
- Optimize prompt templates
- Consider parallel processing

### Issue: Frequent Timeouts

**Symptoms:** Many requests timeout or fail

**Solutions:**
- Reduce batch size
- Increase timeout value
- Simplify the transduction task
- Use faster models
- Check network connectivity

---

## See Also

- 👉 [Transducible Functions](transducible_functions.md) - Core concepts and basic usage
- 👉 [Tool Integration](tool_integration.md) - Using external tools
- 👉 [Map-Reduce Tutorial](../tutorials/map_reduce.ipynb) - Large-scale execution patterns
- 👉 [Index](index.md)
