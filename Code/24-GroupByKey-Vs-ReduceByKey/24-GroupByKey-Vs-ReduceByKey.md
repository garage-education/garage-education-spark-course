---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.2
---

# Aggregation: groupByKey V. reduceByKey

```python

# Example 3: Group By Transformation
pairs_rdd = sc.parallelize([("A", 1), ("B", 1), ("A", 2), ("B", 2), ("A", 3)] * 5000000)
print(f"Original Pairs RDD result: {pairs_rdd.take(10)}")

```

```python
import time
# Measure performance of groupByKey and sum
start_time = time.time()
grouped_rdd = pairs_rdd.groupByKey().mapValues(lambda values: sum(values))
grouped_result = grouped_rdd.collect()
group_by_key_duration = time.time() - start_time
print(f"GroupByKey duration: {group_by_key_duration:.4f} seconds")
print(f"Grouped RDD result (sum): {grouped_result[:10]}")  # Display only the first 10 results for brevity
```

```python
# Measure performance of reduceByKey and sum
start_time = time.time()
reduced_rdd = pairs_rdd.reduceByKey(lambda x, y: x + y)
reduced_result = reduced_rdd.collect()
reduce_by_key_duration = time.time() - start_time
print(f"ReduceByKey duration: {reduce_by_key_duration:.4f} seconds")
print(f"Reduced RDD result: {reduced_result[:10]}")  # Display only the first 10 results for brevity
```
