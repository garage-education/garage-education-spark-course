---
jupyter:
  jupytext:
    formats: ipynb,md
    main_language: python
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.2
---

# Example 2: Text Manipulation RDD

```python
text = ["Hello Spark", "Hello Scala", "Hello World"]
text_rdd = sc.parallelize(text)
print(f"Original Text RDD result: {text_rdd.take(10)}")

```

```python

words_rdd = text_rdd.flatMap(lambda line: line.split(" "))
print(f"Words RDD result: {words_rdd.take(10)}")

```

```python

upper_words_rdd = words_rdd.map(lambda word: word.upper())
print(f"Upper Words RDD result: {upper_words_rdd.take(10)}")

```
