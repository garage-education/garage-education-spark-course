# Databricks notebook source
# MAGIC %md
# MAGIC # Example 2: Text Manipulation RDD

# COMMAND ----------

text = ["Hello Spark", "Hello Scala", "Hello World"]
text_rdd = sc.parallelize(text)
print(f"Original Text RDD result: {text_rdd.take(10)}")


# COMMAND ----------


words_rdd = text_rdd.flatMap(lambda line: line.split(" "))
print(f"Words RDD result: {words_rdd.take(10)}")


# COMMAND ----------


upper_words_rdd = words_rdd.map(lambda word: word.upper())
print(f"Upper Words RDD result: {upper_words_rdd.take(10)}")

