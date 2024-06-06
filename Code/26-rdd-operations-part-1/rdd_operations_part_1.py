# Databricks notebook source
# Example RDD
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Map` Function

# COMMAND ----------

# 1. map
print("### 1. map ###")
print("Description: Return a new RDD by applying a function to all elements of this RDD.")

# Example 1: Multiply each element by 2
simple_map = rdd.map(lambda x: x * 2).collect()
print("01 map example (multiply by 2):", simple_map)

# Example 2: Extract the length of each word in a list of sentences
sentences = ["Hello world", "Apache Spark", "RDD transformations Wide Vs Narrow Spark"]
# Hello World => split (" ") => [(0)-> Hello, (1) -> World]
sentence_rdd = sc.parallelize(sentences)
words_map = sentence_rdd.map(lambda sentence: len(sentence.split(" "))).collect()
print("example_map example (word count in sentences):", words_map)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Filter` Function

# COMMAND ----------

# 2. filter
print("\n### 2. filter ###")
print("Description: Return a new RDD containing only the elements that satisfy a predicate.")

# 01 Example: Filter out even numbers
simple_filter = rdd.filter(lambda x: x % 2 == 0).collect()
print("01 filter example (even numbers):", simple_filter)

# example_Example: Filter sentences containing the word 'Spark'
words_filter = sentence_rdd.filter(lambda sentence: "Spark" in sentence).collect()
print("example_ filter example (sentences with 'Spark'):", words_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `FlatMap` Function

# COMMAND ----------

# 3. flatMap
print("\n### 3. flatMap ###")
print("Description: Return a new RDD by applying a function to all elements of this RDD and then flattening the results.")

# 01 Example: Split sentences into words
sentences_mapped = sentence_rdd.map(lambda sentence: sentence.split(" ")).collect()
print("01 sentences_mapped:", sentences_mapped)

simple_flatMap = sentence_rdd.flatMap(lambda sentence: sentence.split(" ")).collect()
print("02 flatMap example (split sentences into words):", simple_flatMap)

# example_Example: Flatten a list of lists
nested_lists = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
nested_rdd = sc.parallelize(nested_lists)
flatten_list = nested_rdd.flatMap(lambda x: x).collect()
print("flatten_list  flatMap example (flatten list of lists):", flatten_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Reduce` Function

# COMMAND ----------

# 4. reduce
print("\n### 4. reduce ###")
print("Description: Reduces the elements of this RDD using the specified commutative and associative binary operator.")

# 01 Example: Sum of elements
simple_reduce = rdd.reduce(lambda x, y: x + y)
print("01 reduce example (sum of elements):", simple_reduce)

# example_Example: Find the longest word in a list of words
words = ["cat", "elephant", "rat", "hippopotamus"]
words_rdd = sc.parallelize(words)
words_rdd_reduced = words_rdd.reduce(lambda x, y: x if len(x) > len(y) else y)
print("reduce example (longest word):", words_rdd_reduced)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `groupByKey` Function

# COMMAND ----------

# 5. groupByKey
print("\n### 5. groupByKey ###")
print("Description: Group the values for each key in the RDD into a single sequence.")

# 01 Example: Group numbers by even and odd
pairs = [(1, 'a'),(1, 'ali'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')]
pairs_rdd = sc.parallelize(pairs)
simple_groupByKey = pairs_rdd.groupByKey().mapValues(list).collect()
print("01 groupByKey example (group numbers):", simple_groupByKey)

# example_Example: Group words by their starting letter
words_pairs = [("cat", 1), ("car", 2), ("dog", 3), ("deer", 4), ("elephant", 5),("elephant", 20)]
words_rdd = sc.parallelize(words_pairs)
# mapValues(list) converts the grouped values (which are iterable) into lists.
words_grouped = words_rdd.groupByKey().mapValues(list).collect()
print("words_grouped example (group words by starting letter):", words_grouped)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `reduceByKey` Function

# COMMAND ----------

# 6. reduceByKey
print("\n### 6. reduceByKey ###")
print("Description: Merge the values for each key using an associative and commutative reduce function.")
pairs = [(1, 'a'),(1, '_a'), (2, 'b'), (2, '_b'), (3, 'c'), (4, 'd'), (5, 'e')]
pairs_rdd = sc.parallelize(pairs)

# 01 Example: Sum values with the same key
simple_reduceByKey = pairs_rdd.reduceByKey(lambda x, y: x + y).collect()
print("01 reduceByKey example (sum values by key):", simple_reduceByKey)

# example_Example: Count the occurrences of each word in a list
word_list = ["cat", "cat", "dog", "elephant", "dog", "dog"]
word_pairs_rdd = sc.parallelize(word_list).map(lambda word: (word, 1))
example__reduceByKey = word_pairs_rdd.reduceByKey(lambda x, y: x + y).collect()
print("example_ reduceByKey example (word count):", example__reduceByKey)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `join` Function

# COMMAND ----------

# 7. join
print("\n### 7. join ###")
print("Description: Perform an inner join of this RDD and another one.")

# 01 Example: Join two RDDs by key
fruits = sc.parallelize([(1, "apple"), (2, "banana")])
colors = sc.parallelize([(1, "red"), (2, "yellow")])
fruits_color_join = fruits.join(colors).collect()
print("01 join fruits_color_join (join two RDDs):", fruits_color_join)

# example_Example: Join employee data with department data
employees = sc.parallelize([(1, "John"), (2, "Jane"), (3, "Joe")])
departments = sc.parallelize([(1, "HR"), (2, "Finance")])
employees_department_join = employees.join(departments).collect()
print("join example (employee-department join):", employees_department_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `cogroup` Function

# COMMAND ----------

# MAGIC %md
# MAGIC TableA:
# MAGIC
# MAGIC | id | value  |
# MAGIC |----|--------|
# MAGIC |  1 | apple  |
# MAGIC |  2 | banana |
# MAGIC |  3 | orange |
# MAGIC
# MAGIC
# MAGIC TableB:
# MAGIC
# MAGIC | id | color  |
# MAGIC |----|--------|
# MAGIC |  1 | red    |
# MAGIC |  2 | yellow |
# MAGIC
# MAGIC
# MAGIC Result of cogroup:
# MAGIC
# MAGIC | id | value  | color  |
# MAGIC |----|--------|--------|
# MAGIC |  1 | apple  | red    |
# MAGIC |  2 | banana | yellow |
# MAGIC |  3 | orange | NULL   |
# MAGIC
# MAGIC

# COMMAND ----------

# 8. cogroup
# The cogroup function in PySpark is used to group data from two RDDs that share the same key. 
# It combines the values of matching keys from both RDDs into a tuple of lists.
print("\n### 8. cogroup ###")
print("Description: Group data from two RDDs sharing the same key.")

# 01 Example: Cogroup two RDDs
fruits_rdd = sc.parallelize([(1, "apple"), (2, "banana"), (3, "orange")])
colors_rdd = sc.parallelize([(1, "red"), (2, "yellow")])
cogrouped_fruits_colors = fruits_rdd.cogroup(colors_rdd).mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
print("01 cogroup example (group two RDDs):", cogrouped_fruits_colors)



# example_Example: Cogroup sales data with target data
sales_rdd = sc.parallelize([("store1", 100), ("store2", 200)])
targets_rdd = sc.parallelize([("store1", 150), ("store3", 250)])
cogrouped_sales_targets = sales_rdd.cogroup(targets_rdd).mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
print("example_cogroup example (sales-targets cogroup):", cogrouped_sales_targets)


# COMMAND ----------

# MAGIC %md
# MAGIC ## `distinct` Function

# COMMAND ----------

# 9. distinct
print("\n### 9. distinct ###")
print("Description: Return a new RDD containing the distinct elements in this RDD.")

# example_Example: Unique words from a list of words
words = ["cat", "dog", "cat", "elephant", "dog"]
words_rdd = sc.parallelize(words)
example__distinct = words_rdd.distinct().collect()
print("example_distinct example (unique words):", example__distinct)
