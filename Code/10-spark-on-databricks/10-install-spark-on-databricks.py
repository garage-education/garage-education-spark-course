# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To RDD

# COMMAND ----------


# Create an RDD from a list of numbers
numbers = [1, 2, 3, 4, 5]
numbers_rdd = sc.parallelize(numbers)
numbers_rdd

# COMMAND ----------

print(numbers_rdd) 


# COMMAND ----------


# Apply a transformation: multiply each number by 2
doubled_rdd = numbers_rdd.map(lambda x: x * 2)


# COMMAND ----------


# Perform an action: collect the results to a list
result = doubled_rdd.collect()

# Print the result
print(result)  # Output: [2, 4, 6, 8, 10]
