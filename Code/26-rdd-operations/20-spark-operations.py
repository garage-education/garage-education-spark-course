# Databricks notebook source
# MAGIC %md
# MAGIC # Immutable RDDs

# COMMAND ----------

# Test Immutable RDDs
numbers = [1, 2, 3, 4, 5]
numbers_rdd = sc.parallelize(numbers)
print(f"Original RDD ID: {numbers_rdd.id()}")
print(f"Original RDD ID: {numbers_rdd.id()}")

# # Apply a transformation: multiply each number by 2
transformed_rdd = numbers_rdd.map(lambda x: x * 2)
print(f"Transformed RDD ID: {transformed_rdd.id()}")

# # Collect the results to trigger the computation
result = transformed_rdd.collect()
print(f"Transformed RDD result: {result}")


# COMMAND ----------

# MAGIC %scala
# MAGIC // Test Immutable RDDs
# MAGIC val numbers = List(1, 2, 3, 4, 5)
# MAGIC val numbersRdd = sc.parallelize(numbers)
# MAGIC println(s"Original RDD ID: ${numbersRdd.id}")
# MAGIC println(s"Original RDD ID: ${numbersRdd.id}")
# MAGIC println(s"Original RDD ID: ${numbersRdd.id}")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // numbersRdd = numbersRdd.map(x => x * 2) //OPS!!!!!!!!!!!
# MAGIC
# MAGIC // Apply a transformation: multiply each number by 2
# MAGIC val transformedRdd = numbersRdd.map(x => x * 2)
# MAGIC println(s"Transformed RDD ID: ${transformedRdd.id}")
# MAGIC
# MAGIC // Collect the results to trigger the computation
# MAGIC val result = transformedRdd.collect()
# MAGIC println(s"Transformed RDD result: ${result.mkString(", ")}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Immutable DF Example

# COMMAND ----------

# Create an RDD
data = [("John", 28), ("Smith", 44), ("Adam", 65), ("Henry", 23)]
rdd = sc.parallelize(data)

# Show the original RDD
print("Original RDD:")
for row in rdd.collect():
    print(row)


# COMMAND ----------


print(f"Original RDD ID: {rdd.id()}")

rdd = rdd.filter(lambda x: x[1] > 30)

print(f"Original RDD ID After filter: {rdd.id()}")

# Filter rows where the age is greater than 30
filtered_rdd = rdd.filter(lambda x: x[1] > 30)
print(f"Transformed RDD ID: {filtered_rdd.id()}")

# Show the transformed RDD
print("Filtered RDD:")
for row in filtered_rdd.collect():
    print(row)

# COMMAND ----------

# MAGIC %scala
# MAGIC     // Create an RDD
# MAGIC     val data = Seq(("John", 28), ("Smith", 44), ("Adam", 65), ("Henry", 23))
# MAGIC     val rdd = sc.parallelize(data)
# MAGIC
# MAGIC     // Show the original RDD
# MAGIC     println("Original RDD:")
# MAGIC     rdd.collect().foreach(println)
# MAGIC     //rdd = rdd.filter{ case (name, age) => age > 30 }
# MAGIC     // // Filter rows where the age is greater than 30
# MAGIC     val filteredRdd = rdd.filter{ case (name, age) => age > 30 }
# MAGIC     println(s"Transformed RDD ID: ${filteredRdd.id}")
# MAGIC
# MAGIC     // Show the transformed RDD
# MAGIC     println("Filtered RDD:")
# MAGIC     filteredRdd.collect().foreach(println)

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Lazy Evaluation 

# COMMAND ----------

# Create an RDD
rdd = sc.parallelize([
    ("John", 28),
    ("Smith", 44),
    ("Adam", 65),
    ("Henry", 23)
])

# Apply a map transformation to create a new RDD with a tuple including the name and a boolean flag
# if the person is older than 30
mapped_rdd = rdd.map(lambda x: (x[0], x[1], x[1] > 30))

# Filter the RDD to include only people older than 30
filtered_rdd = mapped_rdd.filter(lambda x: x[2])

# Convert the filtered RDD back to a DataFrame
df = spark.createDataFrame(filtered_rdd, ["Name", "Age", "OlderThan30"])

# Select only the name and age columns
final_df = df.select("Name", "Age")

# # Collect the results which triggers the execution of all transformations
results = final_df.collect()
display(results)

