# Databricks notebook source
# MAGIC %md
# MAGIC # Join RDDs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse Databricks datasets 

# COMMAND ----------

#https://docs.databricks.com/en/discover/databricks-datasets.html
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TPCH Data Analysis tpch

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/tpch/data-001/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze `Parts` dataset

# COMMAND ----------

# Read input part dataset as RDD[String]
part_input = sc.textFile("/databricks-datasets/tpch/data-001/part/")
part_input.take(10)

# COMMAND ----------

# Split the input data
part_input_splitted = part_input.map(lambda p: p.split('|'))
part_input_splitted.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply simple filteration for malformed records 
# MAGIC Assume any record with more than 9 columns is malformed
# MAGIC https://github.com/oracle/heatwave-tpch/blob/main/TPCH/create_tables.sql
# MAGIC ```sql
# MAGIC CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
# MAGIC                      P_NAME        VARCHAR(55) NOT NULL,
# MAGIC                      P_MFGR        CHAR(25) NOT NULL,
# MAGIC                      P_BRAND       CHAR(10) NOT NULL,
# MAGIC                      P_TYPE        VARCHAR(25) NOT NULL,
# MAGIC                      P_SIZE        INTEGER NOT NULL,
# MAGIC                      P_CONTAINER   CHAR(10) NOT NULL,
# MAGIC                      P_RETAILPRICE DECIMAL(15,2) NOT NULL,
# MAGIC                      P_COMMENT     VARCHAR(23) NOT NULL,
# MAGIC                      PRIMARY KEY (P_PARTKEY));
# MAGIC CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
# MAGIC                         PS_SUPPKEY     INTEGER NOT NULL,
# MAGIC                         PS_AVAILQTY    INTEGER NOT NULL,
# MAGIC                         PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
# MAGIC                         PS_COMMENT     VARCHAR(199) NOT NULL,
# MAGIC                         PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY));
# MAGIC ```

# COMMAND ----------

PART_SIZE = 10

# Split part input and parse it into RDD[(partKey, Part)]
part_mapped = part_input_splitted \
    .filter(lambda arr: len(arr) == PART_SIZE)
part_mapped.take(10)  

# COMMAND ----------

# Catch the rejected records which aren't matching the case class size
part_rejected = part_input_splitted.filter(lambda arr: len(arr) != PART_SIZE)

## Any better way to filter?
part_rejected.take(10)


# COMMAND ----------

# Display parsed and rejected records count
print(f"Number of rejected records = {part_rejected.count()}")
print(f"Number of parsed records = {part_mapped.count()}")

# COMMAND ----------

class Part:
    def __init__(self, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment):
        self.p_partkey = p_partkey
        self.p_name = p_name
        self.p_mfgr = p_mfgr
        self.p_brand = p_brand
        self.p_type = p_type
        self.p_size = p_size
        self.p_container = p_container
        self.p_retailprice = p_retailprice
        self.p_comment = p_comment

    def __repr__(self):
        return f"Part({self.p_partkey}, {self.p_name}, {self.p_mfgr}, {self.p_brand}, {self.p_type}, {self.p_size}, {self.p_container}, {self.p_retailprice}, {self.p_comment})"


# COMMAND ----------

part_transformed = part_mapped.map(lambda arr: (int(arr[0]), Part(int(arr[0]), arr[1], arr[2], arr[3], arr[4], int(arr[5]), arr[6], float(arr[7]), arr[8])))
part_transformed.take(10)

# RDD [(Int, Part)]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and Parse the `partsupp` Dataset

# COMMAND ----------

class PartSupp:
    def __init__(self, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment):
        self.ps_partkey = ps_partkey
        self.ps_suppkey = ps_suppkey
        self.ps_availqty = ps_availqty
        self.ps_supplycost = ps_supplycost
        self.ps_comment = ps_comment

    def __repr__(self):
        return f"PartSupp({self.ps_partkey}, {self.ps_suppkey}, {self.ps_availqty}, {self.ps_supplycost}, {self.ps_comment})"


# COMMAND ----------

# Read input partSupp dataset as RDD[String]
partsupp_input = sc.textFile("/databricks-datasets/tpch/data-001/partsupp/")

partsupp_input.take(10)

# COMMAND ----------


# Split the input data
partsupp_input_splitted = partsupp_input.map(lambda p: p.split('|'))
partsupp_input_splitted.take(10)


# COMMAND ----------

PARTSUPP_SIZE = 6

# Split partSupp input and parse it into RDD[(partKey, PartSupp)]
partsupp_mapped = partsupp_input_splitted \
    .filter(lambda arr: len(arr) == PARTSUPP_SIZE) \
    .map(lambda arr: (int(arr[0]), PartSupp(int(arr[0]), int(arr[1]), int(arr[2]), float(arr[3]), arr[4])))
partsupp_mapped.take(10)
# RDD [ (integer , PartSupp)]

# COMMAND ----------

    

# Catch the rejected records which aren't matching the case class size
partsupp_rejected = partsupp_input_splitted.filter(lambda arr: len(arr) != PARTSUPP_SIZE)

# Display parsed and rejected records count
print(f"Number of rejected records = {partsupp_rejected.count()}")
print(f"Number of parsed records = {partsupp_mapped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Join the part and partsupp Datasets

# COMMAND ----------



# COMMAND ----------

# PartRDD -> RDD[ (integer, Part)]
# PartSuppRDD -> RDD [ (integer, PartSupp)]
# JoinedRDD -> RDD [(integer,(Part,PartSupp)) ]
# Perform inner join on part and partsupp datasets
part_joined_partsupp = part_transformed.join(partsupp_mapped)

# Take the first 10 elements of the joined RDD and print them
# for record in part_joined_partsupp.take(10):
#     print(record)

# Print the count of joined records
print(f"Number of joined records = {part_joined_partsupp.count()}")
