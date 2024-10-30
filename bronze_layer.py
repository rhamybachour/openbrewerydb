# Databricks notebook source
# MAGIC %md
# MAGIC #Libraries

# COMMAND ----------

import libs as lib
import schemas as schema

from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T,
    Window
)

from concurrent.futures import ThreadPoolExecutor

spark = SparkSession.builder.appName("Breweries Case Bronze Layer").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Entry Parameters

# COMMAND ----------

# Initialize an instance of the Base class to access its methods and attributes
libs = lib.Base()

# Retrieve the number of cores (CORES) from widget input; if not set, use the default value of 8
CORES = int(libs.get_widgets("CORES", 8))

# Retrieve the ROOT_PATH from widget input; if not set, use the default path for the data lake storage
ROOT_PATH = libs.get_widgets("ROOT_PATH", "file:/dbfs/test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persisting the Raw Data with Original Format

# COMMAND ----------

# Instantiate an OpenBreweryDB object with the API endpoint, context, and path for data storage
brewery = lib.OpenBreweryDB("api.openbrewerydb.org", "breweries", ROOT_PATH)

# Retrieve a list of breweries using the "list" method
brewery_list = brewery.list()

# Create a Spark DataFrame from the brewery list with specified column names
df_brewery_list = spark.createDataFrame(brewery_list, ["endpoint", "context", "page", "search_datetime", "id", "load_status"])

# COMMAND ----------

# Initialize an empty list to store results
result = []

# Create a thread pool with CORES threads
with ThreadPoolExecutor(CORES) as worker:
    # Use submit to launch each task asynchronously and collect futures
    futures = [worker.submit(lambda row: brewery.get(row[4], row[2]), row) for row in brewery_list]
    
    # Gather results as they complete
    for future in futures:
        res = future.result()  # Get the result of each future
        if res is not None:  # Ensure only non-None results are added
            result.append(res)

# Create a Spark DataFrame from the get brewery result
df_brewery_result = spark.createDataFrame(result, ["id", "page", "load_status"])


# COMMAND ----------

df_brewery_to_load = (df_brewery_list.alias("list")
    # Join the result of search to brewery list
    .join(
        df_brewery_result.alias("result"),
        how="left",
        on=["id"]
    )
    
    # Select each field from the flattened response structure for final log output
    .select(
        F.col("list.id").alias("id"),
        F.col("list.endpoint").alias("endpoint"),
        F.col("list.context").alias("context"),
        F.col("list.page").alias("page"),
        F.col("list.search_datetime").alias("search_datetime"),
        F.coalesce(F.col("result.load_status"), F.col("list.load_status")).alias("load_status")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Bronze Layer

# COMMAND ----------

df_brewery_to_write = (df_brewery_to_load
    # Deduplicate records by assigning a row number within each partition grouped by "id"
    .withColumn("row", F.row_number().over(Window.partitionBy("id").orderBy(F.col("id").desc())))

    # Keep only the first occurrence (row == 1) within each partition, removing duplicates
    .filter(F.col("row") == 1)

    # Drop the temporary "row" column used for deduplication
    .drop("row")
)

# Write the deduplicated DataFrame in Delta format, overwriting any existing data at the specified path (for first run, after use merge delta)
df_brewery_to_write.write.mode("overwrite").format("delta").option("path", f"{ROOT_PATH}{brewery.get_path('bronze', 'log')}").save()

# COMMAND ----------

# If any record is not marked as "Loaded", this means the registry was not loaded and then, the assert fails and raises the message
assert spark.read.load(f"{ROOT_PATH}{brewery.get_path('bronze', 'log')}").filter(F.col('load_status') != "Loaded").count() == 0, "Some data doesn't loaded!"
