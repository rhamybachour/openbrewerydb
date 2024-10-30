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
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Breweries Case Gold Layer").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Entry Parameters

# COMMAND ----------

# Initialize an instance of the Base class to access its methods and attributes
libs = lib.Base()

# Retrieve the ROOT_PATH from widget input; if not set, use the default path for the data lake storage
ROOT_PATH = libs.get_widgets("ROOT_PATH", "file:/dbfs/test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Silver Layer

# COMMAND ----------

# Initialize an instance of the Base class with specified endpoint and context.
brewery = lib.Base(endpoint="api.openbrewerydb.org", context="breweries")

# Read data from the Delta table located at the specified path (constructed using ROOT_PATH and the get_path method)
df_breweries = spark.read.load(f"{ROOT_PATH}{brewery.get_path('silver')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Formating Gold Layer

# COMMAND ----------

df_gold_layer = (df_breweries
    .groupby(
        F.col('brewery_type').alias('brewery_type'),
        F.col('city').alias('city'),
        F.col('state_province').alias('state_province'),
    )
    .agg(
        F.count_distinct('id').alias('quantity_of_breweries')
    )
).to(schema.GoldSchema().BREWERY_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Gold Layer

# COMMAND ----------

libs.save_overwrite_delta(spark, f"{ROOT_PATH}{brewery.get_path('gold')}", df_gold_layer, True)
