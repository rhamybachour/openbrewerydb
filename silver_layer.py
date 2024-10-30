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

spark = SparkSession.builder.appName("Breweries Case Silver Layer").getOrCreate()

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
# MAGIC ## Reading Bronze Layer

# COMMAND ----------

# Initialize an instance of the Base class with specified endpoint and context.
brewery = lib.Base(endpoint="api.openbrewerydb.org", context="breweries")

# Read data from the Delta table located at the specified path (constructed using ROOT_PATH and the get_path method)
df_silver_layer = (spark
    .read
    # Force schema to read the files with the same columns defined
    .schema(schema.BronzeSchema().BREWERY_SCHEMA)
    .json(f"{ROOT_PATH}{brewery.get_path('bronze', 'raw')}")
    # Convert the schema to save the delta table with the same columns defined
    .to(schema.SilverSchema().BREWERY_SCHEMA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Silver Layer

# COMMAND ----------

libs.save_merged_delta(spark, f"{ROOT_PATH}{brewery.get_path('silver')}", df_silver_layer, ['id'])
