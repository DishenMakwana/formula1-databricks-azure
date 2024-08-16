# Databricks notebook source
# MAGIC %md
# MAGIC 1. setup spark config
# MAGIC 2. list files
# MAGIC 3. read data from file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl0404.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0404.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl0404.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df_spark = spark.read.csv("abfss://demo@formula1dl0404.dfs.core.windows.net/circuits.csv")

df = df_spark.toPandas()

# COMMAND ----------

df.describe()
df.head()