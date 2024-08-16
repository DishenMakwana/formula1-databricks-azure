# Databricks notebook source
# MAGIC %md
# MAGIC 1. setup spark config
# MAGIC 2. list files
# MAGIC 3. read data from file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dl0404.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl0404.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0404.dfs.core.windows.net"))
display(dbutils.fs.ls("abfss://raw@formula1dl0404.dfs.core.windows.net"))
display(dbutils.fs.ls("abfss://processed@formula1dl0404.dfs.core.windows.net"))

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