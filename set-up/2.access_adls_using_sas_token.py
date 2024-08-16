# Databricks notebook source
# MAGIC %md
# MAGIC 1. setup spark config
# MAGIC 2. list files
# MAGIC 3. read data from file

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl0404.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl0404.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl0404.dfs.core.windows.net", formula1_demo_sas_token)

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

df.head()