# Databricks notebook source
# MAGIC %md
# MAGIC 1. setup spark config
# MAGIC 2. list files
# MAGIC 3. read data from file

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl0404.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df_spark = spark.read.csv("dbfs:/mnt/formula1dl/demo/circuits.csv")

df = df_spark.toPandas()

# COMMAND ----------

df.head()

# COMMAND ----------

display(dbutils.fs.mounts())