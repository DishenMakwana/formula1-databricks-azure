# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results WHERE race_year = 2020;

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_result_2020_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")
display(race_result_2020_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;