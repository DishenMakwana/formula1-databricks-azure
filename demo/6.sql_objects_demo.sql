-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;


-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_result_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC race_result_python;

-- COMMAND ----------

DESC EXTENDED race_result_python

-- COMMAND ----------


CREATE TABLE race_results_sql AS 
SELECT * FROM race_result_python WHERE race_year = 2020

-- COMMAND ----------

DESC TABLE EXTENDED race_results_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC TABLE EXTENDED race_results_ext_py


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results AS
SELECT * FROM race_result_python WHERE race_year = 2020


-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results AS
SELECT * FROM race_result_python WHERE race_year = 2018


-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp