-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl0404/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/formula1dl0404/presentation";