-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS dev;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dev.learning_db;

-- COMMAND ----------

DROP DATABASE IF EXISTS dev.default;

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG dev TO `dataeng-grp`

-- COMMAND ----------

GRANT ALL PRIVILEGES ON DATABASE dev.learning_db TO `dataeng-grp`

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION `databricks-operations-datalake-ext-location`;

-- COMMAND ----------

GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `databricks-operations-datalake-ext-location` TO `dataeng-grp`;

-- COMMAND ----------

GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `learningdatalakestorage_ext_location` TO `dataeng-grp`;

-- COMMAND ----------

-- MAGIC %fs ls abfss://datastore@learningdatalakestorage.dfs.core.windows.net/csvdata/
