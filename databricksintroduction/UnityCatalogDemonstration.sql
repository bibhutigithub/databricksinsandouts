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
