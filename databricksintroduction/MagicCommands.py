# Databricks notebook source
# MAGIC %md
# MAGIC &copy; 2024-2025 Bibhuti Pvt Ltd. All rights Reserved

# COMMAND ----------

msg = "Hello"

num_arr = (1, 2, 3, 4)

# COMMAND ----------

print (msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL"

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val value = "Sample Value"
# MAGIC println(value)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls databricks-datasets

# COMMAND ----------

# MAGIC %sh
# MAGIC tail /databricks-datasets/COVID/covid-19-data/us-counties.csv
