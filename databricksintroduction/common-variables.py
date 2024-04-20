# Databricks notebook source
country="India"
state="Odisha"
print(country, state)

# COMMAND ----------

dbutils.widgets.text("msg","","")

# COMMAND ----------

msg = dbutils.widgets.get("msg")

# COMMAND ----------

print(msg)

# COMMAND ----------

dbutils.notebook.exit("200 Ok")
