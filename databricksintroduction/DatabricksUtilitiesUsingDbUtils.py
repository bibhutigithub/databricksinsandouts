# Databricks notebook source
# MAGIC %md
# MAGIC ### There are various utilities available in Databricks. Those are below
# MAGIC - credentials
# MAGIC - data
# MAGIC - fs
# MAGIC - jobs
# MAGIC - library
# MAGIC - meta
# MAGIC - notebook
# MAGIC - secrets
# MAGIC - widgets
# MAGIC - preview
# MAGIC

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

for folder_name in dbutils.fs.ls("dbfs:/databricks-datasets/"):
    print(folder_name.path)

# COMMAND ----------

dbutils.notebook.run("./common-variables",10, {"msg":"Hello!!! It's called from Parent."})

# COMMAND ----------

dbutils.widgets.text("sampleinput","Hello","")

# COMMAND ----------

greet_msg = dbutils.widgets.get("sampleinput")
print(greet_msg)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/mydatalake/csvsource/")

# COMMAND ----------

df = spark.read.option("header","true").csv("dbfs:/mnt/mydatalake/csvsource/country.csv")
display(df)
