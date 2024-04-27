# Databricks notebook source
# MAGIC %md
# MAGIC **Schema Evolution Approaches**
# MAGIC
# MAGIC There are two approaches of schema evolution
# MAGIC
# MAGIC 1. Manual
# MAGIC 2. Automatic

# COMMAND ----------

data_path = "abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json"
db_name = "dev.learning_db.people"
spark.sql(f"""INSERT INTO {db_name} SELECT id, fname, lname FROM json.`{data_path}`""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Manual Schema Evolution**

# COMMAND ----------

spark.sql("ALTER TABLE dev.learning_db.people ADD COLUMNS (dateOfBirth STRING AFTER lastName)")

# COMMAND ----------

spark.sql("INSERT INTO dev.learning_db.people SELECT id, fname, lname, dob FROM json.`abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json`")

# COMMAND ----------

# MAGIC %md
# MAGIC **Set schema evolution automatic**
# MAGIC
# MAGIC spark configuration at the session level. It will work for all the tables in the particular session

# COMMAND ----------

spark.conf.set("sprk.databricks.delta.schema.autoMerged.enabled","false")

# COMMAND ----------

# MAGIC %md
# MAGIC **If we want to do it on the table level use DataFrame API**

# COMMAND ----------

people_schema = "id INT, fname STRING, lname STRING, dob STRING"
people_df = spark.read.format("json").schema(people_schema).load("abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json")
(people_df
 .withColumnRenamed("fname","FirstName")
 .withColumnRenamed("lname","LastName")
 .write
 .format("delta")
 .option("mergeSchema","true")
 .mode("append")
 .saveAsTable("dev.learning_db.people"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.learning_db.people
