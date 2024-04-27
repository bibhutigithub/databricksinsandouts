# Databricks notebook source
# MAGIC %fs ls '/Volumes/dev/learning_db/files/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dev.learning_db.files;

# COMMAND ----------

data_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

from pyspark.sql.functions import to_date, year

result_df = (data_df
             .withColumnRenamed("Call Number", "CallNumber")
             .withColumnRenamed("Unit ID", "UnitId")
             .withColumnRenamed("Call Date", "CallDate")
             .withColumn("Year", year(to_date("CallDate","yyyy-mm-dd")))
             .select("CallNumber","UnitId","CallDate","Year"))

result_df.write.format("parquet").mode("overwrite").partitionBy("Year").save("/Volumes/dev/learning_db/files/sf-fire-calls_tbl")


# COMMAND ----------

# MAGIC %md
# MAGIC **It's not recommended to read all the parquet files and then write that again with delta format. Have to convert it in place.**

# COMMAND ----------

# MAGIC %fs ls "/Volumes/dev/learning_db/files/sf-fire-calls_tbl"

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/Volumes/dev/learning_db/files/sf-fire-calls_tbl` PARTITION BY (Year INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/dev/learning_db/files/sf-fire-calls_tbl`;
