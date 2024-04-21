# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Tables Introduction**

# COMMAND ----------

flight_dbname = "dev.learning_db.flight_details"

# COMMAND ----------

# MAGIC %md
# MAGIC ***Different ways to create delta tables***

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Using SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.learning_db.flight_details(
# MAGIC   FL_DATE DATE,
# MAGIC   OP_CARRIER STRING,
# MAGIC   OP_CARRIER_FL_NUM INT,
# MAGIC   ORIGIN STRING,
# MAGIC   ORIGIN_CITY_NAME STRING,
# MAGIC   DEST STRING,
# MAGIC   DEST_CITY_NAME STRING,
# MAGIC   CRS_DEP_TIME INT,
# MAGIC   DEP_TIME INT,
# MAGIC   WHEELS_ON INT,
# MAGIC   TAXI_IN INT,
# MAGIC   CRS_ARR_TIME INT,
# MAGIC   ARR_TIME INT,
# MAGIC   CANCELLED STRING,
# MAGIC   DISTANCE INT
# MAGIC )

# COMMAND ----------

df = spark.sql(f"DESCRIBE FORMATTED {flight_dbname}");
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Using DataFrame API**

# COMMAND ----------

datasourceloc = (
    "abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/"
)

flight_schema = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED STRING, DISTANCE INT"""

flight_time_df = (
    spark.read.format("json")
    .schema(flight_schema)
    .option("dateFormat","M/d/y")
    .load(f"{datasourceloc}/flight-time.json")
)

flight_time_df.write.format("delta").mode("append").saveAsTable(f"{flight_dbname}")

# COMMAND ----------

result_df = spark.sql(f"SELECT * FROM {flight_dbname}")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Using Delta Table Builder API**

# COMMAND ----------

from delta import DeltaTable

(
    DeltaTable.createOrReplace(spark)
    .tableName(f"{flight_dbname}")
    .addColumn("FL_DATE", "DATE")
    .addColumn("OP_CARRIER", "STRING")
    .addColumn("OP_CARRIER_FL_NUM", "INT")
    .addColumn("ORIGIN", "STRING")
    .addColumn("ORIGIN_CITY_NAME", "STRING")
    .addColumn("DEST", "STRING")
    .addColumn("DEST_CITY_NAME", "STRING")
    .addColumn("CRS_DEP_TIME", "INT")
    .addColumn("DEP_TIME", "INT")
    .addColumn("WHEELS_ON", "INT")
    .addColumn("TAXI_IN", "INT")
    .addColumn("CRS_ARR_TIME", "INT")
    .addColumn("ARR_TIME", "INT")
    .addColumn("CANCELLED", "STRING")
    .addColumn("DISTANCE", "INT")
).execute()
