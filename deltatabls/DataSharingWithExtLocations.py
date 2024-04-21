# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS external_data
# MAGIC URL 'abfss://externaldata@learningdatalakestorage.dfs.core.windows.net/shared_external_data'
# MAGIC WITH (CREDENTIAL `databricks-operations-datalake-mi`)

# COMMAND ----------

# MAGIC %fs ls abfss://externaldata@learningdatalakestorage.dfs.core.windows.net/shared_external_data/

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data as delta format into external tables**

# COMMAND ----------

datasource_loc = (
    "abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/"
)

external_sink_loc = "abfss://externaldata@learningdatalakestorage.dfs.core.windows.net/shared_external_data/"

flight_schema = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED STRING, DISTANCE INT"""

flight_time_df = (
    spark.read.format("json")
    .schema(flight_schema)
    .option("dateFormat", "M/d/y")
    .load(f"{datasource_loc}/flight-time.json")
)

flight_time_df.coalesce(1).write.format("delta").option(
    "path", f"{external_sink_loc}"
).mode("overwrite").save()

# COMMAND ----------

external_sink_loc = "abfss://externaldata@learningdatalakestorage.dfs.core.windows.net/shared_external_data/"
external_data_df = spark.read.format("delta").load(external_sink_loc)
display(external_data_df)
