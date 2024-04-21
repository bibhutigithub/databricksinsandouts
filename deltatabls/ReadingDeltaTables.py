# Databricks notebook source
datasource_loc = (
    "abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/"
)
flight_db_name = "dev.learning_db.flight_details"

flight_schema = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED STRING, DISTANCE INT"""

flight_time_df = (
    spark.read.format("json")
    .schema(flight_schema)
    .option("dateFormat", "M/d/y")
    .load(f"{datasource_loc}/flight-time.json")
)

flight_time_df.write.mode("overwrite").saveAsTable(f"{flight_db_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.learning_db.flight_details

# COMMAND ----------

external_data_df = spark.read.format("delta").table("dev.learning_db.flight_details")
display(external_data_df)
