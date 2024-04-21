# Databricks notebook source
# MAGIC %md
# MAGIC ***Create Delta Table First***

# COMMAND ----------

people_json_loc = "abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json"
schema_ddl = """id INT, fname STRING, lname STRING, dob STRING"""
people_df = spark.read.format("json").schema(schema_ddl).load(f"{people_json_loc}")
people_df.write.format("delta").mode("overwrite").saveAsTable("dev.learning_db.people")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.learning_db.people;

# COMMAND ----------

# MAGIC %md
# MAGIC **Delta table DML operations are supported by SQL only and not through API level** 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM dev.learning_db.people WHERE fname = 'M David'

# COMMAND ----------

# MAGIC %md
# MAGIC **Implement Delete, Update or Merge statements via DeltaTable API**

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import *

source_data = [
    (101, "Prashanta Kumar", "Pandey", "1970-03-20"),
    (102, "Veer Abdul", "Hamid", "1985-11-25"),
    (105, "Siddhartha Sankar", "Mohapatra", "1986-06-15"),
    (106, "Soumya Kumar", "Ratha", "1986-10-20"),
]
source_schema = ["id", "FirstName", "LastName", "BirthDate"]
source_df = spark.createDataFrame(source_data, source_schema)
people_dt = DeltaTable.forName(spark, "dev.learning_db.people")
# display(source_df)
# display(people_dt.toDF())
(
    people_dt.alias("tgt")
    .merge(source_df.alias("src"), "tgt.id=src.id")
    .whenMatchedUpdate(
        condition="src.id = tgt.id",
        set={
            "tgt.fname": "src.FirstName",
            "tgt.lname": "src.LastName",
            "tgt.dob": "src.BirthDate",
        },
    )
    .whenNotMatchedInsert(
        values={
            "id": col("src.id"),
            "fname": col("src.FirstName"),
            "lname": col("src.LastName"),
            "dob": col("src.BirthDate"),
        }
    ).execute()
)
# people_dt = DeltaTable.forName(spark, "dev.learning_db.people")
# people_dt.delete(condition=expr("fname='M David'"))
# people_dt.update(condition=expr("fname='Kailash'"),set={"fname":lit("Kailasha"),"lname":lit("Patel"),#"dob":lit("1973-10-05")})

people_dt.toDF().show()
