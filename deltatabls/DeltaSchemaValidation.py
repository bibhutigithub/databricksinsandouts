# Databricks notebook source
# MAGIC %md
# MAGIC Delta tables have schema validation already in place. Below are the different ways we can Insert/Update data inside a delta table.
# MAGIC
# MAGIC 1. INSERT
# MAGIC 2. OVERWRITE
# MAGIC 3. MERGE STATEMENTS
# MAGIC 4. DataFrame API SAVE Operations.
# MAGIC
# MAGIC **Schema validations**
# MAGIC 1. Column Name validation.
# MAGIC 2. Data Type Validation.
# MAGIC 3. Additional Column Validation.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.learning_db.people (id INT, FirstName STRING, LastName STRING);

# COMMAND ----------

# MAGIC %md
# MAGIC **INSERT Statement illustration**
# MAGIC
# MAGIC Here we are inserting records to a table which have different column names. Still we are managed to insert it as far as column type is same. So, INSERT statement is checking only matching positions not matching names. It's a potential issue. This can corrupt the data.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev.learning_db.people SELECT id, fname, lname from json.`abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json`;

# COMMAND ----------

# MAGIC %md
# MAGIC **INSERT statement with new columns. It's not allowed.**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev.learning_db.people SELECT id, fname, lname, dob from json.`abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json`

# COMMAND ----------

# MAGIC %md
# MAGIC **INSERT OVERWRITE Statements**
# MAGIC
# MAGIC Schema validation is in place for INSERT OVERWRITE and it's not allowed to add new columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE dev.learning_db.people SELECT id, fname, lname, dob from json.`abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json`

# COMMAND ----------

# MAGIC %md
# MAGIC **Merge Statements**
# MAGIC
# MAGIC It's not like INSERT statements. It's checking the column names. So mismatched column names not allowed.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dev.learning_db.people tgt USING (SELECT id, fname, lname FROM json.`abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json`) src ON tgt.id = src.id WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC **Merge Statements New Columns**
# MAGIC
# MAGIC New columns will not throw errors rather it will be silently ignored.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dev.learning_db.people tgt USING (SELECT id, fname FirstName, lname LastName, dob FROM json.`abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json`) src ON tgt.id = src.id WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC **DataFrame API Operations**
# MAGIC
# MAGIC All schema validations are in place with no loopholes.

# COMMAND ----------

people_schema = "id INT, fname STRING, lname STRING"
people_df = spark.read.format("json").schema(people_schema).load("abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json")
people_df.write.format("delta").mode("append").saveAsTable("dev.learning_db.people")

# COMMAND ----------

people_schema = "id INT, fname STRING, lname STRING, dob STRING"
people_df = spark.read.format("json").schema(people_schema).load("abfss://datastore@learningdatalakestorage.dfs.core.windows.net/jsondatasets/people.json")
display(people_df)
people_df.write.format("delta").mode("append").saveAsTable("dev.learning_db.people")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.learning_db.people;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE dev.learning_db.people;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dev.learning_db.people;
