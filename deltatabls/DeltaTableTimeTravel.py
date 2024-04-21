# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY dev.learning_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.learning_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM dev.learning_db.people VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM dev.learning_db.people TIMESTAMP AS OF '2024-04-21T10:31:54Z'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from dev.learning_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.learning_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC RESTORE dev.learning_db.people TO TIMESTAMP AS OF "2024-04-21T11:53:16Z"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM dev.learning_db.people
