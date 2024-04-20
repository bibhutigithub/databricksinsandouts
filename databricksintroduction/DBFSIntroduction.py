# Databricks notebook source
dbutils.fs.ls("dbfs:/")

for file_info in dbutils.fs.ls("dbfs:/"):
    print(file_info.path)

# COMMAND ----------

for mount_info in dbutils.fs.mounts():
    print(mount_info.mountPoint)
