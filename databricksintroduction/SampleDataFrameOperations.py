# Databricks notebook source
mydf = spark.read.option("header","true").csv("abfss://datastore@learningdatalakestorage.dfs.core.windows.net/csvdata/");
display(mydf);
