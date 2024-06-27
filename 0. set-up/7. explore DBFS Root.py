# Databricks notebook source
# MAGIC %md
# MAGIC # Explore DBFS Root
# MAGIC 1. List all the folders in DBFS Root
# MAGIC 2. Interact with DBFS Filer Browser 
# MAGIC 3. Upload file to DBFS Root
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))
