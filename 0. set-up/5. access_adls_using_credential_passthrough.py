# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using credential_passthrough
# MAGIC
# MAGIC 1. set the spark config fs.azure.account.key in the cluster
# MAGIC 2. list files from container 
# MAGIC 3. read data from circuits.csv file

# COMMAND ----------

# azure blob file system driver, and configuration parameter 
# abfss://[container_name]@[storage_account_name].dfs.core.windows.net/[folder_path][file_name]
# list files 
display(
    dbutils.fs.ls("abfss://playground@miaformula1dl.dfs.core.windows.net")
    )

# COMMAND ----------

#  read files 
display(
    spark.read.csv("abfss://playground@miaformula1dl.dfs.core.windows.net/circuits.csv")
)
