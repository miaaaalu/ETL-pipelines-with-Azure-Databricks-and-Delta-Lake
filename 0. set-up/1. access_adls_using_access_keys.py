# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using access keys 
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 2. list files from demo container 
# MAGIC 3. read data from circuits.csv file

# COMMAND ----------

access_key = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-access-key')

# COMMAND ----------

# configuration parameter 
# spark.conf.set(
#   "fs.azure.account.key.[storage_account_name].dfs.core.windows.net",
#   "[access key]"
# )

spark.conf.set(
    "fs.azure.account.key.miaformula1dl.dfs.core.windows.net",
    access_key
)

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
