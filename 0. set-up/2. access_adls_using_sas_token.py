# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using sas token 
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 2. list files from demo container 
# MAGIC 3. read data from circuits.csv file
# MAGIC
# MAGIC ```py
# MAGIC
# MAGIC   storage_account = "your_storage_account"
# MAGIC   sas_token_key = "your_sas_token_key"
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.auth.type.<storage_account>.dfs.core.windows.net", 
# MAGIC     "SAS"
# MAGIC     )
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.sas.token.provider.type.<storage_account>.dfs.core.windows.net", 
# MAGIC     "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
# MAGIC     )
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.sas.fixed.token.<storage_account>.dfs.core.windows.net", 
# MAGIC     dbutils.secrets.get(<sas_token_key>)
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC Replace
# MAGIC - <storage-account> with the Azure Storage account name.
# MAGIC - <sas-token-key> with the name of the key containing the Azure storage SAS token.
# MAGIC
# MAGIC [reference:](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#sastokens)  
# MAGIC

# COMMAND ----------

sas_token = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-sas-token')

# COMMAND ----------


spark.conf.set(
    "fs.azure.account.auth.type.miaformula1dl.dfs.core.windows.net", 
    "SAS"
    )

spark.conf.set(
    "fs.azure.sas.token.provider.type.miaformula1dl.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
    )

spark.conf.set(
    "fs.azure.sas.fixed.token.miaformula1dl.dfs.core.windows.net", 
    sas_token
    )

# COMMAND ----------

#  list files 
display(
    dbutils.fs.ls("abfss://playground@miaformula1dl.dfs.core.windows.net")
    )

#  read files 
display(
    spark.read.csv("abfss://playground@miaformula1dl.dfs.core.windows.net/circuits.csv")
)
