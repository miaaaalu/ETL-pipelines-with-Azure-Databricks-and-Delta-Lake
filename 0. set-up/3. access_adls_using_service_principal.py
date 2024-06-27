# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using service principal
# MAGIC
# MAGIC 1. register Azure AD Application / service principal
# MAGIC 2. generate a secret/password for the application 
# MAGIC 3. [set spark config with app/client id, directory/tenant id & secret](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal) 
# MAGIC 4. assign role 'storage blob data contributor' to the data lake 
# MAGIC
# MAGIC ```py
# MAGIC # step 3 
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", 
# MAGIC     "OAuth"
# MAGIC     )
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", 
# MAGIC     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
# MAGIC     )
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", 
# MAGIC     "<application-id>"
# MAGIC     )
# MAGIC
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", 
# MAGIC     service_credential
# MAGIC     )
# MAGIC     
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", 
# MAGIC     "https://login.microsoftonline.com/<directory-id>/oauth2/token"
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# application-id
client_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-client-id')

# directory-id
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-tenant-id')

# service_credential
client_Secret = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-client-secret')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.auth.type.miaformula1dl.dfs.core.windows.net", 
    "OAuth"
    )

spark.conf.set(
    "fs.azure.account.oauth.provider.type.miaformula1dl.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )

spark.conf.set(
    "fs.azure.account.oauth2.client.id.miaformula1dl.dfs.core.windows.net", 
    client_id
    )

spark.conf.set(
    "fs.azure.account.oauth2.client.secret.miaformula1dl.dfs.core.windows.net", 
    client_Secret
    )

spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.miaformula1dl.dfs.core.windows.net", 
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
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
