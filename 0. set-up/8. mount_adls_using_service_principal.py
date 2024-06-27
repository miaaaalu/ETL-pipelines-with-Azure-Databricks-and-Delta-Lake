# Databricks notebook source
# MAGIC %md
# MAGIC # [Mount ADLS using service principal](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts#--mount-adls-gen2-or-blob-storage-with-abfs)
# MAGIC
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 2. set spark config with app/client id, directory/tenant id & secret
# MAGIC 3. call file system utlity mount to mount the storage
# MAGIC 4. explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

# Get client_id, tenant_id, and client_secret from key vault
# application-id
client_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-client-id')

# directory-id
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-tenant-id')

# service_credential
client_Secret = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-client-secret')

# storage account 
storage_account = "miaformula1dl"
container_name = "playground"
mount_path = f"/mnt/{storage_account}/{container_name}"


# COMMAND ----------

# create a mount point
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_Secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# execute the mount point 
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
  mount_point = mount_path,
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(mount_path))

# COMMAND ----------

#  read files 
display(
    spark.read.csv(f"{mount_path}/circuits.csv")
)

# COMMAND ----------

# other related commands 
display(dbutils.fs.mounts())
