# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Containers

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

    # Get secrets from key vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-client-id')           # application-id
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-tenant-id')           # directory-id
    client_Secret = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-app-client-secret')   # service_credential

    # set spark configurations - create a mount point
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_Secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
                           
    # mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# call the function 
mount_adls('miaformula1dl', 'demo')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/miaformula1dl/demo
