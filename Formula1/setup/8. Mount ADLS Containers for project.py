# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Storage containers for project

# COMMAND ----------

def mount_adls_container(storage_account,container):

  client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-sp-client-id')
  tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-sp-tenant-id')
  client_secret= dbutils.secrets.get(scope='formula1-scope', key='formula1-sp-client-secret')

  configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  
  #Unmoint the mount if mount is lareday present
  if any(mount.mountPoint == f"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f"/mnt/{storage_account}/{container}")

  dbutils.fs.mount(
  source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account}/{container}",
  extra_configs = configs)

# COMMAND ----------

mount_adls_container("formula1azdlv1","raw")
mount_adls_container("formula1azdlv1","processed")
mount_adls_container("formula1azdlv1","presentation")

# COMMAND ----------

mount_adls_container("formula1azdlv1","demo")

# COMMAND ----------

display(dbutils.fs.mounts())
