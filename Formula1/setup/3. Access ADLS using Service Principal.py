# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Storage using Service Principal
# MAGIC ##### Steps to follow :
# MAGIC
# MAGIC 1. Register Azure AD Application /Service principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id and secret
# MAGIC 4. Assign Role "Storage Blob Data Contributor" to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-sp-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-sp-tenant-id')
client_secret= dbutils.secrets.get(scope='formula1-scope', key='formula1-sp-client-secret')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1azdlv1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1azdlv1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1azdlv1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1azdlv1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1azdlv1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1azdlv1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1azdlv1.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

df=spark.read.csv("abfss://demo@formula1azdlv1.dfs.core.windows.net/circuits.csv")
df.show()
