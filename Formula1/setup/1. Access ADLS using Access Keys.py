# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Storage using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from csv file

# COMMAND ----------

access_key = dbutils.secrets.get(scope='formula1-scope',key='formula1-dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1azdlv1.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1azdlv1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1azdlv1.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

df=spark.read.csv("abfss://demo@formula1azdlv1.dfs.core.windows.net/circuits.csv")
df.show()
