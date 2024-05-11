# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Storage using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in Cluster COnfiguration
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1azdlv1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1azdlv1.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

df=spark.read.csv("abfss://demo@formula1azdlv1.dfs.core.windows.net/circuits.csv")
df.show()
