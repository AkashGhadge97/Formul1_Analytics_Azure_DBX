# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS Root
# MAGIC 2. Interact with DBFS File browser
# MAGIC 3. Upload File to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

df = spark.read.csv('/FileStore/tables',header=True)
display(df)
