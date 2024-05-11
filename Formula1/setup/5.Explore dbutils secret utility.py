# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

dbutils.secrets.get("formula1-scope","formula1-dl-account-key")

# COMMAND ----------

access_key = dbutils.secrets.get("formula1-scope","formula1-dl-account-key")    
