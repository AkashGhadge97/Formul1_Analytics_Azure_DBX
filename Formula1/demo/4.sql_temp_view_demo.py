# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Access dataframes using SQL
# MAGIC ##### Objective
# MAGIC 1. Create temporary view on dataframes
# MAGIC 2. Access the view from SQL Cell
# MAGIC 3. Acess the view from Python Cell
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#race_results_df.createTempView("v_race_results")
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *from v_race_results where race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("select *from v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

p_race_year = 2019
race_results_2019_df = spark.sql(f"select *from v_race_results where race_year = {p_race_year}")
display(race_results_2019_df)

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Global Temporary View
# MAGIC 1. Create global temporary view on dataframes
# MAGIC 2. Access the view from sql cell
# MAGIC 3. Acces the view form python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *from global_temp.gv_race_results

# COMMAND ----------

display(spark.sql("select *from global_temp.gv_race_results"))
