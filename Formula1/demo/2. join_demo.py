# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df  = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019").withColumnRenamed("name","race_name")

# COMMAND ----------

circuits_df  = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### INNER JOIN

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"inner").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OUTER JOIN

# COMMAND ----------

circuits_df  = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name").where("circuit_id < 70")

# COMMAND ----------

#Left Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"left").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#Right Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"right").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#Full Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"full").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins

# COMMAND ----------

# Semi Join
# Similar to inner join but you will get columns only from left datafrmae. You won't be able to select any right dataframe columns. Semi joins gives data which was availble on both dataframe 

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"semi").select("*")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Anti Join
# MAGIC
# MAGIC

# COMMAND ----------

# Anti Join
# Anti join gives you everything on left dataframe which is not availble in right dataframe

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"anti").select("*")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join

# COMMAND ----------

#Cross Join gives the cartesian product between two tables
race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)
