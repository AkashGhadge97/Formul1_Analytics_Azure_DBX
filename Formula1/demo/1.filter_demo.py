# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# SQL Way
races_filterd_df = races_df.filter("race_year == 2019 and round <= 5")
#races_filterd_df = races_df.where("race_year == 2019 and round <= 5")

# COMMAND ----------

#Python Way
races_filterd_df = races_df.filter((races_df.race_year == 2019) &(races_df.round <= 5))
#races_filterd_df = races_df.where((races_df.race_year == 2019) &(races_df.round <= 5))

# COMMAND ----------

display(races_filterd_df)
