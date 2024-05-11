# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(f"result_file_date= '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count, when,col, desc,asc

# COMMAND ----------

const_standings_df = race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"), count(when(col("position") == 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

team_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = const_standings_df.withColumn("rank",rank().over(team_rank_spec))

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructors_standings")
merge_delta_Data(db_name="f1_presentation",table="constructors_standings",folder_path=presentation_folder_path,input_df=final_df,partitionColumn="race_year",merge_condition="tgt.team = src.team AND tgt.race_year = src.race_year")
