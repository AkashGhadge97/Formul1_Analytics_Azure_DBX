# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run  "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the year for which the data is to be processed

# COMMAND ----------

race_results_list =spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"result_file_date = '{v_file_date}'").select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col

# COMMAND ----------

driver_standings_df  = race_results_df.groupBy("race_year","driver_name","driver_nationality").agg(sum("points").alias("total_points"),count(when(col("position") == 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc , rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driverRankSpec))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

merge_delta_Data(db_name="f1_presentation",table="driver_standings",folder_path=presentation_folder_path,input_df=final_df,partitionColumn="race_year",merge_condition="tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year")

