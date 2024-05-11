# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
    .filter(f"file_date ='{v_file_date}'")\
    .withColumnRenamed("time","race_time")\
    .withColumnRenamed("race_id","result_race_id")\
    .withColumnRenamed("file_date","result_file_date")

race_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name","race_name").withColumnRenamed("race_timestamp","race_date")

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location","circuit_location")

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")

const_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name","team")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_results_df = race_df.join(results_df, results_df.result_race_id == race_df.race_id)\
.join(circuits_df, circuits_df.circuit_id == race_df.circuit_id)\
.join(drivers_df, drivers_df.driver_id == results_df.driver_id)\
.join(const_df,results_df.constructor_id == const_df.constructor_id)\
    .select(results_df.result_race_id,race_df.race_year,race_df.race_name,race_df.race_date,circuits_df.circuit_location,drivers_df.driver_name,drivers_df.driver_number,drivers_df.driver_nationality,const_df.team,results_df.grid,results_df.fastest_lap,results_df.race_time,results_df.points,results_df.position,results_df.result_file_date).withColumn("created_date",current_timestamp())

# COMMAND ----------

#final_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

#inal_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_d_presentation.race_results")

#handle_incremental_load(db="f1_presentation",table="race_results",partitionColumn="result_race_id",input_df=final_results_df)

merge_delta_Data(db_name="f1_presentation",table="race_results",folder_path=presentation_folder_path,input_df=final_results_df,partitionColumn="result_race_id",merge_condition="tgt.driver_name = src.driver_name AND tgt.result_race_id = src.result_race_id")
