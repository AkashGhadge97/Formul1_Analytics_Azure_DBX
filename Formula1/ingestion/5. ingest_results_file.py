# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-1 : Read the json file using Spark DataFrame Read API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,DoubleType,StringType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId",IntegerType(),False),
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),False),
    StructField("constructorId",IntegerType(),False),
    StructField("number",IntegerType(),True),
    StructField("grid",IntegerType(),False),
    StructField("position",IntegerType(),True),
    StructField("positionText",StringType(),False),
    StructField("positionOrder",IntegerType(),False),
    StructField("points",DoubleType(),False),
    StructField("laps",StringType(),False),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
    StructField("fastestLap",IntegerType(),True),
    StructField("rank",IntegerType(),True),
    StructField("fastestLapTime",StringType(),True),
    StructField("fastestLapSpeed",StringType(),True),
    StructField("statusId",IntegerType(),False)
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-2 : Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

results_df_renamed = results_df.withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("positionText","position_text")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("fastestLap","fastest_lap")\
    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("file_date",lit(v_file_date))
        




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-3 : Drop unwanted columns

# COMMAND ----------

results_final_df = results_df_renamed.drop("statusId")

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-5 : Write to the parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Methdo 1 of incremental load

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Methdo 2 of incremental load

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

results_deduped_df = results_deduped_df.select("result_id",          
"driver_id",          
"constructor_id",     
"number",             
"grid",               
"position",           
"position_text",     
"position_order",     
"points",             
"laps",               
"time",               
"milliseconds",       
"fastest_lap",        
"rank",               
"fastest_lap_time",   
"fastest_lap_speed",  
"ingestion_date",    
"file_date","race_id")


# COMMAND ----------

merge_delta_Data(db_name="f1_processed",table="results",folder_path=processed_folder_path,input_df=results_deduped_df,partitionColumn="race_id",merge_condition="tgt.result_id = src.result_id AND tgt.race_id = src.race_id")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT race_id, count(1) from f1_processed.results GROUP BY race_id ORDER BY race_id desc
