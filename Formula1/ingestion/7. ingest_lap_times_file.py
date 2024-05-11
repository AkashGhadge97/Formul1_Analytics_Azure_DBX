# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest lap times foler

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1 : Read the CSV files using Spark daraframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("lap",IntegerType(),True),
    StructField("position",StringType(),True),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-2 : Rename columns and add new columns
# MAGIC 1. Rename driveId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("raceId","race_id")\
        .withColumn("ingestion_date",current_timestamp())\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 : Write output to processed container in parquet

# COMMAND ----------

#handle_incremental_load(input_df=lap_times_final_df,db="f1_processed",partitionColumn="race_id",table="lap_times")

merge_delta_Data(db_name="f1_processed",table="lap_times",folder_path=processed_folder_path,input_df=lap_times_final_df,partitionColumn="race_id",merge_condition="tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT file_date FROM f1_processed.lap_times

# COMMAND ----------

dbutils.notebook.exit("Success")
