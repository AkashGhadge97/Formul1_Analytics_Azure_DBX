# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest pit_stops.json file

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
# MAGIC #### Step-1 : Read the JSON file using Spark daraframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("stop",StringType(),True),
    StructField("lap",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("duration",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

pit_stops_df=spark.read.option("multiLine",True).schema(pit_stops_schema).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-2 : Rename columns and add new columns
# MAGIC 1. Rename driveId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("raceId","race_id")\
        .withColumn("ingestion_date",current_timestamp())\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 : Write output to processed container in parquet

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
#handle_incremental_load(db="f1_processed",input_df=pit_stops_final_df,table="pit_stops",partitionColumn="race_id")

merge_delta_Data(db_name="f1_processed",table="pit_stops",folder_path=processed_folder_path,input_df=pit_stops_final_df,partitionColumn="race_id",merge_condition="tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECt distinct file_date from f1_processed.pit_stops

# COMMAND ----------

dbutils.notebook.exit("Success")
