# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest qualifying json files

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
# MAGIC
# MAGIC #### Step-1 : Read the json file using Spark DataFrame Read API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,DoubleType,StringType

# COMMAND ----------

qualify_schema = StructType(fields=[
    StructField("qualifyId",IntegerType(),False),
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),False),
    StructField("constructorId",IntegerType(),False),
    StructField("number",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("q1",StringType(),False),
    StructField("q2",StringType(),False),
    StructField("q3",StringType(),True)
])

# COMMAND ----------

qualify_df = spark.read.option("multiLine",True).schema(qualify_schema).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-2 : Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualify_final_df = qualify_df.withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("file_date",lit(v_file_date))
        




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-3 : Write to the parquet format

# COMMAND ----------

#handle_incremental_load(db="f1_processed",partitionColumn="race_id",input_df=qualify_final_df,table="qualifying")
merge_delta_Data(db_name="f1_processed",table="qualifying",folder_path=processed_folder_path,input_df=qualify_final_df,partitionColumn="race_id",merge_condition="tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT file_date from  f1_processed.qualifying
