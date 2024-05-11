# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest races.csv file

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
# MAGIC #### Step -1 : read the CSV file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType,DateType

# COMMAND ----------

races_schema = StructType(fields=[
     StructField("raceId",IntegerType(),False),
     StructField("year",IntegerType(),True),
     StructField("round", IntegerType(),True),
     StructField("circuitId", IntegerType(),True),
     StructField("name",StringType(),True),
     StructField("date",DateType(),True),
     StructField("time",StringType(),True),
     StructField("url",StringType(),True)
])

# COMMAND ----------

races_df=spark.read.schema(races_schema).option("header",True).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step-2 : Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

races_df_selected = races_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step- 3 : Rename required columns

# COMMAND ----------

races_df_renamed = races_df_selected.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("year","race_year")\
    .withColumnRenamed("circuitId","circuit_id")\
        .withColumn("file_date",lit(v_file_date))
     

# COMMAND ----------

display(races_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 4 : Add race_timestanmp and ingestion date column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,lit,concat

# COMMAND ----------

races_derived_df = races_df_renamed.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(races_derived_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 5 : Select only required columns

# COMMAND ----------

races_final_df = races_derived_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step - 6 : Write to processed container in parquet format
# MAGIC
# MAGIC

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROm f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")
