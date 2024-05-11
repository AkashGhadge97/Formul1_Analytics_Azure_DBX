# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest drivers.json file

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
# MAGIC #### Step-1 : Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename",StringType(),True),
    StructField("surname",StringType(),True)
])

# COMMAND ----------

drive_schema = StructType(fields=[
    StructField("driverId",IntegerType(),False),
    StructField("driverRef",StringType(),True),
    StructField("number",IntegerType(),True),
    StructField("code",StringType(),True),
    StructField("name",name_schema),
    StructField("dob",DateType(),True),
    StructField("nationality",StringType(),True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

driver_df = spark.read.schema(drive_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 : Rename the columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. added ingestion_date
# MAGIC 4. Concatenated forename and surname to name

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,col,lit

# COMMAND ----------

driver_df_modified = driver_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
        .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 : Drop the unwanted columns
# MAGIC 1. name.forname
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

driver_final_df = driver_df_modified.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-4 : Write output to processed container in parquest format

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROm f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")
