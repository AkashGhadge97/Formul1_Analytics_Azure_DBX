# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest constructors.json file

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
# MAGIC #### Step 1 : Read the JSON File using Spark DataFrame Reader
# MAGIC
# MAGIC

# COMMAND ----------

const_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

const_df = spark.read.schema(const_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(const_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step -2 : Drop the unwanted columns from the dataframe

# COMMAND ----------

const_df_modified = const_df.drop(const_df["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step -3 : Rename columns and add ingestion date
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

const_final_df = const_df_modified.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
        .withColumn("ingestion_date",current_timestamp())\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step - 4 : Write data to parquet file

# COMMAND ----------

const_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECt *FROm f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
