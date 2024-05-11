# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def handle_incremental_load(input_df,db,table,partitionColumn):

    #set spark config
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    #Move partition column to last
    column_list = []
    for col in input_df.schema.names:
        if col != partitionColumn:
            column_list.append(col)
    column_list.append(partitionColumn)
    output_df = input_df.select(column_list)

    #Handle Incremental
    if(spark._jsparkSession.catalog().tableExists(f"{db}.{table}")):
        output_df.write.mode("overwrite").insertInto(f"{db}.{table}")
    else:
        output_df.write.mode("overwrite").partitionBy(partitionColumn).format("parquet").saveAsTable(f"{db}.{table}")



# COMMAND ----------

def df_column_to_list(input_df,column_name):
    df_row_list = input_df.select(column_name).distinct().collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_Data(db_name,table,folder_path,merge_condition,input_df,partitionColumn):
    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table}")):
        deltaTable = DeltaTable .forPath(spark, f"{folder_path}/{table}")
        deltaTable.alias("tgt").merge(
            input_df.alias('src'),
            merge_condition\
        ).whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partitionColumn).format("delta").saveAsTable(f"{db_name}.{table}")
