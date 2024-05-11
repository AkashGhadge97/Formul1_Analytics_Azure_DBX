# Databricks notebook source
# MAGIC %md
# MAGIC #### Delta Lake
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1azdlv1/demo"

# COMMAND ----------


results_df = spark.read.option("inferSchema",True).json("/mnt/formula1azdlv1/raw/2021-03-28/results.json")

# COMMAND ----------

#Write data to delta lake (managed table)
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *from f1_demo.results_managed

# COMMAND ----------

#Write data to delta lake (external table)
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1azdlv1/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating external table
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1azdlv1/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_external

# COMMAND ----------

results_external_df= spark.read.format("delta").load("/mnt/formula1azdlv1/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

#Writing partioned table to the delta lake

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points  = 11 - position
# MAGIC WHERE position <= 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1azdlv1/demo/results_managed")

deltaTable.update("position <= 10",{"points":"21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM f1_demo.results_managed where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1azdlv1/demo/results_managed")

deltaTable.delete("points= 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using Merge

# COMMAND ----------

    driver_day1_df = spark.read.option("inferSchema",True).json("/mnt/formula1azdlv1/raw/2021-03-28/drivers.json").filter("driverId <= 10").select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df = spark.read.option("inferSchema",True).json("/mnt/formula1azdlv1/raw/2021-03-28/drivers.json").filter("driverId between 6 AND 15").select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

driver_day3_df = spark.read.option("inferSchema",True).json("/mnt/formula1azdlv1/raw/2021-03-28/drivers.json").filter("driverId between 1 AND 5 OR driverID between 16 AND 20").select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day1 upd
# MAGIC ON tgt.driverId = upd.driverID
# MAGIC WHEN MATCHED THEN 
# MAGIC    UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updateDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC    THEN INSERT (driverId,dob,forename,surname,createDate) VALUES (driverID,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECt *FROm f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day2 upd
# MAGIC ON tgt.driverId = upd.driverID
# MAGIC WHEN MATCHED THEN 
# MAGIC    UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updateDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC    THEN INSERT (driverId,dob,forename,surname,createDate) VALUES (driverID,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECt *FROM f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import current_timestamp
deltaTable = DeltaTable.forPath(spark, '/mnt/formula1azdlv1/demo/drivers_merge')

deltaTable.alias('tgt') \
  .merge(
    driver_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updateDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECt *FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECt *FROM f1_demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt *FROM f1_demo.drivers_merge VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt *FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-05-10T06:33:42.000+00:00'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf","2024-05-10T06:33:42.000+00:00").load("/mnt/formula1azdlv1/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0  hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt *FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-05-10T06:33:42.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt *FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM f1_demo.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT *FROM f1_demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Transaction Logs
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT *FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Convert Parquest to DELTA
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECt *FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1azdlv1/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1azdlv1/demo/drivers_convert_to_delta_new`
