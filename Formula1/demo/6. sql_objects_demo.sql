-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. SQP SQL Docs
-- MAGIC 2. Create Database Demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW Command
-- MAGIC 5. DESCRIEBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE DEMO;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run  "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create Managed table using python
-- MAGIC race_results_df.write.saveAsTable("demo.race_results_python")

-- COMMAND ----------

use DEMO

-- COMMAND ----------

show tables

-- COMMAND ----------

desc extended race_results_python

-- COMMAND ----------

select *from demo.race_results_python  where race_year = 2020

-- COMMAND ----------

<!--Create Managed Table using SQL-->
CREATE TABLE  race_results_sql AS
select *from demo.race_results_python  where race_year = 2020

-- COMMAND ----------

select *from race_results_sql

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create external table using python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Create External table using python
-- MAGIC
-- MAGIC race_results_df.write.option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------

<!-- Create External Table using SQL -->

CREATE TABLE demo.race_results_ext_sql 
(
  rece_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1azdlv1/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW tables IN demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql 
SELECT *from demo.race_results_ext_py where race_year = 2020

-- COMMAND ----------

SELECT COUNT(*) from demo.race_results_ext_sql 

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### View on tables
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create globla temp view
-- MAGIC 3. Create permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  v_race_results AS SELECT *from demo.race_results_python WHERE race_year = 2018

-- COMMAND ----------

SELECT *FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW  gv_race_results AS SELECT *from demo.race_results_python WHERE race_year = 2018

-- COMMAND ----------

SELECT *from global_temp.gv_race_results

-- COMMAND ----------

DESC EXTENDED global_temp.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE  VIEW  pv_race_results 
AS SELECT *from demo.race_results_python WHERE race_year = 2018

-- COMMAND ----------

SHOW tables 

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

select *from pv_race_results
