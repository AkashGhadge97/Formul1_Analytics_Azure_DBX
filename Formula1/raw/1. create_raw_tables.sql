-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Circuits Table [External]

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1azdlv1/raw/circuits.csv",header true)

-- COMMAND ----------

SELECT *FROm f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Races Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1azdlv1/raw/races.csv",header true)

-- COMMAND ----------

SELECT *FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create tables for JSON Files
-- MAGIC
-- MAGIC ##### Create constructors table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1azdlv1/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Create constructors table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE, 
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1azdlv1/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId   INT,      
  driverId   INT,    
  constructorId  INT, 
  number INT,        
  grid  INT,         
  position INT,       
  positionText  STRING,
  positionOrder   INT,
  points  DOUBLE,      
  laps STRING,         
  time  STRING,          
  milliseconds  INT, 
  fastestLap  INT,   
  rank INT,          
  fastestLapTime  STRING,
  fastestLapSpeed STRING,
  statusId  INT     
)
USING json
OPTIONS (path "/mnt/formula1azdlv1/raw/results.json")

-- COMMAND ----------

SELECT *FROm f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC 1. Multi Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap int,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS (path  "/mnt/formula1azdlv1/raw/pit_stops.json", multiline true)

-- COMMAND ----------

SELECT *FROm f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC 1. CSV File
-- MAGIC 2. Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS(path  "/mnt/formula1azdlv1/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC 1. JSON File
-- MAGIC 2. Multiline JSON
-- MAGIC 3. Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS(path  "/mnt/formula1azdlv1/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECt *From f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED  f1_raw.qualifying
