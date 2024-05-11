-- Databricks notebook source
SELECT *FROm f1_presentation.calculate_race_results

-- COMMAND ----------

SELECT driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results 
       GROUP BY driver_name 
       HAVING count(1) >=50 
       ORDER BY avg_points desc 

-- COMMAND ----------

SELECT driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results 
       WHERE race_year between 2001 and 2010
       GROUP BY driver_name 
       HAVING count(1) >=50 
       ORDER BY avg_points desc 
