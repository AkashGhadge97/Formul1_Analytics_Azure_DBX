-- Databricks notebook source
-- MAGIC %python
-- MAGIC html =  """<h1 style="color:Black;text-align:center;forn-family:Ariel">Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  v_dominant_drivers
AS
SELECT driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points,
       rank()over(order by AVG(calculated_points) desc) as driver_rank
       FROM f1_presentation.calculate_race_results 
       GROUP BY driver_name 
       HAVING count(1) >=50 
       ORDER BY avg_points desc 

-- COMMAND ----------

SELECT race_year,
       driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results
       WHERE driver_name  IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
       GROUP BY race_year,driver_name 
       ORDER BY race_year, avg_points desc

-- COMMAND ----------

SELECT race_year,
       driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results
       WHERE driver_name  IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
       GROUP BY race_year,driver_name 
       ORDER BY race_year, avg_points desc

-- COMMAND ----------

SELECT race_year,
       driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results
       WHERE driver_name  IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
       GROUP BY race_year,driver_name 
       ORDER BY race_year, avg_points desc
