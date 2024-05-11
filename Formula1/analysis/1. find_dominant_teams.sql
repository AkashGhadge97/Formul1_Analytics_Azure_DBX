-- Databricks notebook source
SELECt *FROM f1_presentation.calculate_race_results

-- COMMAND ----------

SELECT team_name,
       COUNT(1) as total_races,
       SUM(calculated_points)  as team_points,
       AVG(calculated_points) as avg_team_points
FROM f1_presentation.calculate_race_results 
where race_year  BETWEEN 2001 and 2010
group by team_name
having count(1) >= 100
order by avg_team_points desc
