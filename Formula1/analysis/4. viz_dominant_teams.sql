-- Databricks notebook source
-- MAGIC %python
-- MAGIC html =  """<h1 style="color:Black;text-align:center;forn-family:Ariel">Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  v_dominant_teams
AS
SELECT team_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points,
       rank()over(order by AVG(calculated_points) desc) as team_rank
       FROM f1_presentation.calculate_race_results 
       GROUP BY team_name 
       HAVING count(1) >=100
       ORDER BY avg_points desc

-- COMMAND ----------

SELECT *FROm v_dominant_teams

-- COMMAND ----------

SELECT race_year,
       team_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results
       WHERE team_name  IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
       GROUP BY race_year,team_name 
       ORDER BY race_year, avg_points desc

-- COMMAND ----------

SELECT race_year,
       team_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
       FROM f1_presentation.calculate_race_results
       WHERE team_name  IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
       GROUP BY race_year,team_name 
       ORDER BY race_year, avg_points desc
