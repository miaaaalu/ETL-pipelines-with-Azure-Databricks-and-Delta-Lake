# Databricks notebook source
# MAGIC %sql
# MAGIC select 
# MAGIC   c.team_name
# MAGIC   , count(1) as total_races
# MAGIC   , sum(c.calculated_points) as total_points 
# MAGIC   , avg(c.calculated_points) as avg_points
# MAGIC from presentation.calculated_race_results c
# MAGIC group by c.team_name
# MAGIC having total_races >= 100
# MAGIC order by avg_points desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   c.team_name
# MAGIC   , count(1) as total_races
# MAGIC   , sum(c.calculated_points) as total_points 
# MAGIC   , avg(c.calculated_points) as avg_points
# MAGIC from presentation.calculated_race_results c
# MAGIC where c.race_year between 2011 and 2020
# MAGIC group by c.team_name
# MAGIC having total_races >= 100
# MAGIC order by avg_points desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   c.team_name
# MAGIC   , count(1) as total_races
# MAGIC   , sum(c.calculated_points) as total_points 
# MAGIC   , avg(c.calculated_points) as avg_points
# MAGIC from presentation.calculated_race_results c
# MAGIC where c.race_year between 2001 and 2011
# MAGIC group by c.team_name
# MAGIC having total_races >= 100
# MAGIC order by avg_points desc 
