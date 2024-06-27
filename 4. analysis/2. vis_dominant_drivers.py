# Databricks notebook source
# MAGIC %sql
# MAGIC with top_drivers as (
# MAGIC   select 
# MAGIC     c.driver_name
# MAGIC     , count(1) as total_races
# MAGIC     , sum(c.calculated_points) as total_points 
# MAGIC     , avg(c.calculated_points) as avg_points
# MAGIC     , rank() over (order by avg(c.calculated_points) desc) as rank
# MAGIC   from presentation.calculated_race_results c
# MAGIC   group by c.driver_name
# MAGIC   having total_races >= 50
# MAGIC   order by avg_points desc 
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC   c.race_year
# MAGIC   , c.driver_name
# MAGIC   , count(1) as total_races
# MAGIC   , sum(c.calculated_points) as total_points 
# MAGIC   , avg(c.calculated_points) as avg_points
# MAGIC from presentation.calculated_race_results c
# MAGIC join top_drivers t on c.driver_name = t.driver_name and t.rank <= 10
# MAGIC group by 1,2
# MAGIC order by c.race_year, avg_points desc 
