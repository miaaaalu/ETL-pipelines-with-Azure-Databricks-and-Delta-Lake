# Databricks notebook source
# MAGIC %sql
# MAGIC use staging;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists presentation.calculated_race_results;
# MAGIC create table presentation.calculated_race_results 
# MAGIC using parquet 
# MAGIC as 
# MAGIC select 
# MAGIC   rc.race_year
# MAGIC   , c.name as team_name
# MAGIC   , d.name as driver_name
# MAGIC   , rs.position
# MAGIC   , rs.points
# MAGIC   , 11 - rs.position as calculated_points
# MAGIC from staging.results rs 
# MAGIC join staging.drivers d on d.driver_id = rs.driver_id
# MAGIC join staging.constructors c on c.constructor_id = rs.constructor_id
# MAGIC join staging.races rc on rs.race_id = rc.race_id
# MAGIC where rs.position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from presentation.calculated_race_results
# MAGIC
