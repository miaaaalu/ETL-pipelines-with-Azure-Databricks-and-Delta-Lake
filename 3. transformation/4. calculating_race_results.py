# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view race_result_upd
# MAGIC as 
# MAGIC select 
# MAGIC   rc.race_year
# MAGIC   , c.name as team_name
# MAGIC   , d.driver_id
# MAGIC   , d.name as driver_name
# MAGIC   , rc.race_id
# MAGIC   , rs.position
# MAGIC   , rs.points
# MAGIC   , 11 - rs.position as calculated_points
# MAGIC from staging.results rs 
# MAGIC join staging.drivers d      on d.driver_id = rs.driver_id
# MAGIC join staging.constructors c on c.constructor_id = rs.constructor_id
# MAGIC join staging.races rc       on rs.race_id = rc.race_id
# MAGIC where 
# MAGIC   rs.position <= 10
# MAGIC   and rs.file_date = '{v_file_date}'

# COMMAND ----------

# DBTITLE 1,1. create temp view
spark.sql(f"""
    create or replace temp view race_result_upd
    as 
    select 
        rc.race_year
        , c.name as team_name
        , d.driver_id
        , d.name as driver_name
        , rc.race_id
        , rs.position
        , rs.points
        , 11 - rs.position as calculated_points
    from staging.results rs 
    join staging.drivers d      on d.driver_id = rs.driver_id
    join staging.constructors c on c.constructor_id = rs.constructor_id
    join staging.races rc       on rs.race_id = rc.race_id
    where 
    rs.position <= 10
    and rs.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
  create table if not exists presentation.calculated_race_results
  (
    race_year int,
    team_name string, 
    driver_id int,
    driver_name string,
    race_id int, 
    position int,
    calculated_points int,
    created_date timestamp,
    updated_date timestamp
  )
  using delta
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from presentation.calculated_race_results
# MAGIC
