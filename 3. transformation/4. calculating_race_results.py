# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,1. CREATE TABLE WITH SCHEMA
spark.sql(f"""
  create table if not exists presentation.calculated_race_results
  (
    race_year int,
    team_name string, 
    driver_id int,
    driver_name string,
    race_id int, 
    position int,
    points int,
    calculated_points int,
    created_date timestamp,
    updated_date timestamp
  )
  using delta
""")

# COMMAND ----------

# DBTITLE 1,2. CREATE TEMP VIEW
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
    -- FULL LOAD TABLES  
    join staging.drivers d      on d.driver_id = rs.driver_id
    join staging.constructors c on c.constructor_id = rs.constructor_id
    join staging.races rc       on rs.race_id = rc.race_id
    where 
    rs.position <= 10
    and rs.file_date = '{v_file_date}'
""")

# COMMAND ----------

# DBTITLE 1,3. CREATE MERGE STATEMENT
spark.sql(f"""
          
    MERGE INTO presentation.calculated_race_results as tgt
    USING race_result_upd as upd
    ON (tgt.driver_id = upd.driver_id and tgt.race_id = upd.race_id)
    WHEN MATCHED THEN
    UPDATE SET tgt.position = upd.position,
                tgt.points = upd.points,
                tgt.calculated_points = upd.calculated_points,
                tgt.updated_date = current_timestamp
    WHEN NOT MATCHED
    THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, calculated_points,created_date ) 
        VALUES (race_year, team_name, driver_id, driver_name, race_id, position, calculated_points, current_timestamp)        
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table presentation.calculated_race_results;
# MAGIC select count(*) from presentation.calculated_race_results;
