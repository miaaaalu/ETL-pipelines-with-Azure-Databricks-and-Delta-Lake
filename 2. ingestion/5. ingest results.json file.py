# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

# DBTITLE 1,set parameter - data source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,set parameter - ingest date
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,call configuration function
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,call programming used function
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# step 1 - read the json file using the spark dataframe reader
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number",IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])


# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")


# COMMAND ----------

# step 2 - data transformation ( drop columns, add new columns, merging columns)
from pyspark.sql.functions import col, concat, current_timestamp, lit

results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# DBTITLE 1,deduplicate dataset
deduped_final_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

merge_condition = "tgt.result_id = upd.result_id and tgt.race_id = upd.race_id"
merge_delta_data(deduped_final_df, 'staging', 'results', staging_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.file_date,
# MAGIC   count(*) 
# MAGIC from staging.results as r 
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.file_date,
# MAGIC   r.race_id, 
# MAGIC   r.driver_id,
# MAGIC   count(*) 
# MAGIC from staging.results as r 
# MAGIC --where r.driver_id = 612
# MAGIC group by 1,2,3
# MAGIC having count(*) > 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.race_id,
# MAGIC   count(*) 
# MAGIC from staging.results as r 
# MAGIC group by 1
# MAGIC order by 1 desc;
