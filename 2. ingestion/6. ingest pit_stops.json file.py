# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# step 1 - read the json file using the spark dataframe reader API
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

pit_stops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time",StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")


# COMMAND ----------

# step 2 - data transformation ( drop columns, add new columns, merging columns)
from pyspark.sql.functions import current_timestamp, lit

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# merge_delta_data(output_df, 'staging', 'saved file name', staging_folder_path, merge_condition, 'partition key/column')
merge_condition = "tgt.race_id = upd.race_id and tgt.driver_id = upd.driver_id and tgt.stop = upd.stop and tgt.race_id = upd.race_id"
merge_delta_data(pit_stops_final_df, 'staging', 'pit_stops', staging_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.file_date,
# MAGIC   count(*) 
# MAGIC from staging.pit_stops as r 
# MAGIC group by 1
