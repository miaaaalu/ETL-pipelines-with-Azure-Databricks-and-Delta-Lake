# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times_folder

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

# step 1 - read the folder using the spark dataframe reader API
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position",IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# step 2 - data transformation ( drop columns, add new columns, merging columns)
from pyspark.sql.functions import current_timestamp, lit

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# step3 - write out data to parquet file 
lap_times_final_df.write.mode("append").format("parquet").saveAsTable("staging.lap_times")

# COMMAND ----------

df = spark.read.parquet(f"{staging_folder_path}/lap_times")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.file_date,
# MAGIC   count(*) 
# MAGIC from staging.lap_times as r 
# MAGIC group by 1
