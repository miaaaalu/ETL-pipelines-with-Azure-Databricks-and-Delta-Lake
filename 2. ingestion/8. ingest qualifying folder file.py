# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder
# MAGIC

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

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId",IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# step 2 - data transformation ( drop columns, add new columns, merging columns)
from pyspark.sql.functions import current_timestamp, lit

qualifying_final_df = qualifying_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("qualifyId", "qualify_id") \
                                 .withColumnRenamed("constructorId", "constructor_id") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# step3 - write out data to parquet file 
qualifying_final_df.write.mode("append").format("parquet").saveAsTable("staging.qualifyings")

# COMMAND ----------

df = spark.read.parquet(f"{staging_folder_path}/qualifyings")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.file_date,
# MAGIC   count(*) 
# MAGIC from staging.qualifyings as r 
# MAGIC group by 1
