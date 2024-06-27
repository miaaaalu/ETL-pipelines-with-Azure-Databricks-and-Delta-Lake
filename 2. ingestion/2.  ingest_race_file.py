# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest race.csv file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/formula1/includes/common_functions"

# COMMAND ----------


# create schema 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), nullable=False),
    StructField("year", IntegerType(), nullable=True),
    StructField("round", IntegerType(), nullable=True),
    StructField("circuitId", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("date", DateType(), nullable=True),
    StructField("time", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

# COMMAND ----------

races_df = spark.read \
.option("header",True)  \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# add aduit column ingest date and timestamp
from pyspark.sql.functions import current_timestamp, col, lit, to_timestamp, concat

races_added_df = races_df \
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# remove column url 
from pyspark.sql.functions import col 
races_final_df = races_added_df.select(col("raceId").alias("race_id"), 
                                    col("year").alias("race_year"), 
                                    col("round"),
                                    col("circuitId").alias("circuit_id"), 
                                    col("name"),
                                    col("race_timestamp"),
                                    col("ingestion_date"),
                                    col("data_source"))

# COMMAND ----------

# by call spark dataframe write api
races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("staging.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/miaformula1dl/staging/races
