# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------


# create schema 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), nullable=False),
    StructField("circuitRef", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lng", DoubleType(), nullable=True),
    StructField("alt", IntegerType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

# COMMAND ----------

# read dataframe by call spark dataframe read api

circuits_df = spark.read \
.option("header",True)  \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# remove column url - way4
# using col fucntion can be more flexiable in complext columns like nested column 
from pyspark.sql.functions import col, lit
circuits_selected_df = circuits_df.select(col("circuitId"), 
                                          col("circuitRef"),
                                          col("name"),
                                          col("location"),
                                          col("country"),
                                          col("lat"),
                                          col("lng"),
                                          col("alt"))


# COMMAND ----------

# rename columns 
circuits_renamed_df = circuits_selected_df \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# add aduit column - ingest date 
from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# by call spark dataframe write api
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("staging.circuits")
