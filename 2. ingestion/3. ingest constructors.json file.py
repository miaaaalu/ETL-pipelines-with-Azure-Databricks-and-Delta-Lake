# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

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

# step 1 - read the json file using the spark dataframe reader
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# step 2 - drop unwanted columns from the dataframe 
# way1 - constructor_dropped_df = constructor_df.drop('url')
# way2 - constructor_dropped_df = constructor_df.drop(constructor_df['url'])
# way3 
from pyspark.sql.functions import col 
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# step 3 - rename columns and add ingestion date
from pyspark.sql.functions import current_timestamp, lit

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# step4 - write out data to parquet file 
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("staging.constructors")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/miaformula1dl/staging/constructors
