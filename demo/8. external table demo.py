# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# DBTITLE 1,# create spark table by python
race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# DBTITLE 1,# create spark table by sql
# MAGIC %sql
# MAGIC create table demo.race_results_ext_sql
# MAGIC (
# MAGIC   race_year	int,
# MAGIC   race_name	string,
# MAGIC   race_date	timestamp,
# MAGIC   location string,
# MAGIC   driver	string,
# MAGIC   number	int,
# MAGIC   nationality	string,
# MAGIC   team	string,
# MAGIC   grid	int,
# MAGIC   fastest_lap_time	string,
# MAGIC   race_time	string,
# MAGIC   points	float,
# MAGIC   position	int,
# MAGIC   ingestion_date timestamp
# MAGIC )
# MAGIC using parquet
# MAGIC location "/mnt/miaformula1dl/presentation/race_results_ext_sql"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo.race_results_ext_sql
# MAGIC select * from demo.race_results_ext_py where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo.race_results_ext_sql;
