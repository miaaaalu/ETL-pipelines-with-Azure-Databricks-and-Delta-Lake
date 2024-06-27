# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_results_df.write.format("parquet").saveAsTable("demo.race_results_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo;
# MAGIC show tables;
# MAGIC desc extended race_results_py;
# MAGIC -- select * from demo.race_results_py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table race_results_2020
# MAGIC as 
# MAGIC select * from demo.race_results_py where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;
# MAGIC desc extended demo.race_results_2020;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo.race_results_2020;
