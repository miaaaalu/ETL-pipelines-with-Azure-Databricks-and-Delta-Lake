# Databricks notebook source
# MAGIC %md
# MAGIC temp view - only active within spark session
# MAGIC global view - can be attchaed in any spark notebok 
# MAGIC permanent view - 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC # Temp View

# COMMAND ----------

# Create a temp view to query
# temp view only active whitin current spark session, notebook and cluster 
race_result_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# run sql wihitn python 
p_race_year = 2019
race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Global Temp View

# COMMAND ----------

# Global Temporary Views 
# Global temporary can be accessed by all users in the same workspace until cluster is terminated
race_result_df.createGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results

# COMMAND ----------

# run sql wihitn python 
p_race_year = 2019
race_results_2019_df = spark.sql(f"SELECT * FROM global_temp.gv_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)
