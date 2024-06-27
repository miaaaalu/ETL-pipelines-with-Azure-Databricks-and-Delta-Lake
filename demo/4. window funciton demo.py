# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# count of unique race names
from pyspark.sql.functions import count, countDistinct, sum

race_results_df.select(countDistinct("race_name")).show()

# COMMAND ----------

# sum of the points when driver is Lewis Hamilton
race_results_df.filter("driver = 'Lewis Hamilton'").select(
    sum("points").alias("total points"), 
    countDistinct("race_name").alias("races")
).show()                            

# COMMAND ----------

race_results_df.groupby("driver").sum("points").show()

# COMMAND ----------

display(race_results_df)
