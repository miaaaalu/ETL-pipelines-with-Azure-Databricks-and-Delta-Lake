# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

races_df = spark.read.parquet(f"{staging_folder_path}/races")

# COMMAND ----------

# filter funciton - sql way 
races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

# filter funciton - python way 
races_filtered_df = races_df.filter((races_df["race_year"]== 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)
