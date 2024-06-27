# Databricks notebook source
# MAGIC %md
# MAGIC Data Load Types 
# MAGIC - Full Load - receive all data all times 
# MAGIC - Incremental Load - only receive data changed last time 
# MAGIC
# MAGIC Hybrid Scenarios 
# MAGIC - Full Dataset received, but data loaded & transformed incrementally. This would require another proces up front to identify the changes. 
# MAGIC - Incremental dataset received, but data loaded & transformed in full 
# MAGIC - data received contains both full and incremental files
# MAGIC - Incremental dataset received, ingested incrementally & transformed in full 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Data Warehouse 
# MAGIC - good on BI workloads, 
# MAGIC - neg on support ML, Streaming, AI
# MAGIC
# MAGIC Data Lake 
# MAGIC - mainly focused on data science, Machine learning, 
# MAGIC - difficult to combine streaming tech and batch tech 
# MAGIC - difficult on bi projects, ACID, versiong, history, time travel
# MAGIC
# MAGIC Delta lake 
# MAGIC - data lake with ACID transaction control

# COMMAND ----------

# MAGIC %md
# MAGIC **project challenge 
# MAGIC **
# MAGIC how to handle duplicated data 
# MAGIC - inital arthciture desisgn is based on data lake, 
# MAGIC - to handle duplicate data i overwrite the partiton id, but the issue is some past data will be corrected, and it won't be load. 
# MAGIC - so to solve this, to redevelop the entire pipeline to delta lake architcher. 
# MAGIC
# MAGIC
# MAGIC data lake challenge
# MAGIC - unable to handle duplicate data 
# MAGIC - ubable to roll bakc data 
# MAGIC - no history of versioning of the data 
# MAGIC

# COMMAND ----------

# remove column url - way1
# suit for simple select 
circuits_selected_df = circuits_df.select("circuitId", 
                                          "circuitRef",
                                          "name",
                                          "location",
                                          "country",
                                          "lat",
                                          "lng",
                                          "alt")

# remove column url - way2 
# suit for IDE, can reduce spelling error
circuits_selected_df = circuits_df.select(circuits_df.circuitId,
                                          circuits_df.circuitRef,
                                          circuits_df.name,
                                          circuits_df.location,
                                          circuits_df.country,
                                          circuits_df.country,
                                          circuits_df.lat,
                                          circuits_df.lng,
                                          circuits_df.alt)
# remove column url - way3
# suit for IDE, can reduce spelling error

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],
                                          circuits_df["circuitRef"],
                                          circuits_df["name"],
                                          circuits_df["location"],
                                          circuits_df["country"],
                                          circuits_df["country"],
                                          circuits_df["lat"],
                                          circuits_df["lng"],
                                          circuits_df["alt"])
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

# MAGIC %sql
# MAGIC select * 
# MAGIC from staging.drivers
