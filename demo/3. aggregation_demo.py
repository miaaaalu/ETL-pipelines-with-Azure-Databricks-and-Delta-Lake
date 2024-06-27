# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

# RACE
races_df = spark.read.parquet(f"{staging_folder_path}/races")

# CIRCUITS
circuits_df = spark.read.parquet(f"{staging_folder_path}/circuits")

# RESULTS
results_df = spark.read.parquet(f"{staging_folder_path}/results")

# DRIVER
drivers_df = spark.read.parquet(f"{staging_folder_path}/drivers")

# CONSTRUCTORS
constructors_df = spark.read.parquet(f"{staging_folder_path}/constructors")

# COMMAND ----------

from pyspark.sql import functions as F

final_df = add_ingestion_date(
    results_df
    .join(races_df, results_df.race_id == races_df.race_id, "inner")
    .join(drivers_df, drivers_df.driver_id == results_df.driver_id, "inner")
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, "inner")
    .join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner")
    .select(
        races_df.race_year,
        races_df.name.alias("race name"),
        races_df.race_timestamp.alias("race date"),
        circuits_df.location,
        drivers_df.name.alias("driver"),
        drivers_df.number,
        drivers_df.nationality,
        constructors_df.name.alias("team"),
        results_df.grid,
        results_df.fastest_lap_time,
        results_df.time.alias("race time"),
        results_df.points
    )
).orderBy(F.desc("points"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation Function

# COMMAND ----------

# count of unique race names
from pyspark.sql.functions import count, countDistinct, sum
agg_df = final_df.filter("race_year = 2020")
agg_df.select(countDistinct("race name")).show()

# COMMAND ----------

# sum of the points when driver is Lewis Hamilton
agg_df.filter("driver = 'Lewis Hamilton'").select(
    sum("points").alias("total points"), 
    countDistinct("race name").alias("races")
).show()          

# COMMAND ----------

# display the total points and number of races for each driver
display(
    agg_df.groupBy("driver").agg(sum("points").alias("total points"), countDistinct("race name").alias("races"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Window Function

# COMMAND ----------

# display the total points and number of races for each driver
win_df = final_df \
.filter("race_year in (2019, 2020)") \
.groupBy("race_year", "driver") \
.agg(sum("points").alias("total points"), 
     countDistinct("race name").alias("races")
)

# COMMAND ----------

# DBTITLE 1,# rank the drivers by total points
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total points"))
display(win_df.withColumn("rank", rank().over(driverRankSpec)))
