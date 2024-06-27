# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

races_df = spark.read.parquet(f"{staging_folder_path}/races")
circuits_df = spark.read.parquet(f"{staging_folder_path}/circuits").filter(f"file_date = '{v_file_date}'")
results_df = spark.read.parquet(f"{staging_folder_path}/results")
drivers_df = spark.read.parquet(f"{staging_folder_path}/drivers")
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
        races_df.name.alias("race_name"),
        races_df.race_timestamp.alias("race_date"),
        circuits_df.location,
        drivers_df.name.alias("driver"),
        drivers_df.number,
        drivers_df.nationality,
        constructors_df.name.alias("team"),
        results_df.grid,
        results_df.fastest_lap_time,
        results_df.time.alias("race_time"),
        results_df.points, 
        results_df.position
    )
).orderBy(F.desc("points"))


# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))
