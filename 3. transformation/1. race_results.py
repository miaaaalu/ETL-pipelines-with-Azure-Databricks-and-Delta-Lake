# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

# DBTITLE 1,read delta table
races_df = spark.read.format("delta").load(f"{staging_folder_path}/races")
circuits_df = spark.read.format("delta").load(f"{staging_folder_path}/circuits")
results_df = spark.read.format("delta").load(f"{staging_folder_path}/results").filter(f"file_date = '{v_file_date}'")
drivers_df = spark.read.format("delta").load(f"{staging_folder_path}/drivers")
constructors_df = spark.read.format("delta").load(f"{staging_folder_path}/constructors")

# COMMAND ----------

display(
    results_df.select("file_date").distinct()
)

# COMMAND ----------

from pyspark.sql import functions as F

final_df = add_ingestion_date(
    results_df
    .join(races_df, results_df.race_id == races_df.race_id, "inner")
    .join(drivers_df, drivers_df.driver_id == results_df.driver_id, "inner")
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, "inner")
    .join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner")
    .select(
        races_df.race_id,
        races_df.race_year,
        races_df.name.alias("race_name"),
        races_df.race_timestamp.alias("race_date"),
        circuits_df.location,
        drivers_df.name.alias("driver_name"),
        drivers_df.number,
        drivers_df.nationality,
        constructors_df.name.alias("constructors_name"),
        results_df.grid,
        results_df.fastest_lap_time,
        results_df.time.alias("race_time"),
        results_df.points, 
        results_df.position,
        results_df.file_date
    )
    .withColumn("created_date", current_timestamp())
).orderBy(F.desc("points"))


# COMMAND ----------

# merge_delta_data(output_df, 'staging', 'saved file name', staging_folder_path, merge_condition, 'partition key/column')
merge_condition = "tgt.driver_name = upd.driver_name and tgt.race_id = upd.race_id"
merge_delta_data(final_df, 'presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.file_date,
# MAGIC   count(*) 
# MAGIC from presentation.race_results as r 
# MAGIC group by 1

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table presentation.race_results;
