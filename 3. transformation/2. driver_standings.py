# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

from pyspark.sql import functions as F

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

# DRIVER STANDING
from pyspark.sql.functions import sum, count, col, when 

driver_standing_df = race_result_df \
.groupBy("race_year", "driver", "nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank 

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = add_ingestion_date(
    driver_standing_df.withColumn("driver_rank", rank().over(driver_rank_spec))
)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.driver_standing")

# COMMAND ----------

display(
    spark.read.parquet(f"{presentation_folder_path}/driver_standing")
)
