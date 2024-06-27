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

constructors_standing_df = race_result_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank 

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = add_ingestion_date(
    constructors_standing_df.withColumn("driver_rank", rank().over(constructors_rank_spec))
)

# COMMAND ----------

display(final_df.filter(col("race_year") == 2020))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.constructors_standing")

# COMMAND ----------

display(
    spark.read.parquet(f"{presentation_folder_path}/constructors_standing")
)
