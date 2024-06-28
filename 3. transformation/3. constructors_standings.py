# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

from pyspark.sql import functions as F
race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when, desc, rank 
from pyspark.sql.window import Window

constructors_standing_df = race_result_df \
.groupBy("race_year", "constructors_name") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins")
)

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = add_ingestion_date(
    constructors_standing_df.withColumn("driver_rank", rank().over(constructors_rank_spec))
)

# COMMAND ----------

# merge_delta_data(output_df, 'staging', 'saved file name', staging_folder_path, merge_condition, 'partition key/column')
merge_condition = "tgt.constructors_name = upd.constructors_name and tgt.race_year = upd.race_year"
merge_delta_data(final_df, 'presentation', 'constructors_standing', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3122 3142
# MAGIC select
# MAGIC   count(*) 
# MAGIC from presentation.constructors_standing as r ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from presentation.constructors_standing
# MAGIC where constructors_name = 'AlphaTauri'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   file_date,
# MAGIC   constructors_name,
# MAGIC   race_year,
# MAGIC   sum(points),
# MAGIC   count(*)
# MAGIC from presentation.race_results
# MAGIC where 
# MAGIC   1=1
# MAGIC   --and file_date in ('2021-03-28', '2021-04-18')
# MAGIC   and constructors_name = 'AlphaTauri'
# MAGIC group by 1,2,3;
