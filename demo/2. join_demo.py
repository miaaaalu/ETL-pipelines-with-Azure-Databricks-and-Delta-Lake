# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

races_df = spark.read.parquet(f"{staging_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{staging_folder_path}/circuits").filter("circuit_id < 70")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **inner, left, full join**

# COMMAND ----------

# inner join 
race_circuits_df = circuits_df \
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.name.alias("circuit_name")
        , circuits_df.location
        , circuits_df.country
        , races_df.name.alias("race_name")
        , races_df.round
        )

# COMMAND ----------

# left join
race_circuits_df = circuits_df \
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.name.alias("circuit_name")
        , circuits_df.location
        , circuits_df.country
        , races_df.name.alias("race_name")
        , races_df.round
        )

# COMMAND ----------

# full join
race_circuits_df = circuits_df \
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.name.alias("circuit_name")
        , circuits_df.location
        , circuits_df.country
        , races_df.name.alias("race_name")
        , races_df.round
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Semi, Anti, Cross Joins**

# COMMAND ----------

# semi join -  return all rows both in left and right side of join, but only return columns in the left table
race_circuits_df = circuits_df \
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

# anti join -  return all rows both not in left and right side of join, and only return left table columns 
race_circuits_df = circuits_df \
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

# cross join 
race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

display(race_circuits_df)
