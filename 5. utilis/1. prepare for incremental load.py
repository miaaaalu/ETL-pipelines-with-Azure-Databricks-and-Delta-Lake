# Databricks notebook source
# MAGIC %sql
# MAGIC Drop database if exists staging cascade;
# MAGIC create database if not exists staging
# MAGIC location "dbfs:/mnt/miaformula1dl/staging"

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop database if exists presentation cascade;
# MAGIC create database if not exists presentation
# MAGIC location "dbfs:/mnt/miaformula1dl/presentation"

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

raw_folder_path

# COMMAND ----------

path = '2021-04-18'
spark.read.json(f"/mnt/miaformula1dl/raw/{path}/results.json").createOrReplaceTempView("results3")

# COMMAND ----------

# MAGIC %sql
# MAGIC select r1.raceId, count(*)
# MAGIC from results1 r1
# MAGIC group by r1.raceId
# MAGIC ORDER BY r1.raceId DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select r1.raceId, count(*)
# MAGIC from results2 r1
# MAGIC group by r1.raceId
# MAGIC ORDER BY r1.raceId DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select r1.raceId, count(*)
# MAGIC from results3 r1
# MAGIC group by r1.raceId
# MAGIC ORDER BY r1.raceId DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   r.race_id,
# MAGIC   count(*) 
# MAGIC from staging.results as r 
# MAGIC group by 1
