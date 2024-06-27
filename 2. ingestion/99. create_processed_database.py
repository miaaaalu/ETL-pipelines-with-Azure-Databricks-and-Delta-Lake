# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists staging
# MAGIC location "dbfs:/mnt/miaformula1dl/staging"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database staging;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database staging;
