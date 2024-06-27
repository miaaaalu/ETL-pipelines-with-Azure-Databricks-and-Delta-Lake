# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;
# MAGIC SHOW databases;
# MAGIC describe database demo;
# MAGIC describe database extended demo;
# MAGIC select current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;
# MAGIC
# MAGIC use demo;
# MAGIC select current_database();
# MAGIC show tables;
