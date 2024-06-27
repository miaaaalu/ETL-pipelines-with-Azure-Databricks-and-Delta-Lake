# Databricks notebook source
# MAGIC %md
# MAGIC - temp view - only active within spark session
# MAGIC - global view - can be attchaed in any spark notebok 
# MAGIC - permanent view - 

# COMMAND ----------

# DBTITLE 1,temp view
# MAGIC %sql
# MAGIC create or replace temp view v_race_results 
# MAGIC as 
# MAGIC   select * 
# MAGIC   from demo.race_results_py
# MAGIC   where race_year = 2018;

# COMMAND ----------

# DBTITLE 1,global temp view
# MAGIC %sql
# MAGIC create or replace global temp view gv_race_results 
# MAGIC as 
# MAGIC   select * 
# MAGIC   from demo.race_results_py
# MAGIC   where race_year = 2018;

# COMMAND ----------

# DBTITLE 1,permanent view
# MAGIC %sql
# MAGIC create or replace view demo.pv_race_results
# MAGIC as 
# MAGIC   select * 
# MAGIC   from demo.race_results_py
# MAGIC   where race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;
# MAGIC show tables in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.pv_race_results;
