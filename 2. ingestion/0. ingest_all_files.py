# Databricks notebook source
v_result = dbutils.notebook.run("1. ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("2.  ingest_race_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest constructors.json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest drivers.json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest results.json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("6. ingest pit_stops.json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("7. ingest lap_times folder file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("8. ingest qualifying folder file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
