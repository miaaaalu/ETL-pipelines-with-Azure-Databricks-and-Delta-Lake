# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists raw;

# COMMAND ----------

# DBTITLE 1,circuits table
# MAGIC %sql
# MAGIC drop table if exists raw.circuits;
# MAGIC create table if not exists raw.circuits(
# MAGIC   circuitId integer,
# MAGIC   circuitRef string,
# MAGIC   name string,
# MAGIC   location string,
# MAGIC   country string,
# MAGIC   lat double,
# MAGIC   lng double,
# MAGIC   alt integer,
# MAGIC   url string
# MAGIC )
# MAGIC using csv 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/circuits.csv", header true)

# COMMAND ----------

# DBTITLE 1,races table
# MAGIC %sql
# MAGIC drop table if exists raw.races;
# MAGIC create table if not exists raw.races (
# MAGIC   raceId integer,
# MAGIC   year integer,
# MAGIC   round integer,
# MAGIC   circuitId integer,
# MAGIC   name string,
# MAGIC   date date,
# MAGIC   time string,
# MAGIC   url string
# MAGIC )
# MAGIC using csv 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/races.csv", header true)

# COMMAND ----------

# DBTITLE 1,constructors - single line json
# MAGIC %sql
# MAGIC drop table if exists raw.constructors;
# MAGIC
# MAGIC create table if not exists raw.constructors (
# MAGIC   constructorId integer,
# MAGIC   constructorRef string,
# MAGIC   name string,
# MAGIC   nationality string,
# MAGIC   url string
# MAGIC )
# MAGIC using json 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/constructors.json", header true)

# COMMAND ----------

# DBTITLE 1,drivers - single line json with nested cell
# MAGIC %sql
# MAGIC drop table if exists raw.drivers;
# MAGIC
# MAGIC create table if not exists raw.drivers (
# MAGIC   driverId integer,
# MAGIC   driverRef string,
# MAGIC   number integer,
# MAGIC   code string,
# MAGIC   name struct <forename: string, surname string>,
# MAGIC   dob date,
# MAGIC   nationality string,
# MAGIC   url string
# MAGIC )
# MAGIC using json 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/drivers.json", header true)

# COMMAND ----------

# DBTITLE 1,results - single line json
# MAGIC %sql
# MAGIC drop table if exists raw.results;
# MAGIC
# MAGIC create table if not exists raw.results (
# MAGIC   resultId integer,
# MAGIC   raceId integer,
# MAGIC   driverId integer,
# MAGIC   constructorId integer,
# MAGIC   number integer,
# MAGIC   grid integer,
# MAGIC   position integer,
# MAGIC   positionText string,
# MAGIC   positionOrder integer,
# MAGIC   points float,
# MAGIC   laps integer,
# MAGIC   time string,
# MAGIC   milliseconds integer,
# MAGIC   fastestLap integer,
# MAGIC   rank integer,
# MAGIC   fastestLapTime string,
# MAGIC   fastestLapSpeed string,
# MAGIC   statusId integer
# MAGIC )
# MAGIC using json 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/results.json", header true)

# COMMAND ----------

# DBTITLE 1,pit_stops - multi line json
# MAGIC %sql
# MAGIC drop table if exists raw.pit_stops;
# MAGIC
# MAGIC create table if not exists raw.pit_stops (
# MAGIC   raceId integer,
# MAGIC   driverId integer,
# MAGIC   stop integer,
# MAGIC   lap integer,
# MAGIC   time string,
# MAGIC   duration string,
# MAGIC   milliseconds integer
# MAGIC )
# MAGIC using json 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/pit_stops.json", multiLine true, header true)

# COMMAND ----------

# DBTITLE 1,lap_times - multiple files csv
# MAGIC %sql
# MAGIC drop table if exists raw.lap_times;
# MAGIC
# MAGIC create table if not exists raw.lap_times (
# MAGIC   raceId integer,
# MAGIC   driverId integer,
# MAGIC   lap integer,
# MAGIC   position integer,
# MAGIC   time string,
# MAGIC   milliseconds integer
# MAGIC )
# MAGIC using csv 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/lap_times/", header true)

# COMMAND ----------

# DBTITLE 1,qualifying - multiple files & multiline json
# MAGIC %sql
# MAGIC drop table if exists raw.qualifying;
# MAGIC
# MAGIC create table if not exists raw.qualifying (
# MAGIC   qualifyId integer,
# MAGIC   raceId integer,
# MAGIC   driverId integer,
# MAGIC   constructorId integer,
# MAGIC   number integer,
# MAGIC   position integer,
# MAGIC   q1 string,
# MAGIC   q2 string,
# MAGIC   q3 string
# MAGIC )
# MAGIC using json 
# MAGIC options (path "dbfs:/mnt/miaformula1dl/raw/qualifying/", multiLine true, header true)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/miaformula1dl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.qualifying;
