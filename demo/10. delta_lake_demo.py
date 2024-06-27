# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists demo
# MAGIC location 'dbfs:/mnt/miaformula1dl/demo/'

# COMMAND ----------

# DBTITLE 1,create table in delta
result_df = spark.read.option('inferSchema', True).json("dbfs:/mnt/miaformula1dl/raw/2021-03-28/results.json")

# COMMAND ----------

# DBTITLE 1,save both in database and adls
result_df.write.format("delta").mode("overwrite").saveAsTable("demo.results_managed")

# COMMAND ----------

# DBTITLE 1,save external in adls
result_df.write.format("delta").mode("overwrite").save("dbfs:/mnt/miaformula1dl/demo/results_external")

# COMMAND ----------

# DBTITLE 1,create table from external file
# MAGIC %sql
# MAGIC create table demo.results_external
# MAGIC using delta
# MAGIC location "dbfs:/mnt/miaformula1dl/demo/results_external"

# COMMAND ----------

results_external_df = spark.read.format("delta").load("dbfs:/mnt/miaformula1dl/demo/results_external")

# COMMAND ----------

# DBTITLE 1,create table with partition
result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("demo.results_partitioned")

# COMMAND ----------

# DBTITLE 1,Update delta table by SQL
# MAGIC %sql
# MAGIC update demo.results_managed
# MAGIC set points = 11 - position 
# MAGIC where position <= 10

# COMMAND ----------

# DBTITLE 1,Update delta table by python
from delta.tables import DeltaTable
DeltaTable = DeltaTable.forPath(spark, "dbfs:/mnt/miaformula1dl/demo/results_managed")
DeltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# DBTITLE 1,delete by sql
# MAGIC %sql 
# MAGIC delete from demo.results_managed
# MAGIC where results_managed.position > 10

# COMMAND ----------

# DBTITLE 1,delete by python
from delta.tables import DeltaTable
DeltaTable = DeltaTable.forPath(spark, "dbfs:/mnt/miaformula1dl/demo/results_managed")
DeltaTable.delete("points > 10")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge technology 
# MAGIC - day 1 from 1 - 10
# MAGIC - day 2 from 6 to 15
# MAGIC - expect result: 11 - 15

# COMMAND ----------

day1_drivers_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/miaformula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

from pyspark.sql.functions import upper

day2_drivers_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/miaformula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId between 6 and 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

from pyspark.sql.functions import upper

day3_drivers_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/miaformula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId between 1 and 5 or driverID between 16 and 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

from pyspark.sql.functions import upper

day4_drivers_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/miaformula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId between 21 and 25") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

day1_drivers_df.createOrReplaceTempView("day1_drivers");
day2_drivers_df.createOrReplaceTempView("day2_drivers");
day3_drivers_df.createOrReplaceTempView("day3_drivers");

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo.drivers_merge (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createDate Date,
# MAGIC   updateDate Date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,DAY 1
# MAGIC %sql
# MAGIC merge into demo.drivers_merge tgt 
# MAGIC using day1_drivers upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = current_timestamp
# MAGIC when not matched then 
# MAGIC   insert (driverId, dob, forename, surname, createDate) values (driverId, dob, forename, surname, current_timestamp)
# MAGIC       

# COMMAND ----------

# DBTITLE 1,DAY 2
# MAGIC %sql
# MAGIC merge into demo.drivers_merge tgt 
# MAGIC using day2_drivers upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = current_timestamp
# MAGIC when not matched then 
# MAGIC   insert (driverId, dob, forename, surname, createDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# DBTITLE 1,DAY 3
# MAGIC %sql
# MAGIC merge into demo.drivers_merge tgt 
# MAGIC using day3_drivers upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = current_timestamp
# MAGIC when not matched then 
# MAGIC   insert (driverId, 
# MAGIC           dob, 
# MAGIC           forename, 
# MAGIC           surname, 
# MAGIC           createDate
# MAGIC           ) 
# MAGIC   values (driverId, 
# MAGIC           dob, 
# MAGIC           forename, 
# MAGIC           surname, 
# MAGIC           current_timestamp
# MAGIC           )

# COMMAND ----------

# DBTITLE 1,MERGE BY USING PYSPARK
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/miaformula1dl/demo/drivers_merge')

deltaTable.alias('tgt') \
  .merge(
    day4_drivers_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updateDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId" : "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC - History & versioning
# MAGIC - time travel 
# MAGIC - vaccum
# MAGIC

# COMMAND ----------

# DBTITLE 1,HISTORY RECORDS
# MAGIC %sql
# MAGIC desc history demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #在Spark中，VACUUM 是与Delta Lake相关的功能。Delta Lake是一个开源存储层，它增加了对Apache Spark的ACID事务和时间旅行功能。VACUUM 命令用于清理Delta表中已删除或已更新的数据文件，从而释放存储空间并保持存储系统的整洁。
# MAGIC
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM demo.drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### restore deleted records 

# COMMAND ----------

# DBTITLE 1,delete record
# MAGIC %sql
# MAGIC delete from demo.drivers_merge where driverId = 1; 

# COMMAND ----------

# DBTITLE 1,check previous version
# MAGIC %sql
# MAGIC select * from demo.drivers_merge version as of 8;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into demo.drivers_merge tgt 
# MAGIC using demo.drivers_merge version as of 8  upd
# MAGIC on (tgt.driverId = upd.driverId)
# MAGIC
# MAGIC when not matched then 
# MAGIC   insert * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo.drivers_txn (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createDate Date,
# MAGIC   updateDate Date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- desc history demo.drivers_txn;
# MAGIC insert into demo.drivers_txn
# MAGIC select * from demo.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- desc history demo.drivers_txn;
# MAGIC delete from demo.drivers_txn
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history demo.drivers_convert_to_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ### convert parquet to delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,create parquet table
# MAGIC %sql
# MAGIC create table if not exists demo.drivers_convert_to_delta (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createDate Date,
# MAGIC   updateDate Date
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# DBTITLE 1,insert data
# MAGIC %sql
# MAGIC insert into demo.drivers_convert_to_delta
# MAGIC select * from demo.drivers_merge

# COMMAND ----------

# DBTITLE 1,convert parquet to delta
# MAGIC %sql
# MAGIC convert to delta demo.drivers_convert_to_delta

# COMMAND ----------

# DBTITLE 1,save as parquet file in gen2
df = spark.table("demo.drivers_convert_to_delta")
df.write.format("parquet").save("dbfs:/mnt/miaformula1dl/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# DBTITLE 1,convert exteral file into delta
# MAGIC %sql
# MAGIC convert to delta parquet.`dbfs:/mnt/miaformula1dl/demo/drivers_convert_to_delta_new`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from demo.drivers_merge version as of 1;
# MAGIC select * from demo.drivers_merge;
# MAGIC
# MAGIC -- df = spark.read.format("delta").option("version", 1).load("dbfs:/mnt/miaformula1dl/demo/drivers_merge")
# MAGIC -- display(df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/miaformula1dl/demo
