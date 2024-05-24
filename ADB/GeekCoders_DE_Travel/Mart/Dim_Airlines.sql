-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Airlines (
  iata_code STRING,
  icao_code STRING,
  name STRING
) USING DELTA 
-- LOCATION '/mnt/mart_sink_datalake/Dim_Airlines'

-- COMMAND ----------

INSERT OVERWRITE Dim_Airlines
SELECT 
iata_code 
,icao_code 
,name 
FROM  cleansed_sink_db_geekcoders.Airline

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data from Unity Catalog
-- MAGIC df = spark.table("cleansed_sink_db_geekcoders.Airline")
-- MAGIC
-- MAGIC # Write data to ADLS Gen2
-- MAGIC df.write.mode("overwrite").format("delta").save('/mnt/mart_sink_datalake/Dim_Airlines')

-- COMMAND ----------


