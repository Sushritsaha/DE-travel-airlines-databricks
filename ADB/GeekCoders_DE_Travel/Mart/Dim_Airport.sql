-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Airport (
  code STRING,
  city STRING,
  country STRING,
  airport STRING
) USING DELTA 
-- LOCATION '/mnt/mart_sink_datalake/Dim_Airport'

-- COMMAND ----------

INSERT OVERWRITE Dim_Airport
SELECT 
code 
,city 
,country 
,airport 
FROM  cleansed_sink_db_geekcoders.Airport 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data from Unity Catalog
-- MAGIC df = spark.table("cleansed_sink_db_geekcoders.Airport")
-- MAGIC
-- MAGIC # Write data to ADLS Gen2
-- MAGIC df.write.mode("overwrite").format("delta").save('/mnt/mart_sink_datalake/Dim_Airport')

-- COMMAND ----------


