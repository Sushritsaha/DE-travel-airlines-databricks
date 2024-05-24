-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Cancellation (
  code STRING,
  description STRING
) USING DELTA 
-- LOCATION '/mnt/mart_sink_datalake/Dim_Cancellation'

-- COMMAND ----------

INSERT OVERWRITE Dim_Cancellation
SELECT 
code 
,description 
FROM  cleansed_sink_db_geekcoders.Cancellation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data from Unity Catalog
-- MAGIC df = spark.table("cleansed_sink_db_geekcoders.Cancellation")
-- MAGIC
-- MAGIC # Write data to ADLS Gen2
-- MAGIC df.write.mode("overwrite").format("delta").save('/mnt/mart_sink_datalake/Dim_Cancellation')
