-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_UniqueCarrier (
  code STRING,
  description STRING
) USING DELTA 
-- LOCATION '/mnt/mart_sink_datalake/Dim_UniqueCarrier'

-- COMMAND ----------

INSERT OVERWRITE Dim_UniqueCarrier
SELECT 
code 
,description 

FROM  cleansed_sink_db_geekcoders.Unique_Carriers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data from Unity Catalog
-- MAGIC df = spark.table("cleansed_sink_db_geekcoders.Unique_Carriers")
-- MAGIC
-- MAGIC # Write data to ADLS Gen2
-- MAGIC df.write.mode("overwrite").format("delta").save('/mnt/mart_sink_datalake/Dim_UniqueCarrier')

-- COMMAND ----------


