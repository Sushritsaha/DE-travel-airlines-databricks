-- Databricks notebook source
use mart_geekcoders

-- COMMAND ----------

delete from cleansed_sink_db_geekcoders.plane where year is null

-- COMMAND ----------

SELECT count(tailid), count(DISTINCT(tailid)) FROM cleansed_sink_db_geekcoders.plane

-- COMMAND ----------

SELECT count(*), count(DISTINCT(*)) FROM cleansed_sink_db_geekcoders.plane
-- To ccheck if any duplicate exists or not

-- COMMAND ----------

DESC cleansed_sink_db_geekcoders.plane

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_PLANE
(
  tailid STRING,
  type STRING,
  manufacturer STRING, 
  issue_date DATE,
  model STRING,
  status STRING,
  aircraft_type STRING,
  engine_type STRING,
  year INT,
  Date_Part DATE
)
USING DELTA
-- LOCATION '/mnt/mart_sink_datalake/DIM_PLANE'
-- AS SELECT * FROM delta.`/mnt/mart_sink_datalake/DIM_PLANE`

-- COMMAND ----------

insert overwrite DIM_PLANE
select
  tailid,
  type,
  manufacturer,
  issue_date,
  model,
  status,
  aircraft_type,
  engine_type,
  year,
  Date_Part
from cleansed_sink_db_geekcoders.plane

-- COMMAND ----------

select * from cleansed_sink_db_geekcoders.plane

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data from Unity Catalog
-- MAGIC df = spark.table("cleansed_sink_db_geekcoders.plane")
-- MAGIC
-- MAGIC # Write data to ADLS Gen2
-- MAGIC df.write.mode("overwrite").format("delta").save('/mnt/mart_sink_datalake/Dim_Plane')

-- COMMAND ----------


