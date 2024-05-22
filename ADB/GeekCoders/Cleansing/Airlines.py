# Databricks notebook source
# MAGIC %run /Workspace/GeekCoders/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Airport")\
    .load("/mnt/raw_sink_datalake/Airport/")

# COMMAND ----------

from pyspark.sql.functions import explode

df = spark.read.json("/mnt/raw_sink_datalake/airlines/")
df1 = df.select(explode("response"),"Date_Part")
df_final = df1.select("col.*","Date_Part")

# COMMAND ----------

df_final.write.format('delta').mode("overwrite").save('/mnt/cleansed_sink_datalake/airline')

# COMMAND ----------

# Read data from Delta table
df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/airline').limit(1) 
# Prepare schema
schema = prep_schema(df)
# Load data into SQL database catalog
f_delta_cleansed_load_sqlDB(schema, 'airline', 'cleansed_sink_db_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_sink_db_geekcoders.airline;

# COMMAND ----------


