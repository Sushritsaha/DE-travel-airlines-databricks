# Databricks notebook source
# MAGIC %run /Workspace/GeekCoders/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/PLANE")\
    .load("/mnt/raw_sink_datalake/PLANE/")

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/PLANE/',True)

# COMMAND ----------

dbutils.fs.rm('/mnt/cleansed_sink_datalake/plane',True)

# COMMAND ----------

df_base = df.selectExpr("tailnum as tailid", "type", "manufacturer", "to_date(issue_date) as issue_date","model" ,"status","aircraft_type","engine_type","cast('year' as int)as year", "to_date(Date_Part) as Date_Part")
df_base.writeStream.trigger(once= True)\
    .format("Delta")\
    .option("checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/PLANE")\
    .start("/mnt/cleansed_sink_datalake/plane")

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/plane')

# COMMAND ----------

# df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/plane')
# schema = prep_schema(df)
# f_delta_cleansed_load_sqlDB(schema,'plane','/mnt/cleansed_sink_datalake/plane','cleansed_sink_db_geekcoders')

# COMMAND ----------

# Read data from Delta table
df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/plane')

# Prepare schema
schema = prep_schema(df)

# Load data into SQL database catalog
f_delta_cleansed_load_sqlDB(schema, 'plane', 'cleansed_sink_db_geekcoders')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_sink_db_geekcoders.plane;

# COMMAND ----------


