# Databricks notebook source
# MAGIC %run /Workspace/GeekCoders/Utilities

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_sink_datalake/')

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")\
    .load("/mnt/raw_sink_datalake/UNIQUE_CARRIERS/")
)

# COMMAND ----------

# dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport/',True)

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
# display(df_base)
df_base.writeStream.trigger(once= True)\
    .format("Delta")\
    .option("checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS")\
    .start("/mnt/cleansed_sink_datalake/unique_carriers")

# COMMAND ----------

# Read data from Delta table
df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/unique_carriers')
# Prepare schema
schema = prep_schema(df)
# Load data into SQL database catalog
f_delta_cleansed_load_sqlDB(schema, 'unique_carriers', 'cleansed_sink_db_geekcoders')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_sink_db_geekcoders.unique_carriers;

# COMMAND ----------


