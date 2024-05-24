# Databricks notebook source
# MAGIC %run /Workspace/GeekCoders/Utilities

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/Airport')

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Airport")\
    .load("/mnt/raw_sink_datalake/Airport/")

# COMMAND ----------

# dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport/',True)

# COMMAND ----------

df_base = df.selectExpr(
    "Code as code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_Part, 'yyyy-MM-dd') as Date_Part"
)
# display(df_base)
df_base.writeStream.trigger(once= True)\
    .format("Delta")\
    .option("checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/Airport")\
    .start("/mnt/cleansed_sink_datalake/airport")

# COMMAND ----------

# df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/plane')
# schema = prep_schema(df)
# f_delta_cleansed_load_sqlDB(schema,'plane','/mnt/cleansed_sink_datalake/plane','cleansed_sink_db_geekcoders')

# COMMAND ----------

# Read data from Delta table
df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/airport')
# Prepare schema
schema = prep_schema(df)
# Load data into SQL database catalog
f_delta_cleansed_load_sqlDB(schema, 'airport', 'cleansed_sink_db_geekcoders')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_sink_db_geekcoders.airport;

# COMMAND ----------


