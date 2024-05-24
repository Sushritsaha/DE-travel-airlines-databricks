# Databricks notebook source
# MAGIC %run /Workspace/GeekCoders/Utilities

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Flight")\
    .load("/mnt/raw_sink_datalake/flight/")
)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Flight', True)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df_base = df.selectExpr(
    "to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
    "from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as deptime",
    "from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSDepTime",
    "from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as ArrTime",
    "from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSArrTime",
    "UniqueCarrier",
    "cast(FlightNum as int) as FlightNum",
    "cast(TailNum as int) as TailNum" ,
    "cast(ActualElapsedTime as int) as ActualElapsedTime",
    "cast(CRSElapsedTime as int) as CRSElapsedTime",
    "cast(AirTime as int) as AirTime",
    "cast(ArrDelay as int) as ArrDelay",
    "cast(DepDelay as int) as DepDelay",
    "Origin",
    "Dest",
    "cast(Distance as int) as  Distance",
    "cast(TaxiIn as int) as TaxiIn",
    "cast(TaxiOut as int) as TaxiOut",
    "Cancelled",
    "CancellationCode",
    "cast(Diverted as int) as castDiverted",
    "cast(CarrierDelay as int) as CarrierDelay",
    "cast(WeatherDelay as int) as WeatherDelay" ,
    "cast(NASDelay as int) as NASDelay",
    "cast(SecurityDelay as int) as SecurityDelay",
    "cast(LateAircraftDelay as int) as LateAircraftDelay" ,
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part "
)

# display(df_base)
df_base.writeStream.trigger(once= True)\
    .format("Delta")\
    .option("checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/Flight")\
    .start("/mnt/cleansed_sink_datalake/flight")

# COMMAND ----------

# Read data from Delta table
df = spark.read.format('delta').load('/mnt/cleansed_sink_datalake/flight')
# Prepare schema
schema = prep_schema(df)
# Load data into SQL database catalog
f_delta_cleansed_load_sqlDB(schema, 'flight', 'cleansed_sink_db_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_sink_db_geekcoders.flight;

# COMMAND ----------


