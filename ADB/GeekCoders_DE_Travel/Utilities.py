# Databricks notebook source
def prep_schema(df):
    try:
        # df = spark.read.format('delta').load(f'{path}').limit(1)
        schema = ""
        for i in df.dtypes:
            schema = schema + i[0] + " " + i[1] + ","
        return (schema[0:-1])
    except Exception as err:
        print("Error Occoured :", str(err))

# COMMAND ----------

# def f_delta_cleansed_load_sqlDB(schema,table_name,location,database):
#   try:
#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {database}.{table_name}
#     ({schema})
#     using delta
#     location '{location}'
#     """)
#   except Exception as err:
#     print("Error Occurred:" , str(err))

def f_delta_cleansed_load_sqlDB(schema, table_name, database):
    try:
        spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {database}
        """)
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        USING delta
        AS SELECT * FROM delta.`/mnt/cleansed_sink_datalake/{table_name}`
        """)
    except Exception as err:
        print("Error Occurred:", str(err))


# COMMAND ----------

def f_count_check(database, operation_type, table_name, number_diff):
    spark.sql(f"""DESC HISTORY {database}.{table_name}""").createOrReplaceTempView("Table_count")
    count_current = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where trim(lower(operation))=lower('{operation_type}'))""")

    if(count_current.first() is None):
        final_count_current = 0
    else:
        final_count_current = int(count_current.first().numOutputRows)
    count_previous = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where lower(trim(operation))=lower('{operation_type}') order by version desc limit 1)""")
   
    if(count_previous.first() is None):
        final_count_previous = 0
    else:
        final_count_previous = int(count_previous.first().numOutputRows)
    if((final_count_current - final_count_previous) > number_diff):
        raise Exception("Difference is huge in ", table_name)
    else:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Monthly Trend of #DELAY flights(Both Arrival and Departture)
# MAGIC 2. No_FLIGHTS cancelled category wise
# MAGIC 3. No_Flights in different Airlines
# MAGIC 4. No_Airlines whose flight delayed
# MAGIC 5. Plane data like Build Year,Aircraft Name, Engine_name
