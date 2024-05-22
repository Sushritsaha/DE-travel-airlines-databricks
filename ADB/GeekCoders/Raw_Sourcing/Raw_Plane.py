# Databricks notebook source
pip install tabula-py

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import tabula
from datetime import date

dbutils.fs.mkdirs(f"/mnt/raw_sink_datalake/PLANE/Date_Part={str(date.today())}/")

# COMMAND ----------

tabula.convert_into('/dbfs/mnt/source_blob/PLANE.pdf',f'/dbfs/mnt/raw_sink_datalake/PLANE/Date_Part={str(date.today())}/PLANE.csv',output_format = 'csv', pages = 'all')

# COMMAND ----------

dbutils.fs.ls('/mnt/')

# COMMAND ----------

dbutils.fs.ls('/mnt/source_blob/')

# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1] == 'pdf')]
print(list_files)

# COMMAND ----------

def f_source_pdf_datalake(source_path, sink_path, file_name, output_format, pages):
    try:
        dbutils.fs.mkdirs(f"{sink_path}{file_name.split('.')[0]}/Date_Part={str(date.today())}/")
        tabula.convert_into(f"{source_path}{file_name}", f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={str(date.today())}/{file_name.split('.')[0]}.{output_format}", output_format=output_format, pages=pages)
    except Exception as err:
        print("Error Occurred:", str(err))

# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1] == 'pdf')]
print(list_files)
for i in list_files:
    f_source_pdf_datalake('/dbfs/mnt/source_blob/', 'mnt/raw_sink_datalake/',i[0], 'csv', 'all')

# COMMAND ----------


