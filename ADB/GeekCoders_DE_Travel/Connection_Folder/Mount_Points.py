# Databricks notebook source
# container_name = dbutils.secrets.get(scope = "geekcoders-secret", key = "container-name")
container_name = "source"
storage_account_name = "storageaccountsourcedev"
sas_token = dbutils.secrets.get(scope = "geekcoders-secret", key = "sas-token") 

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = "/mnt/source_blob",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)

# COMMAND ----------

# container_name = dbutils.secrets.get(scope = "geekcoders-secret", key = "container-name")
container_name = "raw"
storage_account_name = "geekcodersdatalakeg2dev"
sas_token_raw_sink = dbutils.secrets.get(scope = "geekcoders-secret", key = "sas-token-raw-sink")


dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = "/mnt/raw_sink_datalake",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token_raw_sink}
)

# COMMAND ----------

# container_name = dbutils.secrets.get(scope = "geekcoders-secret", key = "container-name")
container_name = "cleansed"
storage_account_name = "geekcodersdatalakeg2dev"
sas_token_cleansed = dbutils.secrets.get(scope = "geekcoders-secret", key = "sas-token-cleansed")


dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = "/mnt/cleansed_sink_datalake",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token_cleansed}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/cleansed_sink_datalake')

# COMMAND ----------

# dbutils.fs.unmount('/mnt/cleansed_sink_datalake')

# COMMAND ----------

# container_name = dbutils.secrets.get(scope = "geekcoders-secret", key = "container-name")
container_name = "mart"
storage_account_name = "geekcodersdatalakeg2dev"
sas_token_sink_mart = dbutils.secrets.get(scope = "geekcoders-secret", key = "sas-token-sink-mart")


dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = "/mnt/mart_sink_datalake",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token_sink_mart}
)

# COMMAND ----------


