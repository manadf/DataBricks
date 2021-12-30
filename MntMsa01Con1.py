# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://con1@msa01.blob.core.windows.net",
  mount_point = "/mnt/con1",
  extra_configs = {"fs.azure.account.key.msa01.blob.core.windows.net": "M+L2nCE0sZEbe2kAGlSoK+q37fh9xiLLLynx0gOY2mg5VwfpVf9W1GZYIBAa0aYE76Z2z4O1gKb6Ze+9CnLIHw=="})

# COMMAND ----------

dbutils.fs.unmount("/mnt/con1")

# COMMAND ----------


