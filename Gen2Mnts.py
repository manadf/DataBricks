# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://con1@msa01.blob.core.windows.net",
  mount_point = "/mnt/con1",
  extra_configs = {"fs.azure.account.key.msa01.blob.core.windows.net": "M+L2nCE0sZEbe2kAGlSoK+q37fh9xiLLLynx0gOY2mg5VwfpVf9W1GZYIBAa0aYE76Z2z4O1gKb6Ze+9CnLIHw=="})

# COMMAND ----------

dbutils.fs.unmount("/mnt/con1")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://con2@msa01.blob.core.windows.net",
  mount_point = "/mnt/con2",
  extra_configs = {"fs.azure.account.key.msa01.blob.core.windows.net": "M+L2nCE0sZEbe2kAGlSoK+q37fh9xiLLLynx0gOY2mg5VwfpVf9W1GZYIBAa0aYE76Z2z4O1gKb6Ze+9CnLIHw=="})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://con1@msa02.blob.core.windows.net",
  mount_point = "/mnt/msa02con1",
  extra_configs = {"fs.azure.account.key.msa02.blob.core.windows.net": "iXC5d+BdRzUYEQuKvE1XyTJhpeD4ArRW4ixqicTU3EZVyo85hLCuoN/4TbjAecE7V9zlCVZTaIoDqoQ3FBhf6w=="})

# COMMAND ----------

