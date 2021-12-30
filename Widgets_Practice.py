# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("dropdown")

# COMMAND ----------

dbutils.widgets.help("combobox")

# COMMAND ----------

dbutils.widgets.help("multiselect")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Default Value "Null" has to be a part of the options
# Doesn't accept values outside the options give below

dbutils.widgets.dropdown("Model", "Null", ("Null","Skyline", "GTR", "370z", "Miata", "S2000"))

# COMMAND ----------

# Default Value "Null" DOES NOT have to be a part of the options
# Accepts values outside the options give below

dbutils.widgets.combobox("Model", "Null", ("Skyline", "GTR", "370z", "Miata", "S2000"))

# COMMAND ----------

# Default Value "Null" has to be a part of the options
# Allows to select all the options simultaneously

dbutils.widgets.multiselect("Model", "Null", ("Null","Skyline", "GTR", "370z", "Miata", "S2000"))

# COMMAND ----------

