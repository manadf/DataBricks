# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

# Create Widgets
# "JsonFile" stores the input JSON file location
# "TableName" stores the target flattened table name and location

dbutils.widgets.text("JsonFile","")
dbutils.widgets.text("TableName","")

# COMMAND ----------

# Pass those widget values to the variables "JFile" and "Table"

JFile = dbutils.widgets.get("JsonFile")
Table = dbutils.widgets.get("TableName")

# COMMAND ----------

# Print both variables to see the values stored

print(JFile)
print(Table)

# COMMAND ----------

# Read JSON file by passing "JFile"

df = spark.read.option("multiline", "true").json(JFile)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
#Flatten array of structs and structs
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema  
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
   
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
   
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
   
      # recompute remaining Complex Fields in Schema      
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

# COMMAND ----------

df_flatten = flatten(df)

# COMMAND ----------

df_flatten.display()

# COMMAND ----------

# Save the flattened file in "delta" format by passing "Table"

df_flatten.write.format("delta").mode("overwrite").save(Table)

# COMMAND ----------

# Load the exported delta file to run queries

df1 = spark.read.format("delta").load("/mnt/con1/Silver/FlatData/Medical")

# COMMAND ----------

#  Creates or replaces a local temporary view in DataFrame

df1.createOrReplaceTempView("Med")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from Med limit 10

# COMMAND ----------

