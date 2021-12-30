# Databricks notebook source
from pyspark.sql.functions import*
from datetime import datetime
from databricks import*
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("csv").options(header='true', inferSchema='true').load("/mnt/con1/Bronze/SalesOrderHeader.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("FreightTax", col("Freight")*0.13)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS SalesAW

# COMMAND ----------

df.write.format("delta").saveAsTable("SalesAW.SOHeader")

# COMMAND ----------

df.write.format("parquet").save("/mnt/con1/Delta/SOH")

# COMMAND ----------

df1 = spark.read.format("parquet").load("/mnt/con1/Delta/SOH")

# COMMAND ----------

df1.createOrReplaceTempView("SOH")

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from SOH

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE SalesAW.Events (
# MAGIC   date DATE,
# MAGIC   eventId STRING,
# MAGIC   eventType STRING,
# MAGIC   data STRING)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesaw.soheader where SubTotal > 100

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/mnt/con1/Bronze/Records.json")

# COMMAND ----------

df.display()

# COMMAND ----------

Records = df.select(explode("records").alias("Records"))

# COMMAND ----------

Records.display()

# COMMAND ----------

FlatRecords = Records.select(col("Records.name").alias("Name"),
                             col("Records.job").alias("Job"),
                             col("Records.city").alias("City"),
                             explode("Records.cars").alias("Cars")
                            ).show()

# COMMAND ----------

FlatRecords.display()

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/mnt/con1/Bronze/Medical.json")

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

FlattenMedical = flatten(df)

# COMMAND ----------

display(FlattenMedical)

# COMMAND ----------

FlattenMedical.display()

# COMMAND ----------

FlattenMedical.write.format("delta").save("/mnt/con1/Delta/Medical")

# COMMAND ----------

Med = spark.read.format("delta").load("/mnt/con1/Delta/Medical")

# COMMAND ----------

display(Med)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/con1/Delta/SOH`

# COMMAND ----------

