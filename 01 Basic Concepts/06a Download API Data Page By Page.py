# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Downloading from REST API - Page by Page

# COMMAND ----------

# Start, End, Skip. 
# Note: End Number is exclusive, meaning it will not be included.
display(spark.range(1,22,3))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers?format=json&page=2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a consequtive number dataframe using Range function

# COMMAND ----------

df_page = spark.range(1,225)

df_page = df_page.withColumnRenamed("id","page_no")

# COMMAND ----------

display(df_page)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Importing Pre-Req libraries

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import *
import requests
import json

# COMMAND ----------

# df_page1 = df_page.withColumn("page_no",col("id") + 1) \
#                    .drop("id")
# display(df_page1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Python Function to download Manufacturers data based on page number

# COMMAND ----------

def getData(page_no):

    res = None
    # Model API request, get response object back, create dataframe from above schema.
    try:
        res = requests.get(f"https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers?format=json&page={page_no}")
    except Exception as e:
        return e

    if res != None and res.status_code == 200:
        return json.loads(res.text)

    return None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define your desired schema

# COMMAND ----------

manufacturersSchema = StructType([
    StructField("Results", ArrayType(
        StructType([
            StructField("Mfr_CommonName", StringType()),
            StructField("Country", IntegerType()),
            StructField("VehicleTypes",ArrayType(
                StructType([
                    StructField("IsPrimary",BooleanType()),
                    StructField("Name",StringType())
                ])
            )),
            StructField("Mfr_ID", IntegerType()),
            StructField("Mfr_Name", StringType())
        ])
      ))
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Register your Python function

# COMMAND ----------

udf_getdata_df = udf(getData,manufacturersSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Pass the page number and download data

# COMMAND ----------

df_manufacturers1 = df_page.withColumn("result",udf_getdata_df(col("page_no")))

# COMMAND ----------

display(df_manufacturers1)

# COMMAND ----------

df_manufacturers1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explode the single Results row to multiple rows

# COMMAND ----------

df_manufacturers2 = df_manufacturers1.select("page_no",explode(col("result.Results"))).select("page_no",col("col.*"))

# COMMAND ----------

df_manufacturers2.count()

# COMMAND ----------

display(df_manufacturers2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explode the next level VehicleTypes

# COMMAND ----------

df_manufacturers3 = df_manufacturers2.select("page_no","mfr_commonname",explode(col("VehicleTypes")),"Mfr_ID","Mfr_Name").select("page_no","mfr_commonname","mfr_id","mfr_name",col("col.*"))

# COMMAND ----------

df_manufacturers3.show(4)

# COMMAND ----------

df_manufacturers4 = df_manufacturers3.withColumnRenamed("mfr_commonname","manufacturer_commonname") \
                                      .withColumnRenamed("IsPrimary","is_primary") \
                                      .withColumnRenamed("Name","vehicle_type")

# COMMAND ----------

df_manufacturers4.count()

# COMMAND ----------

display(df_manufacturers4)

# COMMAND ----------


