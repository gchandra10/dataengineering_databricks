# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## JSON String Example

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Simple String

# COMMAND ----------

## JSON Example
txtJson = '{"Name":"Rachel Green","Gender":"Female","Age":30,"Profession":"Fashion Designer"}'

# COMMAND ----------

type(txtJson)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Read it using Spark as Dataframe

# COMMAND ----------

df = spark.read.json(sc.parallelize([txtJson]))

df.show(truncate=False)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Another Example (Array of Strings)

# COMMAND ----------

newJson = '[ \
{"Name":"Rachel Green","Gender":"Female","Age":30,"Profession":"Fashion Designer"}, \
{"Name":"Monica Geller","Gender":"Female","Age":30,"Profession":"Chef"} ]'

df = spark.read.json(sc.parallelize([newJson]))

df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query the dataframe

# COMMAND ----------

display(df.select("Name"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## REST API

# COMMAND ----------

## Standard pre-requisite imports

import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Download All Vehicle Makes as JSON

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json

# COMMAND ----------

res = requests.get("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check for the Output type and preview the data

# COMMAND ----------

type(res)

# COMMAND ----------

type(res.text)

# COMMAND ----------

print(res.text)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a Spark Dataframe from JSON String

# COMMAND ----------

make_df1 = spark.read.json(sc.parallelize([res.text]))

# COMMAND ----------

## Check the row count
make_df1.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Preview the Dataframe

# COMMAND ----------

display(make_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Convert the Results Array column into multiple rows using Explode function

# COMMAND ----------

make_df2 = make_df1.select(explode("Results").alias("results"))
display(make_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check the number of rows

# COMMAND ----------

## df1 had one row now its expanded to 10,419 rows

make_df2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Using column.* expands all key value pairs to individual columns

# COMMAND ----------

make_df3 = make_df2.select(col("results.*"))
display(make_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Creating SQL View from the above Dataframe

# COMMAND ----------

make_df3.createOrReplaceTempView("vw_make")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Viewing the same output in SQL Format

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from vw_make;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Same query inside Python, instead of switching between %py and %sql

# COMMAND ----------

## Since spark.sql returns a Dataframe .show() can be used too

spark.sql("select * from vw_make").show(truncate=False)

# COMMAND ----------

## As an alternate option, Explode and * can be combined in a single transformation

make_df3 = make_df1.select(explode("Results")).select(col("col.*"))

# COMMAND ----------

display(make_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now that we have list of Vehicle Makes, lets run next API to get the respective models for given Make.
# MAGIC 
# MAGIC Sample API Call:  https://vpic.nhtsa.dot.gov/api/vehicles/getmodelsformake/AUDI?format=json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Creating a simple PYTHON function to make a API call - Retrieve String - Convert it to JSON and return it back to the caller

# COMMAND ----------

def getModels(make):

    res = None
    # Model API request, get response object back, create dataframe from above schema.
    try:
        res = requests.get("https://vpic.nhtsa.dot.gov/api/vehicles/getmodelsformake/{0}?format=json".format(make))
    except Exception as e:
        return e

    if res != None and res.status_code == 200:
        return json.loads(res.text)

    return None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Schema is needed so the returned output is represented in JSON type. Else return is of type String**

# COMMAND ----------

## Schema

modelSchema = StructType([
  StructField("Results", ArrayType(
    StructType([
      StructField("Make_Name", StringType()),
      StructField("Model_ID", IntegerType()),
      StructField("Model_Name", StringType()),
    ])
  ))
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Register the Python function

# COMMAND ----------

# Register the Python function using UDF so that the python function can be used in Dataframe.

udf_getAllModels = udf(getModels,modelSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the Type of the UDF function

# COMMAND ----------

type(udf_getAllModels)

# COMMAND ----------

## If you are looking for specific Vehicle alone, filter it first. If you are looking for all Vehicles then this is not needed.
make_df3 = make_df3.filter(col("Make_Name")=="JAGUAR")

# COMMAND ----------

display(make_df3)

# COMMAND ----------

# Using the registered Spark Function inside Dataframe

make_model_df1 = make_df3.withColumn("result", udf_getAllModels(col("Make_Name"))) \
                     .withColumnRenamed("Make_ID","makeid") \
                     .withColumnRenamed("Make_Name","makename")

# COMMAND ----------

display(make_model_df1)

# COMMAND ----------

make_model_df1.printSchema()

# COMMAND ----------

make_model_df2 = model_result_df1.select("makeid","makename",explode(col("result.Results")).alias("modelresults"))
make_model_df2.show(truncate=False)

# COMMAND ----------

make_model_df2.count()

# COMMAND ----------

make_model_df3 = make_model_df2.select("makeid",col("modelresults.*"))

# COMMAND ----------

make_model_df4 = make_model_df3.withColumnRenamed("Make_Name","makename") \
                               .withColumnRenamed("Model_ID","modelid") \
                               .withColumnRenamed("Model_Name","modelname")

# COMMAND ----------

display(make_model_df4)

# COMMAND ----------

make_model_df4.write.repartition(rdd.getNumPartitions).save("/FileStore/vehicle_api/make_model/")
