# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Creating simple UDF using Python for Dataframes

# COMMAND ----------

columns = ["p","n","r"]
data = [(1300,"2",2.5), (2032,"5",5.6), (2356,"3",3.9), (6464,"8",1.5)]

df = sc.parallelize(data).toDF(columns)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("n",df.n.cast("int"))
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## build SI function

# COMMAND ----------

def si(p,n,r):
    return((p*n*r)/100)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Register function for DF

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

udf_si_df = udf(si,FloatType())

# COMMAND ----------

df = df.withColumn("si",udf_si_df(col("p"), col("n"), col("r")))

display(df)

# COMMAND ----------

def iseven(n):
    return(not n%2)

# COMMAND ----------

udf_iseven_df = udf(iseven,BooleanType())

# COMMAND ----------



# COMMAND ----------

df = df.withColumn("iseven",udf_iseven_df(col("n")))

display(df)

# COMMAND ----------

udf_iseven_df1 = udf(iseven)

# COMMAND ----------

df = df.withColumn("iseven1",udf_iseven_df1(col("n")))
display(df)

# COMMAND ----------


