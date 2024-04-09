# Databricks notebook source


from pyspark.sql import SparkSession
from functools import reduce
import pyspark.sql.functions as F


# COMMAND ----------


# Enlargens Kontali silver supply dataset

spark = SparkSession\
    .builder\
    .appName("replicate_data")\
    .getOrCreate()



def replicate_df(df, n):
    """
    Make a dummy variable as an array and explode it n times
    """
    return df.withColumn("dummy", F.explode(F.array([F.lit(x) for x in range(n)]))).drop("dummy")


# COMMAND ----------

df_supply = spark.sql("SELECT * FROM dev.kontali.silver_kontali_supply")

df_supply = spark.read.table("kontali.silver_kontali_supply")


df_supply_large = replicate_df(df_supply, 5000)

df_supply_large.write.saveAsTable("kontali.silver_kontali_supply_large")