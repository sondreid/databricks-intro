# Databricks notebook source

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
# COMMAND ----------
%sql
SELECT * FROM dev.kontali.silver_kontali_harvest



# COMMAND ----------
%sql
-- Aggregating by species
SELECT sum(volume_tonne_wfe) as sum_volume, species FROM dev.kontali.silver_kontali_harvest
GROUP BY species


# COMMAND ----------
spark = SparkSession\
    .builder\
    .appName("dbx_basics")\
    .getOrCreate()


# COMMAND ----------

df_harvest = spark.sql("SELECT * FROM dev.kontali.silver_kontali_harvest")
df_harvest.display()



# COMMAND ----------

df_supply = spark.read.table("kontali.silver_kontali_supply")
df_supply.display()


# COMMAND ----------
# Aggregating by species
df_harvest = spark.sql("SELECT * FROM dev.kontali.silver_kontali_harvest")
df_harvest.groupBy("species").agg(F.sum("volume_tonne_wfe").alias("sum_volume")).display()
