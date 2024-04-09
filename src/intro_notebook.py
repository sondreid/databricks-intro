# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
import time


spark = SparkSession\
    .builder\
    .appName("dbx_intro")\
    .getOrCreate()

# COMMAND ----------

# Loading Kontali data

df_supply = spark.read.table("kontali.silver_kontali_supply").cache() # Cache in-memory
df_supply_large = spark.read.table("kontali.silver_kontali_supply_large").cache() # Cache in-memory

# Specifying columns to aggregate by
columns_aggregate = ["producing_country", "market_consumption", "date", "species"]

# COMMAND ----------

#  Call on pandas api, to create Pandas dataframes

df_supply_pd = df_supply.toPandas()
df_supply_large_pd = df_supply_large.toPandas()

# COMMAND ----------

# On original kontali supply dataset

df_supply.display()

# COMMAND ----------



df_supply.count()


# COMMAND ----------

start_pd = time.time()
df_supply_pd_grouped = df_supply_pd.groupby(columns_aggregate).agg({"volume_tonne_wfe" : "sum"}).head(20)
time_supply_pd =  time.time() - start_pd
print(time_supply_pd)

# Display aggregation
df_supply_pd_grouped


# COMMAND ----------

start_time = time.time()
df_supply.groupby(columns_aggregate).agg(F.sum("volume_tonne_wfe").alias("volume_tonne_wfe_sum")).show()
time_supply =  time.time() - start_time
print(time_supply)


# COMMAND ----------


# On large supply set

df_supply.count()


# COMMAND ----------


start_pd = time.time()
df_supply_large_pd_grouped = df_supply_large_pd.groupby(columns_aggregate).agg({"volume_tonne_wfe" : "sum"}).head(20)
time_supply_pd =  time.time() - start_pd
print(time_supply_pd)

# Display aggregation
df_supply_large_pd_grouped


# COMMAND ----------


start_time = time.time()
df_supply_large.groupby(columns_aggregate).agg(F.sum("volume_tonne_wfe").alias("volume_tonne_wfe_sum")).show()
time_supply: float =  time.time() - start_time

print(time_supply)

