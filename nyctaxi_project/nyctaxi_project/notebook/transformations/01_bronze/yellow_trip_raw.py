# Databricks notebook source
import os 
from pyspark.sql.functions import current_timestamp ,to_date, max, min

# COMMAND ----------

df =  spark.read.format("parquet").option("header","true").option("inferSchema","true") \
           .load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trip_raw")
