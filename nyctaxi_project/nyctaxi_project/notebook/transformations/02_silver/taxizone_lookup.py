# Databricks notebook source
from pyspark.sql.functions import current_timestamp ,lit , col
from pyspark.sql.types import TimestampType , StringType , StructType , StructField , IntegerType , DoubleType , LongType

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

df =df.select(
    df.LocationID.cast(IntegerType()).alias("location_id") , 
    df.Borough.alias("borough"),
    df.Zone.alias("zone"),
    df.service_zone,
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
    )


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")