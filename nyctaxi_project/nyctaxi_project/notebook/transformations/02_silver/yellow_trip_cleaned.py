# Databricks notebook source
from pyspark.sql.functions import when ,col,lit, to_date,timestamp_diff

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trip_raw")

# COMMAND ----------

df = df.filter("tpep_pickup_datetime >= '2025-10-01' AND tpep_pickup_datetime < '2026-04-01'")


# COMMAND ----------

df = df.withColumn(
       "VendorID",
        when(col("VendorID") == 1, lit("Creative Mobile Technologies, LLC")) \
       .when(col("VendorID") == 2, lit("Curb Mobility, LLC")) \
       .when(col("VendorID") == 6, lit("Myle Technologies Inc")) \
       .when(col("VendorID") == 7, lit("Helix")) \
       .otherwise(col("VendorID").cast("string"))
)

# COMMAND ----------

df = df.withColumn(
       "RatecodeID",
        when(col("RatecodeID") == 1, lit("Standard rate")) \
       .when(col("RatecodeID") == 2, lit("JFK")) \
       .when(col("RatecodeID") == 3, lit("Newark")) \
       .when(col("RatecodeID") == 4, lit("Nassau or Westchester")) \
       .when(col("RatecodeID") == 5, lit("Negotiated fare")) \
       .when(col("RatecodeID") == 6, lit("Group ride")) \
       .when(col("RatecodeID") == 99, lit("Unknown")) \
       .otherwise(col("RatecodeID").cast("string"))
)

# COMMAND ----------

df = df.withColumn(
       "payment_type",
        when(col("payment_type") == 0, lit("Flex Fare trip")) \
       .when(col("payment_type") == 1, lit("Credit card")) \
       .when(col("payment_type") == 2, lit("Cash")) \
       .when(col("payment_type") == 3, lit("No charge")) \
       .when(col("payment_type") == 4, lit("Dispute")) \
       .when(col("payment_type") == 5, lit("Unknown")) \
       .when(col("payment_type") == 6, lit("Voided trip")) \
       .otherwise(col("payment_type").cast("string"))
).withColumn("trip_duration", timestamp_diff('MINUTE',col("tpep_pickup_datetime"),col("tpep_dropoff_datetime")))

# COMMAND ----------

df =  df.withColumnsRenamed({"VendorID":"vendor",
                            "PULocationID":"pu_location_id",
                            "DOLocationID":"do_location_id",
                            "RatecodeID":"rate_type",
                            "Airport_fee":"airport_fee"})

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trip_cleaned")