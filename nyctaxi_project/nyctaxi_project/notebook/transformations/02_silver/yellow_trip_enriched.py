# Databricks notebook source
from pyspark.sql.functions import col 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, LongType

# COMMAND ----------

df_trips = spark.read.table("nyctaxi.02_silver.yellow_trip_cleaned")
df_zone  = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

df_join =df_trips.join(df_zone, df_trips.pu_location_id == df_zone.location_id , "left") \
        .select("vendor",
                 "tpep_pickup_datetime",
                  "tpep_dropoff_datetime",
                  col("trip_duration").alias("trip_duration_mins"),
                   "passenger_count",
                    "trip_distance",
                    "rate_type",
                    "store_and_fwd_flag",
                    df_zone.borough.alias("pu_borough"),
                    df_zone.zone.alias("pu_zone"),
                    df_zone.location_id ,
                    "pu_location_id",
                    "do_location_id",
                    "payment_type",
                    "fare_amount",
                    "extra",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "improvement_surcharge",
                    "total_amount",
                    "congestion_surcharge",
                    "airport_fee",
                    "cbd_congestion_fee",
                    "processed_timestamp" )

# COMMAND ----------

df_join.join(df_zone , df_join.do_location_id == df_zone.location_id , "left") \
        .select("vendor",
                 "tpep_pickup_datetime",
                  "tpep_dropoff_datetime",
                  "trip_duration_mins",
                   "passenger_count",
                    "trip_distance",
                    "rate_type",
                    "store_and_fwd_flag",
                    "pu_borough",
                    df_zone.borough.alias("du_borough"),
                    "pu_zone",
                    df_zone.zone.alias("du_zone"),
                    "payment_type",
                    "fare_amount",
                    "extra",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "improvement_surcharge",
                    "total_amount",
                    "congestion_surcharge",
                    "airport_fee",
                    "cbd_congestion_fee",
                    "processed_timestamp" ) \
        .write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_enriched")
                    