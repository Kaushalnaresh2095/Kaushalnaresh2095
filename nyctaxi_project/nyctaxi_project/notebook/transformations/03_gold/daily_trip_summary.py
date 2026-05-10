# Databricks notebook source
from pyspark.sql.functions import col , min , max , avg , count, sum , when , date_format, datediff, lit, to_timestamp, to_date, to_timestamp, unix_timestamp, datediff,current_timestamp,round

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

df_gold = df.groupBy(to_date(col("tpep_pickup_datetime") , "YYYY-MM-DD").alias("pickup_date")) \
            .agg(count("*").alias("total_trips") 
            , avg(col("passenger_count")).alias("avg_passenger_per_trip")
            , avg(col("trip_distance")).alias("avg_distance_per_trip")
            , avg(col("fare_amount")).alias("avg_fare_per_trip")
            , max(col("fare_amount")).alias("max_fare")
            , min(col("fare_amount")).alias("min_fare")
            , sum(col("total_amount")).alias("total_revenue")
        )
               
        

# COMMAND ----------

df_final = df_gold.select("pickup_date","total_trips",round("avg_passenger_per_trip",2).alias("avg_passenger_per_trip"),round("avg_distance_per_trip",2).alias("avg_distance_per_trip"),round("avg_fare_per_trip",2).alias("avg_fare_per_trip"),"max_fare","min_fare",round("total_revenue",2).alias("total_revenue"))

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")