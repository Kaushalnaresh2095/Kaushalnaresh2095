# Databricks notebook source
import urllib.request
import shutil
import os 


# COMMAND ----------

date = ["2026-03" , "2026-02","2026-01" ,"2025-12","2025-11","2025-10"]

for c in date:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{c}.parquet"
    response = urllib.request.urlopen(url)
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{c}"
    os.makedirs(dir_path, exist_ok=True)
    local_path = os.path.join(dir_path,f"yellow_tripdata_{c}.parquet")
    with open(local_path , 'wb') as f :
        shutil.copyfileobj(response,f)
        print(f"file downloaded : {local_path}")

    
