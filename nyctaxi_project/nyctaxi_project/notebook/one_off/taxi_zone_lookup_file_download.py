# Databricks notebook source
import urllib.request
import shutil
import os 
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
response = urllib.request.urlopen(url)
filename = os.path.basename(url)
dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
os.makedirs(dir_path,exist_ok=True)
print(os.path.exists(dir_path))
local_path = os.path.join(dir_path,filename)
with open(local_path , 'wb') as f:
    shutil.copyfileobj(response,f)
    print(f"file download : {local_path}")
