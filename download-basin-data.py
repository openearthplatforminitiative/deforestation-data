# Databricks notebook source
import requests
import os
import zipfile

# COMMAND ----------

source_url = "https://data.hydrosheds.org/file/hydrobasins/standard/hybas_af_lev01-12_v1c.zip"
filename = source_url.split("/")[-1]
download_path = f"/tmp/{filename}"
extract_path = "/dbfs/mnt/openepi-storage/hydrobasin-africa/"

# COMMAND ----------

if not os.path.exists(extract_path):
    os.makedirs(extract_path)

# COMMAND ----------

r = requests.get(source_url)
with open(download_path, 'wb') as outfile:
    outfile.write(r.content)

# COMMAND ----------

with zipfile.ZipFile(download_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)
