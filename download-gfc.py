# Databricks notebook source
dbutils.widgets.dropdown("overwrite_files", "False", ["False", "True"])
overwrite_files = dbutils.widgets.get("overwrite_files") == "True"

# COMMAND ----------

import requests
import os
import json
from time import sleep

# COMMAND ----------

with open("config.json") as f:
    config = json.load(f)

# COMMAND ----------

base_url = config["GFC_BASE_URL"]
download_base_path = config["GFC_DOWNLOAD_BASE_PATH"]
products = config["GFC_REQUIRED_PRODUCTS"]
areas = config["GFC_TILE_AREAS"]

# COMMAND ----------

for product_name in products:
    download_product_path = os.path.join(download_base_path, product_name)
    os.makedirs(download_product_path, exist_ok=True)
    for area_name in areas:
        download_path = os.path.join(download_product_path, f"{area_name}.tif")
        if os.path.isfile(download_path) and not overwrite_files:
            continue
        url = base_url + f"Hansen_GFC-2022-v1.10_{product_name}_{area_name}.tif"
        r = requests.get(url)
        with open(download_path, 'wb') as outfile:
            outfile.write(r.content)
        print(download_path)
        sleep(0.5)
