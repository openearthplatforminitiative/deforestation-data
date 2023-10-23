# Databricks notebook source
import requests
import os
from time import sleep

# COMMAND ----------

base_url = "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10/"
filename_template = "Hansen_GFC-2022-v1.10_{product}_{area}.tif"
products = ["treecover2000", "lossyear"]
areas = [
  "10N_020W", "10N_010W", "10N_000E", "10N_010E", "10N_020E", "10N_030E", "10N_040E", 
  "00N_010E", "00N_020E", "00N_030E", "00N_040E"
]

# COMMAND ----------

download_base_path = "/dbfs/mnt/openepi-storage/global-forest-change/"
if not os.path.exists(download_base_path):
    os.makedirs(download_base_path)

# COMMAND ----------

for product_name in products:
    for area_name in areas:
        filename = filename_template.format(product=product_name, area=area_name)
        url = base_url + filename
        download_path = download_base_path + filename
        r = requests.get(url)
        with open(download_path, 'wb') as outfile:
            outfile.write(r.content)
        print(download_path)
        sleep(0.5)

# COMMAND ----------

for product_name in products:
    # os.makedirs(download_base_path + product_name)
    # print(download_base_path + product_name)
    for area_name in areas:
        filename = filename_template.format(product=product_name, area=area_name)
        download_path = download_base_path + filename
        target_path = os.path.join(download_base_path, product_name, filename)
        print(target_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/openepi-storage/global-forest-change/
