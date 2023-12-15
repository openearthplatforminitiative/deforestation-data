# Databricks notebook source
overwrite_files = False


import requests
import os
import json
import tempfile
import shutil

from rio_cogeo import cog_translate, cog_profiles


with open("config.json") as f:
    config = json.load(f)


# base_url = config["GFC_BASE_URL"]
# gfc_url_template =  "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10/Hansen_GFC-2022-v1.10_$product_name_$area_name.tif"
# download_base_path = config["GFC_DOWNLOAD_BASE_PATH"]
# products = config["GFC_REQUIRED_PRODUCTS"]
# areas = config["GFC_TILE_AREAS"]


def save_as_cog(data: bytes, path: str, profile: str = "deflate") -> None:
    """
    Translate bytes from a GeoTIFF to COG and save the result.
    A tempfile is created because cog_translate seems to have problems
    reading from and writing to S3.
    """
    with tempfile.NamedTemporaryFile() as tmpfile:
        tmpfile.write(data)
        cog_translate(tmpfile.name, tmpfile.name, cog_profiles[profile])
        os.makedirs(os.path.dirname(path), exist_ok=True)
        shutil.copyfile(tmpfile.name, path)


def download_gfc_tile(gfc_root: str, product: str, area: str):
    output_file_path = os.path.join(gfc_root, product, f"{area}.tif")
    url = f"https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10/Hansen_GFC-2022-v1.10_{product}_{area}.tif"
    r = requests.get(url)
    save_as_cog(r.content, output_file_path, profile="deflate")


# for product_name in products:
#     product_path = os.path.join(download_base_path, product_name)
#     os.makedirs(product_path, exist_ok=True)
#     for area_name in areas:
#         output_path = os.path.join(product_path, f"{area_name}.tif")
#         if os.path.isfile(output_path) and not overwrite_files:
#             continue

#         print(f"{product_name}_{area_name}")
#         url = base_url + f"Hansen_GFC-2022-v1.10_{product_name}_{area_name}.tif"
#         r = requests.get(url)
#         save_as_cog(r.content, output_path, profile="deflate")
