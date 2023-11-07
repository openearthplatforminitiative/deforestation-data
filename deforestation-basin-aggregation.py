# Databricks notebook source
# MAGIC %md
# MAGIC # Non-parallelized implementation (runtime ~1h)
# MAGIC This is a naive implementation for aggregating deforestation data on river sub-basin level running on a single node.

# COMMAND ----------

from utils import *

# COMMAND ----------

import geopandas as gpd
import pandas as pd
import numpy as np
import xarray as xr
import rioxarray
import rasterio
import rio_cogeo
from shapely.geometry import Polygon
import json
import os
from tqdm import tqdm

# COMMAND ----------

with open("config.json") as f:
    config = json.load(f)
    
roi = config["GFC_ROI_CENTRAL_AFRICA"]
basin_path = config["BASIN_PATH"]
basin_level = config["GFC_BASIN_AGGREGATION_LEVEL"]
gfc_tile_areas = config["GFC_TILE_AREAS"]
gfc_path = config["GFC_DOWNLOAD_BASE_PATH"]
gfc_treecover = config["GFC_TREECOVER_THRESHOLD"]
chunk_size = config["GFC_CHUNK_SIZE"]
basins_output_path = config["GFC_BASINS_OUTPUT_PATH"]
lossyear_output_path = config["GFC_LOSSYEAR_OUTPUT_PATH"]

# COMMAND ----------

roi_bbox_polygon = Polygon([
    (roi["lon_min"], roi["lat_min"]), 
    (roi["lon_max"], roi["lat_min"]), 
    (roi["lon_max"], roi["lat_max"]), 
    (roi["lon_min"], roi["lat_max"])
])

roi_bbox_gdf = gpd.GeoDataFrame(geometry=[roi_bbox_polygon], crs="EPSG:4326")

# COMMAND ----------

basin_file_name = f"hybas_af_lev{basin_level:02}_v1c.shp"
basin_file_path = os.path.join(basin_path, basin_file_name)

# Load basin data and select only ploygons within ROI 
basins = gpd.read_file(basin_file_path)
basins = basins.sjoin(roi_bbox_gdf, predicate="within").drop(columns="index_right")
basins.info()

# COMMAND ----------

lossyear_df = pd.DataFrame({"id": basins["HYBAS_ID"]})
years = pd.DataFrame({"year": range(1, 23)})
lossyear_df = lossyear_df.merge(years, how="cross")
lossyear_df["total"] = 0.0
lossyear_df = lossyear_df.set_index(["id", "year"])

# COMMAND ----------

# WARNING: long running code (~1h)
for tile_area in gfc_tile_areas:
    print(tile_area)
    lossyear_path = os.path.join(gfc_path, f"lossyear/{tile_area}.tif")
    lossyear_data = open_gfc_tile(lossyear_path, roi)
    h, w = lossyear_data.shape
    # The GFC raster data is is organized in blocks of size (512, 512).
    # For optimal performance the data should be read in chunks where 
    # the size is a multiple of the block size of the raster file.
    chunk_size = 2*2048
    for i in tqdm(range(h // chunk_size)):
        chunk_slice = {"y": slice(i*chunk_size, (i+1)*chunk_size)}
        lossyear_chunk = lossyear_data.isel(y=slice(i*chunk_size, (i+1)*chunk_size))
        lossyear_points = non_zero_to_df(lossyear_chunk, "lossyear")
        lossyear_count = count_loss_per_basin_year(lossyear_points, basins)
        lossyear_df.loc[lossyear_count.index, "total"] += lossyear_count["lossarea"]
    lossyear_data.close()

# COMMAND ----------

basin_idx = lossyear_df.index.get_level_values(0) # Get basin indices from lossyear_df
basin_area = basins.set_index("HYBAS_ID").loc[basin_idx, "SUB_AREA"].values # Get area per basin index
lossyear_df["relative"] = lossyear_df["total"] / basin_area # Divide 

# COMMAND ----------

lossyear_df = lossyear_df.reset_index()
lossyear_df["year"] = lossyear_df["year"] + 2000
lossyear_df.to_parquet(lossyear_output_path, index=False)

# COMMAND ----------

basins = basins.rename(columns={
    "HYBAS_ID": "id",
    "NEXT_DOWN": "downstream",
    "SUB_AREA": "basin_area",
    "UP_AREA": "upstream_area"
})

basins[["id", "downstream", "basin_area", "upstream_area", "geometry"]].to_parquet(basins_output_path, index=False)
