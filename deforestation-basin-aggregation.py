# Databricks notebook source
# MAGIC %md
# MAGIC # Non-parallelized implementation (runtime ~1h)
# MAGIC This is a naive implementation for aggregating deforestation data on river sub-basin level running on a single node.

# COMMAND ----------

import geopandas as gpd
import pandas as pd
import xarray as xr
import rioxarray
import rasterio
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
output_path = config["GFC_BASINS_AGGREGATION_OUTPUT_PATH"]

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
basins = gpd.read_file(basin_file_path).set_index("HYBAS_ID")
basins = basins.sjoin(roi_bbox_gdf, predicate="within").drop(columns="index_right")
basins.info()

# COMMAND ----------

# Initialize lossyear columns to zero
lossyear_cols = [f"lossyear{y:02}" for y in range(1, 23)]
for colname in lossyear_cols:
    basins[colname] = 0

# COMMAND ----------

def get_gfc_path(product: str, area: str, spark_api: bool=True) -> str:
    prefix = "dbfs:/" if spark_api else "/dbfs/"
    gfc_root = "mnt/openepi-storage/global-forest-change/" 
    gfc_path = os.path.join(prefix, gfc_root, product, f"{area}.tif")
    return gfc_path

def open_GFC_tile(src_path: str, roi: dict[str, float]) -> xr.DataArray:
    """Load GFC tile, squeeze and slice to ROI."""
    data = rioxarray.open_rasterio(src_path)
    data = data.squeeze() # data is single band, use squeeze to drop band dimension
    data = data.sel(
        x=slice(roi["lon_min"], roi["lon_max"]), 
        y=slice(roi["lat_max"], roi["lat_min"])
    )
    return data

def non_zero_to_df(data: xr.DataArray, name: str="non_zero") -> gpd.GeoDataFrame:
    """Take all non-zero cells in a DataArray and convert them to points in a GeoDataFrame."""
    df = data.to_dataframe(name=name).reset_index()
    df = df[df[name] > 0]
    gdf = gpd.GeoDataFrame(
        df[name], 
        geometry=gpd.points_from_xy(df.x, df.y), 
        crs=data.spatial_ref.crs_wkt
    )
    return gdf

def count_loss_per_basin_year(lossyear_points: gpd.GeoDataFrame, basins: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Join lossyear point with basin polygons and pivot over basins and lossyear.
    For each basin polygon and for each year from 2001 to 2022, count the 
    number of pixels within the polygon with observed forest loss. 
    """
    basins_lossyear = gpd.sjoin(
        basins, 
        lossyear_points, 
        how="inner", 
        predicate='contains'
    ).reset_index()

    lossyear_count = basins_lossyear.groupby(["HYBAS_ID", "lossyear"]).size().rename("n").reset_index()
    
    lossyear_count = pd.pivot_table(
        lossyear_count, 
        values="n", 
        index="HYBAS_ID", 
        columns="lossyear", 
        fill_value=0
    ).rename(columns={y: f"lossyear{y:02}" for y in range(1, 23)})
    return lossyear_count


# COMMAND ----------

# WARNING: long running code (~1h)
for tile_area in gfc_tile_areas:
    print(tile_area)
    lossyear_path = os.path.join(gfc_path, f"lossyear/{tile_area}.tif")
    lossyear_data = open_GFC_tile(lossyear_path, roi)
    h, w = lossyear_data.shape
    chunk_size = 100
    # The GFC raster data is is organized in blocks of size (1, 40000).
    # For optimal performance the data should be read in chunks where 
    # the size is a multiple of the block size of the raster file.
    # Here the data is loaded in chunks of size (chunk_size, 40000)
    for i in tqdm(range(h // chunk_size)):
        chunk_slice = {"y": slice(i*chunk_size, (i+1)*chunk_size)}
        lossyear_chunk = lossyear_data.isel(y=slice(i*chunk_size, (i+1)*chunk_size))
        lossyear_points = non_zero_to_df(lossyear_chunk, "lossyear")
        lossyear_count = count_loss_per_basin_year(lossyear_points, basins)
        basins.loc[lossyear_count.index, lossyear_count.columns] += lossyear_count
    lossyear_data.close()

# COMMAND ----------

basins["total_loss"] = basins[lossyear_cols].sum(axis=1)
basins["relative_loss"] = basins["total_loss"] / basins["SUB_AREA"]

# COMMAND ----------

basins.to_parquet(output_path)
