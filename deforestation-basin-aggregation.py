# Databricks notebook source
# MAGIC %md
# MAGIC # Naive Implementation (runtime ~1.5h)
# MAGIC This is a naive implementation for aggregating deforestation data on river sub-basin level.

# COMMAND ----------

import geopandas as gpd
import pandas as pd
import xarray as xr
import rioxarray
from shapely.geometry import Polygon
import json
import os

# COMMAND ----------

with open("config.json") as f:
    config = json.load(f)
    
roi = config["GFC_ROI_CENTRAL_AFRICA"]
basin_path = config["BASIN_PATH"]
basin_level = config["GFC_BASIN_AGGREGATION_LEVEL"]
gfc_tile_areas = config["GFC_TILE_AREAS"]
gfc_path = config["GFC_PATH"]
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

def load_GFC_tile(src_path: str, roi: dict[str, float]) -> xr.DataArray:
    """Load GFC tile, squeeze and slice to ROI."""
    data = rioxarray.open_rasterio(src_path)
    data = data.squeeze() # data is single band, use squeeze to drop band dimension
    data = data.sel(
        x=slice(roi["lon_min"], roi["lon_max"]), 
        y=slice(roi["lat_max"], roi["lat_min"])
    )
    return data

def data_chunk_to_points(data_chunk: xr.DataArray) -> gpd.GeoDataFrame:
    """Convert a chunk from the GFC tiles to a GeoDataFrame."""
    df = data_chunk.to_dataframe(name="lossyear").reset_index()
    df = df[df["lossyear"] > 0]
    lossyear_points = gpd.GeoDataFrame(
        df["lossyear"], 
        geometry=gpd.points_from_xy(df.x, df.y), 
        crs=data_chunk.spatial_ref.crs_wkt
    )
    return lossyear_points

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

# WARNING: long running code (~1.5h)
for tile_area in gfc_tile_areas:
    tile_name = f"Hansen_GFC-2022-v1.10_lossyear_{tile_area}.tif"
    src_path = os.path.join(gfc_path, tile_name)
    data = load_GFC_tile(src_path, roi)
    h, w = data.shape
    for i in range(h // chunk_size):
        for j in range(w // chunk_size):
            data_chunk = data.isel(
                x=slice(i*chunk_size, (i+1)*chunk_size), 
                y=slice(j*chunk_size, (j+1)*chunk_size))
            lossyear_points = data_chunk_to_points(data_chunk)
            lossyear_count = count_loss_per_basin_year(lossyear_points, basins)
            basins.loc[lossyear_count.index, lossyear_count.columns] += lossyear_count
    data.close()

# COMMAND ----------

basins["total_loss"] = basins[lossyear_cols].sum(axis=1)
# Polygons need to be projected to a cartesian projection for the area calculation to be valid
# cea: cylindrical equal area projection
basins["area"] = basins["geometry"].to_crs({'proj':'cea'}).area
basins["relative_loss"] = basins["total_loss"] / basins["area"]

# COMMAND ----------

basins.to_parquet(output_path)
