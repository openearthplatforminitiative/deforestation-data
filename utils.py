import geopandas as gpd
import pandas as pd
import dask.array as da
import xarray as xr
import rioxarray
import rio_cogeo
import os

def get_gfc_path(product: str, area: str, spark_api: bool=True) -> str:
    prefix = "dbfs:/" if spark_api else "/dbfs/"
    gfc_root = "mnt/openepi-storage/global-forest-change/" 
    gfc_path = os.path.join(prefix, gfc_root, product, f"{area}.tif")
    return gfc_path

def get_resolution(raster_src: str | xr.DataArray | xr.Dataset) -> float:
    """Return the resoluton of a GeoTIFF given by path or an open xarray object."""
    if isinstance(raster_src, str):
        return abs(rio_cogeo.cog_info(raster_src).GEO.Resolution[0])
    else:
        return abs(raster_src.rio.resolution()[0])

def open_gfc_tile(src_path: str, roi: dict[str, float]) -> xr.DataArray:
    """Load GFC tile, squeeze and slice to ROI."""
    data = rioxarray.open_rasterio(src_path, chunks=(1, 1024, 1024))
    data = data.squeeze() # data is single band, use squeeze to drop band dimension
    data = data.sel(
        x=slice(roi["lon_min"], roi["lon_max"]), 
        y=slice(roi["lat_max"], roi["lat_min"])
    )
    return data

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the haversine distance between two geographical points in meters."""
    R = 6371  # radius of Earth in km
    phi_1 = da.radians(lat1)
    phi_2 = da.radians(lat2)
    delta_phi = da.radians(lat2 - lat1)
    delta_lambda = da.radians(lon2 - lon1)
    a = da.sin(delta_phi/2.0)**2 + da.cos(phi_1) * da.cos(phi_2) * da.sin(delta_lambda/2.0)**2
    c = 2 * da.arctan2(da.sqrt(a), da.sqrt(1-a))
    meters = R * c  # output distance in km
    return meters

def calculate_pixel_area(df, pixel_size):
    lat, lon = df.y, df.x
    # Calculate coordinates of neighboring points
    lat_north = lat + pixel_size / 2
    lat_south = lat - pixel_size / 2
    lon_east = lon + pixel_size / 2
    lon_west = lon - pixel_size / 2
    
    # Calculate distances to neighboring points
    height = haversine(lat_south, lon_west, lat_north, lon_west)
    width = haversine(lat_south, lon_west, lat_south, lon_east)
    
    # Calculate area
    area = height * width
    return area

def non_zero_to_df(data: xr.DataArray, name: str="non_zero") -> gpd.GeoDataFrame:
    """Take all non-zero cells in a DataArray and convert them to points in a GeoDataFrame."""
    df = data.to_dask_dataframe(name=name).reset_index()
    df = df[df[name] > 0]
    gdf = gpd.GeoDataFrame(
        df[name], 
        geometry=gpd.points_from_xy(df.x, df.y), 
        crs=data.spatial_ref.crs_wkt
    )
    pixel_size = get_resolution(data)
    gdf["lossarea"] = calculate_pixel_area(df, pixel_size)
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
    )
    print(basins_lossyear.columns)
    lossyear_count = basins_lossyear.groupby(["HYBAS_ID", "lossyear"]).agg({"lossarea": "sum"})
    return lossyear_count
