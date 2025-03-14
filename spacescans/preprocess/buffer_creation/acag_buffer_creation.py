import pandas as pd
import dask.dataframe as dd
import dask_geopandas

dat = dd.read_csv("~/Documents/Exposome/zip_code_info/combined_zip9s.csv")

dat = dat.rename(columns={'X':'long', 'Y':'lat'})
gdf = dask_geopandas.from_dask_dataframe(dat)
gdf = gdf.set_geometry(dask_geopandas.points_from_xy(gdf, 'long', 'lat'))

gdf = gdf.set_crs('epsg:4326')
gdf = gdf.to_crs("esri:102003")
gdf.geometry = gdf.geometry.buffer(250, resolution=100)
gdf = gdf.to_crs("epsg:4326")

gdf.to_csv("/home/gdf.csv")