"""
Script for preparing 250m buffer data for zcta.

This script loads geographic and zip code data, filters relevant zip codes (like FL),
creates GeoDataFrames, applies geographic transformations, and calculates intersections
between different geographic layers. The output is used for linking exposome data
with geographic areas, allowing environmental exposure assessments.

Usage:
    buffer_creation_block_groups.py -d=<data_path> -o=<output_path> -s=<shape_path>

Arguments:
    -d=<data_path>    Path to the directory containing the CSV file with zip9s,
                      longitude, and latitude information.
                      The CSV must have the following headers: AREAKEY, X, Y
    -o=<output_path>  Path where the output files will be saved.
    -s=<shape_path>   Path to the directory containing the shapefile to be used.
                      This shapefile should represent geographic boundaries relevant
                      to the study (e.g., census block groups).

Functions:
    prepare_zip_data(zip_data): Filters and prepares zip code data for processing.
    create_geodataframe(data): Converts data into a GeoDataFrame setting appropriate
                               geographic coordinates and CRS.
    apply_buffer_and_transform(geo_df, buffer_distance, epsg_initial):
                               Applies a buffer to GeoDataFrame, converts CRS, and
                               calculates area.
    calculate_intersection(buffered_df, shape_df): Calculates the intersection
                                                   between buffered GeoDataFrame
                                                   and a shapefile, and computes
                                                   value percentage.

Example of running the script:
    python buffer_creation_block_groups.py -d="./data" -o="./output" -s="./shapefiles"
"""

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyproj import CRS

# Set up paths for data and output
data_path = ""
output_path = ""
shape_path = "" # "/Dropbox (UFL)/Exposome_Data_Linkage_Tool/buffer_files/shape_files/"

# Load the shapefile
flzcta10 = gpd.read_file(shape_path + 'tl_2010_12_zcta510/tl_2010_12_zcta510.shp')

# Load ZIP code data
zip9 = pd.read_csv(data_path + "combined_zip9s.csv")

def prepare_zip_data(zip_data):
    """Ensure 'zip9' column is string type and filter relevant ZIP codes."""
    zip_data['zip9'] = zip_data['AREAKEY'].astype(str)
    filtered_zip = zip_data[zip_data['zip9'].str[:2].isin(['32', '33', '34'])]
    return filtered_zip

def create_geodataframe(data):
    """Convert a DataFrame with longitude and latitude into a GeoDataFrame, setting coordinates and CRS."""
    data['long'] = data['X']
    data['lat'] = data['Y']
    data['geometry'] = gpd.points_from_xy(data['long'], data['lat'])
    geo_df = gpd.GeoDataFrame(data, crs="EPSG:4326")
    return geo_df

def apply_buffer_and_transform(geo_df, buffer_distance, epsg_initial):
    """Apply a buffer to GeoDataFrame, convert CRS, and calculate area."""
    geo_df = geo_df.to_crs(epsg_initial)
    geo_df['geometry'] = geo_df['geometry'].buffer(buffer_distance, resolution=100)
    geo_df['area'] = geo_df.geometry.area
    return geo_df

def calculate_intersection(buffered_df, shape_df):
    """Calculate the intersection between buffered GeoDataFrame and shapefile, and compute value percentage."""
    shape_df = shape_df.to_crs(buffered_df.crs)
    overlap = gpd.overlay(buffered_df, shape_df[['ZCTA5CE10', 'geometry']], how='intersection')
    overlap['intersection_area'] = overlap.geometry.area
    overlap['value'] = overlap['intersection_area'] / overlap['area']
    return overlap[['value', 'zip9', 'ZCTA5CE10']]

# Prepare the FL ZIP code data
flzip = prepare_zip_data(zip9)

# Create GeoDataFrame
dat1 = create_geodataframe(flzip)

# Define project and apply buffer and project
usa_contiguous_albers_equal_area = CRS.from_proj4(
    "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
buffer250 = apply_buffer_and_transform(dat1, 250, usa_contiguous_albers_equal_area)

# Calculate intersection
p4ZCTA250m = calculate_intersection(buffer250, flzcta10)

# save to CSV
p4ZCTA250m.to_csv(output_path + 'buffer250tozcta_all.csv')

# Display the first few rows
print(p4ZCTA250m.head())

