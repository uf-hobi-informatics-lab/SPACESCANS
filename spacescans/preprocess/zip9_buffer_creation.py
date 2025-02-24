import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import transform
from pyproj import CRS, Transformer

def zip9_to_geospatial(data_list, output_dir, buffer_distance=250):
    """
    Processes geospatial data by:
    - Reading CSV data
    - Renaming columns
    - Converting to a GeoDataFrame
    - Transforming to Albers Equal Area projection
    - Creating buffer geometry
    - Converting back to EPSG:4326
    - Saving outputs as CSV and GeoJSON

    Parameters:
    - data_list (str): Path to input CSV file
    - output_dir (str): Directory to save outputs
    - buffer_distance (int): Buffer size in meters
    """

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    dat = pd.read_csv(data_list)

    # Subset data
    # dat = dat.head(100000)    #just for demo

    # Rename columns 
    dat = dat.rename(columns={'X': 'long', 'Y': 'lat'})  

    # Convert DataFrame to GeoDataFrame
    gdf = gpd.GeoDataFrame(dat, geometry=gpd.points_from_xy(dat['long'], dat['lat']), crs="EPSG:4326")

    # Define target CRS (USA Contiguous Albers Equal Area Conic)
    target_crs = CRS.from_proj4("+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")

    # Convert to projected CRS
    gdf = gdf.to_crs(target_crs)

    # Create buffer geometry
    gdf["geometry"] = gdf.geometry.buffer(buffer_distance, resolution=100)

    # Transform back to geographic CRS (EPSG:4326)
    gdf = gdf.to_crs("EPSG:4326")

    # Save as CSV
    # gdf.to_csv(os.path.join(output_dir, "long_lat_buffer250_wgs84.csv"), index=False)

    Save as GeoJSON
    gdf.to_file(os.path.join(output_dir, "zip9_buffer250_wgs84_sf.geojson"), driver="GeoJSON")
    return gdf

    print(f"Processing completed. Files saved to {output_dir}")
    

    
    
# main
if __name__ == 'main':
    zip9_to_geospatial(
        data_list="/home/cwang6/exposome/data/original_data/combined_zip9s.csv",
        output_dir="/blue/chenaokun1990/cwang6/exposome/output/spacescan",
        buffer_distance=250
    )
