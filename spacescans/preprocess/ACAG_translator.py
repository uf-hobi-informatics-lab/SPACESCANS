import os
import re
import glob
import xarray as xr
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_bounds
import geopandas as gpd
from exactextract import exact_extract
from args import parse_args_with_defaults


def ncTotiff(file_path, output_dir, pollutant):

    print(f"Processing: {file_path}")
    t1 = pd.Timestamp.now()

    # Load NetCDF file
    ds = xr.open_dataset(file_path)
    
    # Extract pollutant data
    da = ds[pollutant]

    # Select time step if "time" is a dimension
    if "time" in da.dims:
        da = da.isel(time=0) 

    # Convert to NumPy array
    raster_values = da.values
    print(f"Raster data shape: {raster_values.shape}")

    lon = ds["LON"].values
    lat = ds["LAT"].values

    # Create geotransform (aligning grid)
    transform = from_bounds(lon.min(), lat.min(), lon.max(), lat.max(), 
                            raster_values.shape[1], raster_values.shape[0])

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Define output file path
    tiff_filename = f"{pollutant}_{os.path.basename(file_path).replace('.nc', '.tif')}"
    tiff_path = os.path.join(output_dir, tiff_filename)

    # Save as GeoTIFF
    with rasterio.open(
        tiff_path, "w", driver="GTiff",
        height=raster_values.shape[0], width=raster_values.shape[1],
        count=1, dtype=raster_values.dtype, crs="EPSG:4326", transform=transform
    ) as dst:
        dst.write(raster_values, 1)

    print(f"Saved raster to: {tiff_path}")
    return tiff_path

def read_buffer(buffer_path):
    return gpd.read_file(buffer_path + 'zip9_buffer250_wgs84_sf.geojson')

def extract_year_month(file_path):
    """
    Extracts the year and month from a file name given its full path.

    Parameters:
        file_path (str): The full path of the file.

    Returns:
        tuple: (year, month) as strings if found, otherwise (None, None).
    """
    # Extract filename from the full path
    filename = os.path.basename(file_path)

    # Regular expression to find the year and month pattern
    match = re.search(r"_(\d{4})(\d{2})_", filename)

    if match:
        return match.group(1), match.group(2)  # (year, month)
    
    return None, None  # Return None if no match found


def raster_to_zip9_translator(pollutant, zip_buffer_gdf, data_list, output_dir, start_index=121):
    """
    Processes NetCDF (.nc) raster data for a specific pollutant using `exact_extract()`.
    Extracts values using buffered ZIP areas and saves the results.

    Parameters:
    - pollutant (str): The name of the pollutant being processed.
    - zip_buffer_gdf (GeoDataFrame): Buffered spatial areas for extraction.
    - data_list (str): Path to input raster files.
    - output_dir (str): Path to save processed files.
    - start_index (int): The starting index for file processing (default 121).
    """
    
    # Get all .nc files for the pollutant
    file_list = sorted(glob.glob(os.path.join(data_list, pollutant, "*.nc")))

    if not file_list:
        raise FileNotFoundError(f"No .nc files found in {os.path.join(data_list, pollutant)}")
    
    for j in range(start_index, len(file_list)):
        file_path = file_list[j]
        
        # Load the .nc file and convert into .tiff
        tiff_path = ncTotiff(file_path, output_dir, pollutant)

        # Perform exact extraction
        dat = exact_extract(tiff_path, zip_buffer_gdf,'mean', include_cols= 'AREAKEY',  output='pandas')
        
        # Create output DataFrame
        dat.rename(columns={'mean': pollutant, 'AREAKEY': 'ZIP_9'}, inplace=True)
        year, month = extract_year_month(file_path)
        dat['YEAR'] = year
        dat['MONTH'] = month

        # Save extracted data
        save_path = os.path.join(output_dir, f"{pollutant}_{year}_{month}.csv")
        dat.to_csv(save_path, index=False)
        

    print("raster_to_zip9_translator complete.")
    

def merge_pollutant_data(output_dir, pollutants):
    """
    Loads pollutant CSV files, concatenates data for the same pollutant,
    and merges different pollutants by ZIP_9, YEAR, and MONTH.

    Parameters:
        output_dir (str): Directory where CSV files are saved.
        pollutants (list): List of pollutant names used in filenames.

    Returns:
        pd.DataFrame: Merged DataFrame of all pollutant files.
    """
    merged_pollutant = {}  # Dictionary to store concatenated data for each pollutant
    merged_df = None  # Final merged DataFrame

    # Step 1: Concatenate files for each pollutant
    for pollutant in pollutants:
        # Find all matching files for the pollutant
        pollutant_files = glob.glob(os.path.join(output_dir, f"{pollutant}_*.csv"))
        
        if not pollutant_files:
            print(f"Warning: No files found for {pollutant}. Skipping...")
            continue
        
        # Read and concatenate files
        all_files = [pd.read_csv(file) for file in pollutant_files]
        merged_pollutant[pollutant] = pd.concat(all_files, ignore_index=True)

    # Step 2: Merge pollutants on ZIP_9, YEAR, MONTH
    for pollutant, df in merged_pollutant.items():
        if merged_df is None:
            merged_df = df  # First pollutant becomes the base DataFrame
        else:
            merged_df = pd.merge(merged_df, df, on=["ZIP_9", "YEAR", "MONTH"], how="outer")

    # Step 3: Return merged DataFrame or None if empty
    if merged_df is not None:
        return merged_df
    else:
        print("No valid files found for merging.")
        return None
    
def save_preprocess_exposome(types, exposome, output_path):
    types = types.lower()
    exposome.to_csv(f'{output_path}preprocess_{types}.csv', index=False)

def main(data_list, output_dir, zip_buffer_gdf, types): 

    t1 = pd.Timestamp.now()
    
    pollutants = ['BC', 'NH4', 'NIT', 'OM', 'SO4', 'SOIL', 'SS']  
    zip_buffer_gdf = read_buffer(buffer_path) 
    for pollutant in pollutants:
        raster_to_zip9_translator(pollutant, zip_buffer_gdf, data_list, output_dir, start_index=215)    
    merged_df = merge_pollutant_data(output_dir, pollutants)
    save_preprocess_exposome(types, merged_df, output_dir)
    
        
    t2 = pd.Timestamp.now()
    print(f"Completed, Took {(t2 - t1).total_seconds()/60:.2f} Minutes")   
    
if __name__== '__main__':
      
    buffer_path = "/blue/chenaokun1990/cwang6/exposome/output/spacescan/"
    data_list = "/blue/chenaokun1990/cwang6/exposome/original/ACAG/"
    output_dir = "/blue/chenaokun1990/cwang6/exposome/output/spacescan/acag/"
    types = 'ACAG'
    main(data_list, output_dir,buffer_path,types)
