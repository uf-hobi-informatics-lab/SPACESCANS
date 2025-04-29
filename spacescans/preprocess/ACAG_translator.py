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
from multiprocessing import Pool, cpu_count
from functools import partial


def ncTotiff(file_path, output_dir, pollutant):
    print(f"Processing: {file_path}")
    t1 = pd.Timestamp.now()

    ds = xr.open_dataset(file_path)
    da = ds[pollutant]

    if "time" in da.dims:
        da = da.isel(time=0)

    raster_values = da.values
    print(f"Raster data shape: {raster_values.shape}")

    lon = ds["LON"].values
    lat = ds["LAT"].values

    transform = from_bounds(lon.min(), lat.min(), lon.max(), lat.max(),
                            raster_values.shape[1], raster_values.shape[0])

    os.makedirs(output_dir, exist_ok=True)

    tiff_filename = f"{pollutant}_{os.path.basename(file_path).replace('.nc', '.tif')}"
    tiff_path = os.path.join(output_dir, tiff_filename)

    with rasterio.open(
        tiff_path, "w", driver="GTiff",
        height=raster_values.shape[0], width=raster_values.shape[1],
        count=1, dtype=raster_values.dtype, crs="EPSG:4326", transform=transform
    ) as dst:
        dst.write(raster_values, 1)

    print(f"Saved raster to: {tiff_path}")
    return tiff_path


def read_buffer(buffer_path):
    return gpd.read_file(os.path.join(buffer_path, 'zip9_buffer250_wgs84_sf.geojson'))


def extract_year_month(file_path):
    filename = os.path.basename(file_path)
    match = re.search(r"_(\d{4})(\d{2})_", filename)
    if match:
        return match.group(1), match.group(2)
    return None, None


def raster_to_zip9_translator(pollutant, zip_buffer_gdf, data_list, output_dir, start_index=215):
    file_list = sorted(glob.glob(os.path.join(data_list, pollutant, "*.nc")))

    if not file_list:
        print(f"No .nc files found for {pollutant}")
        return

    for j in range(start_index, len(file_list)):
        file_path = file_list[j]
        try:
            tiff_path = ncTotiff(file_path, output_dir, pollutant)
            dat = exact_extract(tiff_path, zip_buffer_gdf, 'mean', include_cols='AREAKEY', output='pandas')

            dat.rename(columns={'mean': pollutant, 'AREAKEY': 'ZIP_9'}, inplace=True)
            year, month = extract_year_month(file_path)
            dat['YEAR'] = year
            dat['MONTH'] = month

            save_path = os.path.join(output_dir, f"{pollutant}_{year}_{month}.csv")
            dat.to_csv(save_path, index=False)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    print(f"Finished: {pollutant}")


def merge_pollutant_data(output_dir, pollutants):
    merged_pollutant = {}
    merged_df = None

    for pollutant in pollutants:
        files = glob.glob(os.path.join(output_dir, f"{pollutant}_*.csv"))
        if not files:
            print(f"Warning: No files for {pollutant}")
            continue
        all_files = [pd.read_csv(f) for f in files]
        merged_pollutant[pollutant] = pd.concat(all_files, ignore_index=True)

    for pollutant, df in merged_pollutant.items():
        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=["ZIP_9", "YEAR", "MONTH"], how="outer")

    return merged_df


def save_preprocess_exposome(types, exposome, output_path):
    types = types.lower()
    exposome.to_csv(os.path.join(output_path, f'preprocess_{types}.csv'), index=False)


def process_pollutant(pollutant, zip_buffer_gdf, data_list, output_dir, start_index):
    raster_to_zip9_translator(pollutant, zip_buffer_gdf, data_list, output_dir, start_index)


def main(data_list, output_dir, buffer_path, types, parallel_num=2, start_index=215):
    t1 = pd.Timestamp.now()
 
    pollutants = ['BC', 'NH4', 'DUST', 'NO3', 'OM'] 
    zip_buffer_gdf = read_buffer(buffer_path)

    print(f"Starting with {parallel_num} parallel workers...")

    with Pool(processes=parallel_num) as pool:
        pool.map(
            partial(process_pollutant, zip_buffer_gdf=zip_buffer_gdf,
                    data_list=data_list, output_dir=output_dir, start_index=start_index),
            pollutants
        )

    merged_df = merge_pollutant_data(output_dir, pollutants)
    if merged_df is not None:
        save_preprocess_exposome(types, merged_df, output_dir)

    t2 = pd.Timestamp.now()
    print(f"Completed in {(t2 - t1).total_seconds()/60:.2f} minutes")


if __name__ == '__main__':
    # === Editable Parameters ===
    buffer_path = "/data/ACAG"
    data_list = "/data/unzipped_data/ACAG/monthly_V4NA02"
    output_dir = "/data/ACAG"
    types = 'ACAG'
    parallel_num = 10  # Adjust based on memory (2 is safer, increase if RAM allows)
    start_index = 0  # Resume from a specific file index if needed

    main(data_list, output_dir, buffer_path, types, parallel_num, start_index)
