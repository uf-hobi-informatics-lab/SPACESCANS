import netCDF4 as nc
import pandas as pd
import numpy as np
import os
import glob

# Predefined list of variables to search for in the file names
VARIABLES_LIST = ['BC', 'NH4', 'NIT', 'OM', 'SO4', 'SOIL', 'SS']

def extract_file_info(file_name):
    """Extracts variable, year, and month from the file name based on a predefined list."""
    try:
        # Detect and remove '.HEI_' if it exists in the filename. This is the case for year 2017 and up
        file_name = file_name.replace('.HEI', '')
        # Example file name: GWRwSPEC_BC_NA_201104_201104.nc
        parts = file_name.split('_')

        # Ensure the filename is correctly formatted
        if len(parts) < 4:
            raise ValueError(f"Filename does not match expected format: {file_name}")

        # Search for the variable in the predefined list
        variable = None
        for var in VARIABLES_LIST:
            if var in parts[1]:
                variable = var
                break
        
        if not variable:
            raise ValueError(f"Variable not found in filename: {file_name}")
        
        year = parts[3][:4]  # Extract the year (first four digits from the 4th part)
        month = parts[3][4:6]  # Extract the month (last two digits from the 4th part)
        
        return variable, year, month
    except Exception as e:
        print(f"Error processing file name {file_name}: {e}")
        return None, None, None  # Return None values if there's an error

def process_nc_file(file_path, variable, year, month,): # for testing, add limit=1000 as last arg to only test 1000 rows per file
    """Reads a NetCDF file and extracts LAT, LON, and the specified variable."""
    dataset = nc.Dataset(file_path)

    # Extract 1D variables (LON, LAT)
    lon = dataset.variables['LON'][:]
    lat = dataset.variables['LAT'][:]

    # Extract the 2D variable (e.g., BC, OM) and flatten it
    var_data = dataset.variables[variable][:].flatten()

    # Create a meshgrid of LAT and LON, so we have pairs of LAT-LON for each variable value
    lon_grid, lat_grid = np.meshgrid(lon, lat)

    # Flatten the grids so that we can pair each LAT-LON with its variable value
    lon_flat = lon_grid.flatten()
    lat_flat = lat_grid.flatten()

    # Limit the number of rows to process for testing purposes
    """if len(lon_flat) > limit:
        lon_flat = lon_flat[:limit]
        lat_flat = lat_flat[:limit]
        var_data = var_data[:limit]"""

    # Create a pandas DataFrame where each row corresponds to a (LAT, LON, Variable) triplet
    df = pd.DataFrame({
        'LAT': lat_flat,
        'LON': lon_flat,
        variable: var_data,  # This stores the actual data for the variable (e.g., 'BC')
        'Year': year,
        'Month': month
    })

    return df

def merge_with_existing(combined_df, new_df, variable):
    """Merges a new DataFrame into an existing one, updating the same variable column."""
    if combined_df is None:
        return new_df
    else:
        # Merge new data into the combined DataFrame without creating duplicate columns
        combined_df = pd.merge(combined_df, new_df, on=['LAT', 'LON', 'Year', 'Month'], how='outer')

        # If both columns for the same variable (e.g., 'BC') exist, merge them into one
        if f"{variable}_x" in combined_df.columns and f"{variable}_y" in combined_df.columns:
            combined_df[variable] = combined_df[f"{variable}_x"].combine_first(combined_df[f"{variable}_y"])
            combined_df = combined_df.drop(columns=[f"{variable}_x", f"{variable}_y"])
        
        return combined_df

def process_multiple_files(directory, output_csv): # for testing, add limit=1000 as last arg to only test 1000 rows per file
    """Processes multiple NetCDF files in a directory and appends results to a CSV with columns for each variable."""
    combined_df = None  # Initialize as None to handle the first merge properly

    # Get a list of all .nc files in the directory
    file_paths = glob.glob(os.path.join(directory, '*.nc'))

    # Loop through each file and process it
    file_count = 0
    for file_path in file_paths:
        file_name = os.path.basename(file_path)  # Get the filename
        variable, year, month = extract_file_info(file_name)  # Extract variable, year, month from the filename
        file_count += 1  # Increment the counter
        print(f"Process File {file_count}: {file_path}")
        if variable is None or year is None or month is None:
            print(f"Skipping file due to missing information: {file_name}")
            continue  # Skip files with missing information

        # Process the current NetCDF file
        new_df = process_nc_file(file_path, variable, year, month)

        # Merge with the existing combined DataFrame
        combined_df = merge_with_existing(combined_df, new_df, variable)

    # Sort the final DataFrame by Year and Month
    combined_df.sort_values(by=['Year', 'Month'], inplace=True)
    combined_df.columns = combined_df.columns.str.upper()

    # Write the combined data to a CSV file
    combined_df.to_csv(output_csv, index=False)
    print(f"All data has been successfully exported to {output_csv}")


# File path for input and output files
directory = '/Users/allison.burns/Desktop/exposome/ACAG/TEST_DATA' 
output_csv = '/Users/allison.burns/Desktop/exposome/ACAG/FINAL/final_acag_output.csv'

def main(): 
    print('processing ... ')
    process_multiple_files(directory, output_csv) # for testing, add limit=1000 as last arg to only test 1000 rows per file
    

if __name__== '__main__':
    main()


