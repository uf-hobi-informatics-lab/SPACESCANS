import pandas as pd
import os
import re
import sys
import argparse
from args import parse_args_with_defaults

# Extract year from file name
def extract_year_from_filename(file_path):
    file_name = os.path.basename(file_path)
    match = re.search(r'(19|20)\d{2}', file_name)
    if match:
        return match.group(0)
    else:
        raise ValueError(f"No year found in the file name: {file_name}")

# Standardize column names
def standardize_column_names(df):
    df.columns = df.columns.str.strip().str.upper()
    return df

# Rename specific columns
def rename_columns(df):
    column_mapping = {
        'CensusTract': 'FIPS',
        'POP2010': 'POP',
        'Pop2010': 'POP',
    }
    return df.rename(columns=column_mapping)

# Read and process a single raw file
def read_raw_exposome(file_path):
    year = extract_year_from_filename(file_path)
    new_fara = pd.read_excel(file_path, sheet_name=2, engine='openpyxl')
    new_fara = rename_columns(new_fara)
    new_fara = standardize_column_names(new_fara)
    new_fara['YEAR'] = year
    return new_fara

# Save combined data to CSV
def save_exposome(dataframe, output_path):
    dataframe.to_csv(output_path + "formatted_fara.csv", index=False)

# Process multiple files and combine them
def process_and_combine_files(file_paths, output_path):
    if not file_paths:
        print("Error: No file paths provided.")
        sys.exit(1)

    combined_fara = None
    for idx, file_path in enumerate(file_paths):
        print(f"Processing file {idx + 1}: {file_path}")
        current_fara = read_raw_exposome(file_path)
        combined_fara = pd.concat([combined_fara, current_fara], ignore_index=True) if combined_fara is not None else current_fara

    desired_order = ['FIPS', 'YEAR', 'STATE', 'COUNTY','POP']
    remaining_columns = [col for col in combined_fara.columns if col not in desired_order]
    final_column_order = desired_order + remaining_columns
    
    combined_fara = combined_fara[final_column_order]
    combined_fara.drop(columns=['STATE', 'COUNTY', 'URBAN'], inplace=True) # Drop the columns not needed

    save_exposome(combined_fara, output_path)
    print(f"Combined data saved to {output_path}formatted_fara.csv")

def main(file_paths, output_path):
    process_and_combine_files(file_paths, output_path)
    
if __name__ == "__main__":
    
    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["data_list"],args["output_dir"])    