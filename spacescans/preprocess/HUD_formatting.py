import pandas as pd
import os
import re
from dbfread import DBF
import csv
import glob
import sys
import argparse
from args import parse_args_with_defaults

def extract_year_quarter(filename):
    # Assuming the filename is in the format: usps_vac_032023_tractsum_2kx.dbf 
    parts = filename.split('_')[-3]  # Get the 'mmYYYY.dbf' part
    month = int(parts[:2])  # Extract month (mm)
    year = int(parts[2:6])  # Extract year (YYYY)
    quarter = (month - 1) // 3 + 1  # Calculate the quarter
    return year, quarter

def dbfs_to_csv_with_date(dbf_folder_path, csv_file_path):
    with open(f'{csv_file_path}formatted_hud.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        headers_written = False

        for dbf_file in dbf_folder_path:
            year, quarter = extract_year_quarter(dbf_file)
            table = DBF(dbf_file, ignore_missing_memofile=True, lowernames=False)

            # Load records into DataFrame for easy column handling
            df = pd.DataFrame(iter(table))
            df.columns = [col.upper() for col in df.columns]  # Capitalize all column names
            df.rename(columns={'GEOID': 'FIPS'}, inplace=True)  # Rename GEOID to FIPS

            # Write headers once
            if not headers_written:
                writer.writerow(['YEAR', 'QUARTER'] + list(df.columns))
                headers_written = True

            # Write data rows
            for _, row in df.iterrows():
                writer.writerow([year, quarter] + list(row))

def main(dbf_folder_path, csv_file_path):

    dbfs_to_csv_with_date(dbf_folder_path, csv_file_path)

if __name__ == '__main__':

    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["data_list"],args["output_dir"])    