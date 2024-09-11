import pandas as pd
import os
import re
from dbfread import DBF
import csv
import glob

def extract_year_quarter(filename):
    # Assuming the filename is in the format: usps_vac_032023_tractsum_2kx.dbf 
    parts = filename.split('_')[-3]  # Get the 'mmYYYY.dbf' part
    month = int(parts[:2])  # Extract month (mm)
    year = int(parts[2:6])  # Extract year (YYYY)
    quarter = (month - 1) // 3 + 1  # Calculate the quarter
    return year, quarter

def dbfs_to_csv_with_date(dbf_folder_path, csv_file_path):
    dbf_files = glob.glob(f'{dbf_folder_path}/*.dbf')
    if not dbf_files:
        print("No DBF files found in the specified folder.")
        return

    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        headers_written = False

        for dbf_file in dbf_files:
            year, quarter = extract_year_quarter(dbf_file)
            table = DBF(dbf_file, ignore_missing_memofile=True, lowernames=False)

            # Write the header from the first file and append 'Year', 'Quarter'
            if not headers_written:
                # Capitalize ALL field names and append 'YEAR', 'QUARTER'
                
                field_names = [field.upper() for field in table.field_names]
                writer.writerow(['YEAR', 'QUARTER'] + field_names )
                headers_written = True

            # Write each record with the added year and quarter
            for record in table:
                row = [year, quarter] + list(record.values())  
                writer.writerow(row)

def main():
    dbf_folder_path = '/Users/allison.burns/Desktop/HUD/DB_Files'
    csv_file_path = '/Users/allison.burns/Desktop/HUD/OUTPUT/FORMATTED_HUD.csv'

    dbfs_to_csv_with_date(dbf_folder_path, csv_file_path)

if __name__ == '__main__':
    main()
