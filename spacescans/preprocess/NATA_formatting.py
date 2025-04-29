import pandas as pd
import re
import sys
import argparse
from args import parse_args_with_defaults

def process_NATA(excel_file_path, csv_file_path):
    filename = excel_file_path.split('/')[-1]  # Get the filename from the path
    year_match = re.search(r'\d{4}', filename)
    if year_match:
        year = year_match.group(0)
    else:
        raise ValueError("No year found in the filename")

    # Read the Excel file
    df = pd.read_excel(excel_file_path)

    # Add the year column and change all column headers to upper case
    df['YEAR'] = year
    df.columns = df.columns.str.upper()
    df.rename(columns={'TRACT': 'FIPS'}, inplace=True)

    

    # Export the DataFrame to a CSV file
    df.to_csv(csv_file_path + 'formatted_nata.csv', index=False)

    print("NATA formatting completed!")

def main(excel_file_path, csv_file_path):

    process_NATA(excel_file_path, csv_file_path)

if __name__== '__main__':
     
    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["data_list"][0],args["output_dir"]) 