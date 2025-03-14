import pandas as pd
import re

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

    

    # Export the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)

    print("NATA formatting completed!")

def main():
    # Define the path to the Excel file and the desired CSV file name
    excel_file_path = input("Please enter the path to the NATA exposome file: ")
    csv_file_path = '/Users/allison.burns/Desktop/exposome/NATA/formatted_nata.csv'  # Update with the desired CSV file name

    process_NATA(excel_file_path, csv_file_path)

if __name__== '__main__':
    main()