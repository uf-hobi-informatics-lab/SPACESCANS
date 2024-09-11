import pandas as pd
import os
import re

def extract_year_from_filename(file_path):
    # Assuming the year is the last 4 digits before the file extension in the file name
    match = re.search(r'(\d{4})', os.path.basename(file_path))
    if match:
        return match.group(1)
    else:
        raise ValueError("No year found in the file name")

def standardize_column_names(df):
    # Convert all column names to lowercase and remove leading/trailing spaces
    df.columns = df.columns.str.strip().str.upper()
    return df

def rename_columns(df):
    # Define the column name changes
    column_mapping = {
        'CensusTract': 'FIPS',
        'POP2010': 'POP',
        'Pop2010': 'POP',
    }
    
    df = df.rename(columns=column_mapping)
    return df

def read_raw_exposome(file_path):
    year = extract_year_from_filename(file_path)
    new_fara = pd.read_excel(file_path, sheet_name=2, engine='openpyxl')
    new_fara = rename_columns(new_fara)
    new_fara = standardize_column_names(new_fara)
    new_fara['YEAR'] = year
    return new_fara

def save_exposome(dataframe, output_path):
    dataframe.to_csv(output_path, index=False)

def main():
    file_path1 = '/Users/allison.burns/Desktop/exposome/FARA/FoodAccessResearchAtlasData2019.xlsx'
    file_path2 = '/Users/allison.burns/Desktop/exposome/FARA/FoodAccessResearchAtlasData2015.xlsx'
    output_path = '/Users/allison.burns/Desktop/exposome/FARA/formatted_fara.csv'


    # Read and process the first file
    new_fara1 = read_raw_exposome(file_path1)
    
    # Read and process the second file
    new_fara2 = read_raw_exposome(file_path2)

    # Align columns to prevent duplication
    common_columns = list(set(new_fara1.columns) & set(new_fara2.columns))
    new_fara1 = new_fara1[common_columns]
    new_fara2 = new_fara2[common_columns]

    # Append the data from the second file to the first file
    combined_fara = pd.concat([new_fara1, new_fara2], ignore_index=True)
    print(combined_fara.columns.values)

    # Define the desired order of the columns
    desired_order = ['FIPS', 'POP', 'YEAR', 'STATE', 'COUNTY']  # Add other columns as needed

    # Add remaining columns to the desired order
    remaining_columns = [col for col in combined_fara.columns if col not in desired_order]
    final_column_order = desired_order + remaining_columns

    # Reorder the columns in the DataFrame
    combined_fara = combined_fara[final_column_order]

    # Save the combined data to a CSV file
    save_exposome(combined_fara, output_path)

if __name__ == '__main__':
    main()
