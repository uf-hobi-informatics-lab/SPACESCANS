'''
    TO: Convert fara, nata, hud, and acs into by zip9 and per time identifier
    Notes: Original data structure is by census tract and YEAR (or "YEAR AND QUARTER" or YEAR-QUARTER)

'''

import pandas as pd
import os
import sys
import argparse
from args import parse_args_with_defaults

def read_formatted_exposome(exposome_path):
    return pd.read_csv(exposome_path)

def read_buffer(buffer_path):
    return pd.read_csv(buffer_path + 'buffer250totract_all.csv')

def rename(formatted_exposome, exposome_type):
    if 'fara' in exposome_type:    
        formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    
    elif 'nata' in exposome_type:
        formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    
    elif 'hud' in exposome_type:
        formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    
    elif 'acs' in exposome_type:
        formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    return formatted_exposome

    

def cencus_tract_tanslator(formatted_exposome, buffer):
    #formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    buffer.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    
    df = pd.merge(buffer, formatted_exposome, on='GEOID10', how='left')
    columns_name = df.columns

    # Case 1: Both YEAR and QUARTER are in the columns
    if 'YEAR' in columns_name and 'QUARTER' in columns_name:
        exclude_columns = ['zip9', 'GEOID10', 'value', 'YEAR', 'QUARTER']
        columns_expo = [col for col in df.columns if col not in exclude_columns]

        for var in columns_expo:
            df[var + '_weighted'] = df['value'] * df[var]
            
        # Format YEAR as String(4), QUARTER as String(2)
        df = df.dropna(subset=['YEAR'])
        df = df.dropna(subset=['QUARTER'])
        df['YEAR'] = df['YEAR'].astype(int).astype(str).str.zfill(4)
        df['QUARTER'] = df['QUARTER'].astype(int).astype(str).str.zfill(1)
        
        grouped = df.groupby(['zip9', 'YEAR', 'QUARTER']).agg({var + '_weighted': 'sum' for var in columns_expo})
        grouped['value_sum'] = df.groupby(['zip9', 'YEAR', 'QUARTER'])['value'].sum()

    # Case 2: YEAR is in the columns but QUARTER is not
    elif 'YEAR' in columns_name and 'QUARTER' not in columns_name:
        exclude_columns = ['zip9', 'GEOID10', 'value', 'YEAR']
        columns_expo = [col for col in df.columns if col not in exclude_columns]

        for var in columns_expo:
            df[var + '_weighted'] = df['value'] * df[var]

        # Format YEAR as String(4)
        df = df.dropna(subset=['YEAR'])
        df['YEAR'] = df['YEAR'].astype(int).astype(str).str.zfill(4)
        
        
        grouped = df.groupby(['zip9', 'YEAR']).agg({var + '_weighted': 'sum' for var in columns_expo})
        grouped['value_sum'] = df.groupby(['zip9', 'YEAR'])['value'].sum()

    # Case 3: YEAR-QUARTER is in the columns
    elif 'YEAR-QUARTER' in columns_name:
        exclude_columns = ['zip9', 'GEOID10', 'value', 'YEAR-QUARTER']
        columns_expo = [col for col in df.columns if col not in exclude_columns]

        for var in columns_expo:
            df[var + '_weighted'] = df['value'] * df[var]

        # Format YEAR as String(4), QUARTER as String(6)
        df['YEAR-QUARTER'] = df['YEAR-QUARTER'].astype(str).str.zfill(6)
        
        grouped = df.groupby(['zip9', 'YEAR-QUARTER']).agg({var + '_weighted': 'sum' for var in columns_expo})
        grouped['value_sum'] = df.groupby(['zip9', 'YEAR-QUARTER'])['value'].sum()
     
    # Drop the original DataFrame to free up memory
    del df

    for var in columns_expo:
        grouped[var] = grouped[var + '_weighted'] / grouped['value_sum']

    # Drop weighted columns to free up memory
    grouped = grouped.drop(columns=[var + '_weighted' for var in columns_expo])

    df_result = grouped[columns_expo].reset_index()
    df_result.rename(columns={'zip9': 'ZIP_9'}, inplace=True)  
    # Format ZIP_9 as String(9)
    df_result['ZIP_9'] = df_result['ZIP_9'].astype(str).str.zfill(9)
         
    return df_result

def save_preprocess_exposome(types, exposome, output_path):
    exposome.to_csv(f'{output_path}preprocess_{types}.csv', index=False)

def main(exposome_type, output_path, buffer_path):
    
    
    # Check if the file name contains certain keywords
    if 'fara' in exposome_type:    
        exposome_path = f'{output_path}formatted_fara.csv'
    
    elif 'nata' in exposome_type:
        exposome_path = f'{output_path}formatted_nata.csv'
    
    elif 'hud' in exposome_type:
        exposome_path = f'{output_path}formatted_hud.csv'
    
    elif 'acs' in exposome_type:
        exposome_path = f'{output_path}formatted_acs.csv'
        
    elif 'exposome' in exposome_type:
        exposome_path = f'{output_path}formatted_exposome.csv'

        
    formatted_exposome = read_formatted_exposome(exposome_path)
    rename(formatted_exposome, exposome_type)

    buffer = read_buffer(buffer_path)
    preprocess_exposome = cencus_tract_tanslator(formatted_exposome, buffer)
    
    # Extract the file name from the path
    file_name = os.path.basename(exposome_path).lower()
    
    # Check if the file name contains certain keywords
    if 'fara' in file_name:    
        save_preprocess_exposome('fara', preprocess_exposome, output_path)
    
    elif 'nata' in file_name:
        save_preprocess_exposome('nata', preprocess_exposome, output_path)
    
    elif 'hud' in file_name:
        save_preprocess_exposome('hud', preprocess_exposome, output_path)
    
    elif 'acs' in file_name:
        save_preprocess_exposome('acs', preprocess_exposome, output_path)
    
    else:
        save_preprocess_exposome('exposome', preprocess_exposome, output_path)

if __name__ == '__main__':
    
    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["exposome_type"], args["output_dir"], args["buffer_dir"])    