from itertools import product
import pandas as pd
import sys
import os
import argparse
from args import parse_args_with_defaults

def read_formatted_exposome(exposome_path):
    formatted_exposome = pd.read_csv(exposome_path)
    return formatted_exposome

def read_buffer(buffer_path):
    buffer = pd.read_csv(buffer_path + "buffer250tobg_all.csv")
    return buffer

def rename(formatted_exposome, exposome_type):
    
    if 'caces' in exposome_type.lower():    
        formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
        
    if exposome_type.lower() == 'wi':
        formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
        formatted_exposome.rename(columns={'NatWalkInd': 'WALKABILITY'}, inplace=True)

    return formatted_exposome

def drop_columns(formatted_exposome, exposome_type):
  
    return formatted_exposome

def block_group_translator(formatted_exposome, buffer):
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    
    df = pd.merge(buffer, formatted_exposome, on='GEOID10', how='left')
    
    exclude_columns = ['zip9', 'GEOID10', 'value', 'YEAR']
    columns_expo = [col for col in df.columns if col not in exclude_columns]
    
    for var in columns_expo:
        df[var + '_weighted'] = df['value'] * df[var]

    grouped = df.groupby(['zip9', 'YEAR']).agg({var + '_weighted': 'sum' for var in columns_expo})
    grouped['value_sum'] = df.groupby(['zip9', 'YEAR'])['value'].sum()
    
    # Drop the original DataFrame to free up memory
    del df
    
    for var in columns_expo:
        grouped[var] = grouped[var + '_weighted'] / grouped['value_sum']

    df_result = grouped[columns_expo].reset_index()
    df_result.rename(columns = {'zip9': 'ZIP_9'}, inplace = True)
    # Format ZIP_9 as String(9)
    df_result['ZIP_9'] = df_result['ZIP_9'].astype(str).str.zfill(9)

    # Format YEAR as String(4)
    df_result = df_result.dropna(subset=['YEAR'])
    df_result['YEAR'] = df_result['YEAR'].astype(int).astype(str).str.zfill(4)
    
    return df_result

def save_preprocess_exposome(types, exposome, output_path):
    exposome.to_csv(f'{output_path}preprocess_{types}.csv', index=False)
    

def main(exposome_type, output_path, buffer_path):
    
    # Check if the file name contains certain keywords
    if 'caces' in exposome_type:    
        exposome_path = f'{output_path}formatted_caces.csv'
    
    elif 'wi' in exposome_type:
        exposome_path = f'{output_path}formatted_wi.csv'
 
        
    formatted_exposome = read_formatted_exposome(exposome_path)
    rename(formatted_exposome, exposome_type)
    drop_columns(formatted_exposome, exposome_type)
    buffer = read_buffer(buffer_path)
    preprocess_exposome = block_group_translator(formatted_exposome, buffer)

    # Extract the file name from the path
    file_name = os.path.basename(exposome_path).lower()
    
    # Check if the file name contains certain keywords
    if 'caces' in file_name:    
        save_preprocess_exposome('caces', preprocess_exposome, output_path)
    
    elif 'wi' in file_name:
        save_preprocess_exposome('wi', preprocess_exposome, output_path)
    
    else:
        save_preprocess_exposome('exposome', preprocess_exposome, output_path)
    
    
if __name__== '__main__':

    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["exposome_type"], args["output_dir"], args["buffer_dir"])