'''
    TO: Convert caces and National Walkability Index into by zip9 and per time identifier
    Notes: Original data structure is by block ground and YEAR

'''
import pandas as pd
import os

def read_formatted_exposome():
    formatted_exposome = pd.read_csv(exposome_path)
    return formatted_exposome

def read_buffer():
    buffer_path = '/home/cwang6/exposome/data/original_data/buffer250tobg_all.csv' # Change this path if necessary
    buffer = pd.read_csv(buffer_path)
    return buffer

def block_group_translator(formatted_exposome, buffer):
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    
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
    df_result.rename(columns={'zip9': 'ZIP_9'}, inplace=True)  
    
    return df_result

def save_preprocess_exposome(types,exposome):
    exposome.to_csv(f'preprocess_{types}.csv', index=False)

def main():
    #CACES: year
    #exposome_path = '/home/cwang6/exposome/data/original_data/formatted_caces.csv'   
    formatted_exposome = read_formatted_exposome()
    buffer = read_buffer()
    preprocess_exposome = block_group_translator(formatted_exposome, buffer)

    #National Walkability Index: cross-sectional
    #exposome_path = '/home/cwang6/exposome/data/original_data/formatted_walk_index.csv'
    
    # Extract the file name from the path
    file_name = os.path.basename(exposome_path).lower()
    
    # Check if the file name contains certain keywords
    if 'cace' in file_name:    
        save_preprocess_exposome('caces', preprocess_exposome)
    elif 'wi' in file_name or 'walk' in file_name or 'walkbility' in file_name:
        save_preprocess_exposome('wi', preprocess_exposome)
    else:
        save_preprocess_exposome('exposome', preprocess_exposome)
        
if __name__== '__main__':
    main()
