'''
    TO: Convert fara, nata, hud, and acs into by zip9 and per time identifier
    Notes: Original data structure is by cencus tract and YEAR (or "YEAR AND QUARTER" or YEAR-QUARTER)

'''

import pandas as pd
import os

def read_formatted_exposome():
    return pd.read_csv(exposome_path)

def read_buffer():
    buffer_path = '/home/cwang6/exposome/data/original_data/' #change this path if necessary
    return pd.read_csv(data_path + 'buffer250totract_all.csv')

def cencus_tract_tanslator(formatted_exposome, buffer):
    formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
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

        grouped = df.groupby(['zip9', 'YEAR', 'QUARTER']).agg({var + '_weighted': 'sum' for var in columns_expo})
        grouped['value_sum'] = df.groupby(['zip9', 'YEAR', 'QUARTER'])['value'].sum()

    # Case 2: YEAR is in the columns but QUARTER is not
    elif 'YEAR' in columns_name and 'QUARTER' not in columns_name:
        exclude_columns = ['zip9', 'GEOID10', 'value', 'YEAR']
        columns_expo = [col for col in df.columns if col not in exclude_columns]

        for var in columns_expo:
            df[var + '_weighted'] = df['value'] * df[var]

        grouped = df.groupby(['zip9', 'YEAR']).agg({var + '_weighted': 'sum' for var in columns_expo})
        grouped['value_sum'] = df.groupby(['zip9', 'YEAR'])['value'].sum()

    # Case 3: YEAR-QUARTER is in the columns
    elif 'YEAR-QUARTER' in columns_name:
        exclude_columns = ['zip9', 'GEOID10', 'value', 'YEAR-QUARTER']
        columns_expo = [col for col in df.columns if col not in exclude_columns]

        for var in columns_expo:
            df[var + '_weighted'] = df['value'] * df[var]

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
         
    return df_result

def save_preprocess_exposome(types,exposome):
    exposome.to_csv(f'preprocess_{types}.csv', index=False)

def main():
    #exposome_path = '/home/cwang6/exposome/data/original_data/formatted_fara.csv' #change this path if necessary
    
    formatted_exposome = read_formatted_exposome()
    buffer = read_buffer()
    preprocess_exposome = cencus_tract_tanslator(formatted_exposome, buffer)
    
    # Extract the file name from the path
    file_name = os.path.basename(exposome_path).lower()
    
    # Check if the file name contains certain keywords
    if 'fara' in file_name:    
        save_preprocess_exposome('fara', preprocess_exposome)
    
    elif 'nata' in file_name:
        save_preprocess_exposome('nata', preprocess_exposome)
    
    elif 'hud' in file_name:
        save_preprocess_exposome('hud', preprocess_exposome)
    
    elif 'acs' in file_name:
        save_preprocess_exposome('acs', preprocess_exposome)
    
    else:
        save_preprocess_exposome('exposome', preprocess_exposome)

if __name__ == '__main__':
    main()
