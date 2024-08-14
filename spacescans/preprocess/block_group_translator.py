from itertools import product
import pandas as pd

def read_formatted_exposome():
    formatted_exposome = pd.read_csv(exposome_path)
    return formatted_exposome

def read_buffer():
    buffer_path = '/home/cwang6/exposome/data/original_data/buffer250tobg_all.csv'
    buffer = pd.read_csv(buffer_path)
    return buffer

def block_group_translator(formatted_exposome, buffer):
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    formatted_exposome.rename(columns={'fips': 'GEOID10'}, inplace=True)
    
    df = pd.merge(buffer, formatted_exposome, on='GEOID10', how='left')
    
    exclude_columns = ['zip9', 'GEOID10', 'value', 'year']
    columns_expo = [col for col in df.columns if col not in exclude_columns]
    
    for var in columns_expo:
        df[var + '_weighted'] = df['value'] * df[var]

    grouped = df.groupby(['zip9', 'year']).agg({var + '_weighted': 'sum' for var in columns_expo})
    grouped['value_sum'] = df.groupby(['zip9', 'year'])['value'].sum()
    
    # Drop the original DataFrame to free up memory
    del df
    
    for var in columns_expo:
        grouped[var] = grouped[var + '_weighted'] / grouped['value_sum']

    df_result = grouped[columns_expo].reset_index()
    return df_result

def save_preprocess_caces(exposome):
    exposome.to_csv(output_path + 'preprocess_caces.csv', index=False)
    
def save_preprocess_nwi(exposome):
    exposome.to_csv(output_path + 'preprocess_nwi.csv', index=False)

def main():
    output_path = '/home/cwang6/exposome/data/output_data/temp/'
    #CACES: year
    exposome_path = '/home/cwang6/exposome/data/original_data/formatted_caces.csv'   
    formatted_exposome = read_formatted_exposome()
    buffer = read_buffer()
    preprocess_exposome = block_group_translator(formatted_exposome, buffer)
    save_preprocess_caces(preprocess_exposome)

    #National Walkability Index: cross-sectional
    exposome_path = '/home/cwang6/exposome/data/original_data/formatted_walk_index.csv'
    formatted_exposome = read_formatted_exposome()
    buffer = read_buffer()
    preprocess_exposome = block_group_translator(formatted_exposome, buffer)
    save_preprocess_nwi(preprocess_exposome)
    
if __name__== '__main__':
    main()
