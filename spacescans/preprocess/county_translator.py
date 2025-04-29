# Convert ucr into by zip9 and per YEAR
## original data structure is by county and YEAR

import pandas as pd
import argparse
from args import parse_args_with_defaults

def read_formatted_exposome(exposome_path):
    return pd.read_csv(exposome_path)

def read_buffer(buffer_path):
    return pd.read_csv(buffer_path + 'buffer250tocounty_all.csv')

def county_tanslator(formatted_exposome, buffer):
    formatted_exposome.rename(columns={'FIPS': 'GEOID10'}, inplace=True)
    buffer.rename(columns={'fips': 'GEOID10'}, inplace=True)
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

    # Drop weighted columns to free up memory
    grouped = grouped.drop(columns=[var + '_weighted' for var in columns_expo])

    df_result = grouped[columns_expo].reset_index()
    df_result.rename(columns = {'zip9': 'ZIP_9'}, inplace = True)
    # Format ZIP_9 as String(9)
    df_result['ZIP_9'] = df_result['ZIP_9'].astype(str).str.zfill(9)

    # Format YEAR as String(4)
    df_result['YEAR'] = df_result['YEAR'].astype(str).str.zfill(4)
    
    keep_columns = ['ZIP_9', 'YEAR', 'p_assault', 'p_burglary', 'p_fso', 'p_larceny', 'p_murder', 'p_mvt', 'p_rob']
    df_result = df_result[keep_columns]
    return df_result

def save_preprocess_ucr(exposome, exposome_type, output_path):
    
    exposome.to_csv(f'{output_path}preprocess_{exposome_type}.csv', index=False)

def main(exposome_type, output_path, buffer_path):
 
    exposome_path = f'{output_path}formatted_{exposome_type}.csv'
    formatted_exposome = read_formatted_exposome(exposome_path)
    buffer = read_buffer(buffer_path)
    preprocess_exposome = county_tanslator(formatted_exposome, buffer)
    save_preprocess_ucr(preprocess_exposome, exposome_type, output_path)

if __name__ == '__main__':

    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["exposome_type"], args["output_dir"], args["buffer_dir"])