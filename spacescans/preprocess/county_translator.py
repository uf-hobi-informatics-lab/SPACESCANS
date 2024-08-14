# Convert ucr into by zip9 and per year
## original data structure is by county and year

import pandas as pd

def read_formatted_exposome():
    return pd.read_csv(exposome_path + 'formatted_ucr.csv')

def read_buffer():
    buffer_path = '/home/cwang6/exposome/data/original_data/'
    return pd.read_csv(data_path + 'buffer250tocounty_all.csv')

def county_tanslator(formatted_exposome, buffer):
    formatted_exposome.rename(columns={'fips': 'GEOID10'}, inplace=True)
    buffer.rename(columns={'fips': 'GEOID10'}, inplace=True)
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    
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

    # Drop weighted columns to free up memory
    grouped = grouped.drop(columns=[var + '_weighted' for var in columns_expo])

    df_result = grouped[columns_expo].reset_index()
    return df_result

def save_preprocess_ucr(exposome):
    exposome.to_csv(output_path + 'preprocess_ucr.csv', index=False)

def main():
    exposome_path = '/home/cwang6/exposome/data/original_data/'
    output_path = '/home/cwang6/exposome/data/output_data/temp/'
    formatted_exposome = read_formatted_exposome()
    buffer = read_buffer()
    preprocess_exposome = county_tanslator(formatted_exposome, buffer)
    save_preprocess_ucr(preprocess_exposome)

if __name__ == '__main__':
    main()
