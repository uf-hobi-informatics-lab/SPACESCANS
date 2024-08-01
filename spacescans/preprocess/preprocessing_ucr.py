# Convert ucr into by zip9 and per year
## original data structure is by county and year

import pandas as pd

# Define file paths
data_path = '/home/cwang6/exposome/data/original_data/'
output_path = '/home/cwang6/exposome/data/output_data/temp/'

def read_raw_exposome():
    return pd.read_csv(data_path + 'formatted_ucr.csv')

def read_buffer():
    return pd.read_csv(data_path + 'buffer250tocounty_all.csv')

def translate_exposome(raw_ucr, buffer):
    raw_ucr.rename(columns={'fips': 'GEOID10'}, inplace=True)
    buffer.rename(columns={'fips': 'GEOID10'}, inplace=True)
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    
    df = pd.merge(buffer, raw_ucr, on='GEOID10', how='left')
    
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

def save_exposome(exposome):
    exposome.to_csv(output_path + 'preprocess_ucr.csv', index=False)

def main():
    raw_ucr = read_raw_exposome()
    buffer = read_buffer()
    new_ucr = translate_exposome(raw_ucr, buffer)
    save_exposome(new_ucr)

if __name__ == '__main__':
    main()
