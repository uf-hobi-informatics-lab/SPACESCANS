from itertools import product
import pandas as pd

# Define file paths
data_path = '/home/cwang6/exposome/data/original_data/'
output_path = '/home/cwang6/exposome/data/output_data/temp/'

def read_raw_exposome():
    """
    Reads the raw exposome data from a CSV file.
    """
    raw_caces = pd.read_csv(data_path + 'formatted_caces.csv')
    return raw_caces

def read_buffer():
    """
    Reads the buffer data from a CSV file.
    """
    buffer = pd.read_csv(data_path + 'buffer250tobg_all.csv')
    return buffer

def translate_exposome(raw_caces, buffer):
    """
    Translates the raw exposome data to include area-weighted averages by merging with buffer data.
    
    Parameters:
    raw_caces (DataFrame): The raw exposome data.
    buffer (DataFrame): The buffer data.
    
    Returns:
    DataFrame: The processed exposome data with area-weighted averages.
    """
    # Select relevant columns from buffer and rename for merging
    buffer = buffer[['zip9', 'GEOID10', 'value']]
    raw_caces.rename(columns={'fips': 'GEOID10'}, inplace=True)
    
    # Merge datasets on 'GEOID10'
    df = pd.merge(buffer, raw_caces, on='GEOID10', how='left')
    
    # Identify columns to process
    exclude_columns = ['zip9', 'GEOID10', 'value', 'year']
    columns_expo = [col for col in df.columns if col not in exclude_columns]
    
    # Calculate area-weighted variables
    for var in columns_expo:
        df[var + '_weighted'] = df['value'] * df[var]

    # Group by 'zip9' and 'year', summing weighted variables and values
    grouped = df.groupby(['zip9', 'year']).agg({var + '_weighted': 'sum' for var in columns_expo})
    grouped['value_sum'] = df.groupby(['zip9', 'year'])['value'].sum()

    # Calculate area-weighted averages with original names
    for var in columns_expo:
        grouped[var] = grouped[var + '_weighted'] / grouped['value_sum']

    # Select and reset index for the final DataFrame
    df_result = grouped[columns_expo].reset_index()
    return df_result

def save_exposome(caces):
    """
    Saves the processed exposome data to a CSV file.
    
    Parameters:
    caces (DataFrame): The processed exposome data.
    """
    caces.to_csv(output_path + 'preprocess_caces.csv', index=False)

def main():
    """
    Main function to orchestrate the workflow.
    """
    raw_caces = read_raw_exposome()
    buffer = read_buffer()
    new_caces = translate_exposome(raw_caces, buffer)
    save_exposome(new_caces)

if __name__== '__main__':
    main()
