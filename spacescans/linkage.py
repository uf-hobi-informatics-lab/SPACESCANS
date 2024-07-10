'''

    TO-DO: 
    1. Change the script to query the database for exposome information
    2. remove the hardcoded paths
    3. Add function parameters to determine which exposomes/variables to link against
    4. Remove the hardcoded parameters
        - actually, the function itself takes these all for linkage, this is just for testing
        - still need to clean up to remove the testing values



'''

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, Table, MetaData, select
from sqlalchemy.sql import text


# Macro variables
TARGET_START = pd.Timestamp('2012-05-01')
TARGET_END = pd.Timestamp('2020-12-31')
SELECT_VAR = ['pop', 'murder', 'robbery']  # ucr: ['zip9', 'year', 'pop', 'murder', 'fso', 'robbery', 'assault', 'burglary', 'larceny', 'mvt', 'total', 'p_total', 'p_murder', 'p_fso', 'p_rob', 'p_assault', 'p_burglary', 'p_larceny', 'p_mvt']
SAMPLE_SIZE = 1000

# Change the file path - remove the hardcoded values
EXPOSOME_PATH = '/app/app/spacescans/expo_data/'
DATA_PATH = ''
OUTPUT_PATH = '/app/output/'
FIX_VAR = ['zip9', 'year']  # Default




# Load data - We do not need to load data here - right?
def load_data(file_name):

    # This needs to change - we want to query from the database
    return pd.read_csv(file_name)

# Sample a subset of IDs - this is a testing function?
def sample_ids(df_address, sample_size):
    sub_ID = np.random.choice(df_address['ID'], sample_size, replace=False)
    return df_address[df_address['ID'].isin(sub_ID)]

# Adjust dates based on target start and end dates
def adjust_dates(group, target_start, target_end):
    if group['ADDRESS_PERIOD_START'].min() <= target_start and group['ADDRESS_PERIOD_END'].max() >= target_end:
        group.loc[:, 'ADDRESS_PERIOD_START'] = group['ADDRESS_PERIOD_START'].apply(lambda x: max(x, target_start))
        group.loc[:, 'ADDRESS_PERIOD_END'] = group['ADDRESS_PERIOD_END'].apply(lambda x: min(x, target_end))
    return group

def query_by_date_and_zip9(database_url, table_name, date_value, zip9_value):
    # Create an engine
    engine = create_engine(database_url)
    
    # Reflect the table
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]
    
    # Create a select statement
    stmt = select(table).where(text("year = :date_value and zip9 = :zip9_value"))
    
    # Execute the statement
    with engine.connect() as connection:
        result = connection.execute(stmt, {'date_value': date_value, 'zip9_value': zip9_value})
        
        # Fetch all results
        rows = result.fetchall()
        
        # Print the results
        for row in rows:
            print(row)


# Split periods into yearly chunks
def split_periods(row):
    start = row['ADDRESS_PERIOD_START']
    end = row['ADDRESS_PERIOD_END']
    periods = []
    
    current = start
    while current <= end:
        year_end = min(datetime(current.year, 12, 31), end)
        days = (year_end - current).days + 1
        periods.append([row['PATID'], row['ADDRESS_ZIP9'], current.year, days])
        current = year_end + timedelta(days=1)
    
    return periods

# Main function to process the data
def process_data(target_start, target_end, select_var, geoid, file_name: str):
    target_start = pd.Timestamp(target_start)
    target_end = pd.Timestamp(target_end)

    db_path = os.path.join(os.getcwd(), 'database', 'db_files', 'zip9_exposomes.db')
    db_url = f'sqlite:////{db_path}'

    # Load data once
    print("Loading data...")
    df_address = pd.read_csv(file_name)
    print("Loaded address data.")
    df_exposme = pd.read_csv(EXPOSOME_PATH + 'preprocess_ucr.csv')
    print("Loaded exposome data.")

    #df_address_sub = sample_ids(df_address, sample_size)
    df_exposme_sub = df_exposme[select_var['UCR']+ FIX_VAR]
    
    # Convert date columns to datetime format
    print("Converting dates...)")
    df_address.loc[:, 'ADDRESS_PERIOD_START'] = pd.to_datetime(df_address['ADDRESS_PERIOD_START'])
    df_address.loc[:, 'ADDRESS_PERIOD_END'] = pd.to_datetime(df_address['ADDRESS_PERIOD_END'])
    
    # Adjust dates
    df_address_adjusted = df_address.groupby('PATID').apply(adjust_dates, target_start, target_end).reset_index(drop=True)
    df_address_adjusted = df_address_adjusted[df_address_adjusted['ADDRESS_PERIOD_START'] <= df_address_adjusted['ADDRESS_PERIOD_END']]
    
    # Split periods into yearly chunks
    all_periods = []
    for index, row in df_address_adjusted.iterrows():
        all_periods.extend(split_periods(row))
    
    # Create a new DataFrame
    df_new = pd.DataFrame(all_periods, columns=['PATID', 'zip9', 'year', 'DAYS'])
    
    # Merge with exposure data
    print("Merging with exposure data...")
    merge_addr_expo = pd.merge(df_new, df_exposme_sub, on=['zip9', 'year'], how='left')
    for var in select_var['UCR']:
        merge_addr_expo[f"{var}_accu"] = merge_addr_expo['DAYS'] * merge_addr_expo[var]
    
    df_final = merge_addr_expo.groupby('PATID').agg({**{f"{var}_accu": 'sum' for var in select_var['UCR']}, 'DAYS': 'sum'}).reset_index()
    
    for var in select_var['UCR']:
        df_final[var] = df_final[f"{var}_accu"] / df_final['DAYS']
    
    out_var = ['PATID']
    df_final = df_final[out_var + select_var['UCR']]
    
    print("Saving...")
    df_final.to_csv(OUTPUT_PATH + 'output.csv', index=False)

# Run the processing function
#df_final = process_data(TARGET_START, TARGET_END, SAMPLE_SIZE, SELECT_VAR)
#df_final.info()