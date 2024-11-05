import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time

def csv_to_db(csv_file, db_file, table_name):
    OUTPUT_PATH = '' # replace the dir
    df_exposme = pd.read_csv(f'{OUTPUT_PATH}{csv_file}')
    engine = create_engine(f'sqlite:///{OUTPUT_PATH}{db_file}')
    df_exposme.to_sql(table_name, engine, if_exists='replace', index=False)
    engine.dispose()
    print(f"Data from {csv_file} has been successfully imported into {db_file} in the table {table_name}.")

def sample_ids(df_address, sample_size):
    sub_ID = np.random.choice(df_address['ID'], sample_size, replace=False)
    return df_address[df_address['ID'].isin(sub_ID)]

# Adjust address periods
def adjust_dates(df_address, target_start, target_end):
    df_address['ADDRESS_PERIOD_START'] = pd.to_datetime(df_address['ADDRESS_PERIOD_START'], errors='coerce')
    df_address['ADDRESS_PERIOD_END'] = pd.to_datetime(df_address['ADDRESS_PERIOD_END'], errors='coerce')
    df_address['ADDRESS_PERIOD_START'] = df_address['ADDRESS_PERIOD_START'].apply(lambda x: max(x, target_start))
    df_address['ADDRESS_PERIOD_END'] = df_address['ADDRESS_PERIOD_END'].apply(lambda x: min(x, target_end))
    return df_address[df_address['ADDRESS_PERIOD_START'] <= df_address['ADDRESS_PERIOD_END']]

def split_periods(row):
    start = row['ADDRESS_PERIOD_START']
    end = row['ADDRESS_PERIOD_END']
    periods = []
    current = start
    while current <= end:
        year_end = min(datetime(current.year, 12, 31), end)
        days = (year_end - current).days + 1
        periods.append([row['ID'], row['ADDRESS_ZIP9'], current.year, days])
        current = year_end + timedelta(days=1)
    return periods

def address_wrangling(df_address, TARGET_START, TARGET_END):
    df_address_sub_adjusted = adjust_dates(df_address, TARGET_START, TARGET_END)
    df_address_sub_adjusted = df_address_sub_adjusted[df_address_sub_adjusted['ADDRESS_PERIOD_START'] <= df_address_sub_adjusted['ADDRESS_PERIOD_END']]
    
    all_periods = []
    for _, row in df_address_sub_adjusted.iterrows():
        all_periods.extend(split_periods(row))
    
    return pd.DataFrame(all_periods, columns=['ID', 'zip9', 'year', 'DAYS'])

def exposome_linkage(df_address, TARGET_START, TARGET_END, SELECT_VAR):
    OUTPUT_PATH = '' # replace the dir
    engine = create_engine(f'sqlite:///{OUTPUT_PATH}exposome.db')
    FIX_VAR = ['zip9', 'year']
    query = f"SELECT {', '.join(SELECT_VAR + FIX_VAR)} FROM ucr"
    df_exposme_sub = pd.read_sql(query, engine)
    
    all_periods = address_wrangling(df_address, TARGET_START, TARGET_END)
    df_new = pd.DataFrame(all_periods, columns=['ID', 'zip9', 'year', 'DAYS'])
    df_new.to_sql('df_new_temp', engine, if_exists='replace', index=False)
    
    query = f"""
        SELECT
            df_new_temp.ID,
            df_new_temp.zip9,
            df_new_temp.year,
            df_new_temp.DAYS,
            {', '.join([f'ucr.{var}' for var in SELECT_VAR])}
        FROM
            df_new_temp
        LEFT JOIN
            ucr
        ON
            df_new_temp.zip9 = ucr.zip9 AND df_new_temp.year = ucr.year
    """
    merge_addr_expo = pd.read_sql(query, engine)
    
    
    for var in SELECT_VAR:
        merge_addr_expo[f"{var}_accu"] = merge_addr_expo['DAYS'] * merge_addr_expo[var]
    
    df_final = merge_addr_expo.groupby('ID').agg({**{f"{var}_accu": 'sum' for var in SELECT_VAR}, 'DAYS': 'sum'}).reset_index()
    
    for var in SELECT_VAR:
        df_final[var] = df_final[f"{var}_accu"] / df_final['DAYS']
    
    out_var = ['ID']
    df_final = df_final[out_var + SELECT_VAR]
    
    engine.dispose()
    return df_final

def main():
    # pre-set parameter
    TARGET_START = pd.Timestamp('2020-02-21')
    TARGET_END = pd.Timestamp('2022-5-30')
    SELECT_VAR = ['pop', 'murder', 'fso', 'robbery', 'assault', 'burglary']    
    ADDR_PATH = '/home/cwang6/exposome/data/original_data/address/yoon_ldsz9f1519.csv'
   
    # Ececution
    start_time = time.time()  # Start timer to track performance
    
    DB_FILE = 'exposome.db'
    csv_to_db('preprocess_ucr_sub.csv', DB_FILE, 'ucr') # Delete the codes if the .db exists.
    
    df_address = pd.read_csv(ADDR_PATH) 
    df_address = sample_ids(df_address, 100) #Just subselect for quick check

    df_final = exposome_linkage(df_address, TARGET_START, TARGET_END, SELECT_VAR)
    print(df_final.head())
    
    end_time = time.time()  # Start timer to track performance
    print("Runing time:", end_time-start_time) 
    
if __name__ == '__main__':
    main()
