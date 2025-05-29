import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time


def csv_to_db(preprocess_exposome_url, db_url, exposome_type):    
    
    df_exposme = pd.read_csv(preprocess_exposome_url) 
    engine = create_engine(f"sqlite:///{db_url}") 
    df_exposme.to_sql(exposome_type, engine, if_exists='replace', index=False)
    engine.dispose()
    print(f"Data from {preprocess_exposome_url} has been successfully imported into {db_url} in the table {exposome_type}.")

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
        periods.append([row['PATID'], row['ADDRESS_ZIP9'], current.year, days])
        current = year_end + timedelta(days=1)
    return periods

def address_wrangling(df_address, TARGET_START, TARGET_END):
    
    df_address_sub_adjusted = adjust_dates(df_address, TARGET_START, TARGET_END)
    df_address_sub_adjusted = df_address_sub_adjusted[df_address_sub_adjusted['ADDRESS_PERIOD_START'] <= df_address_sub_adjusted['ADDRESS_PERIOD_END']]
    
    all_periods = []
    for _, row in df_address_sub_adjusted.iterrows():
        all_periods.extend(split_periods(row))
    
    return pd.DataFrame(all_periods, columns=['PATID', 'ZIP_9', 'YEAR', 'DAYS'])

def db_linkage(df_address, db_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR):
    engine = create_engine(f"sqlite:///{db_url}") 

    # Read the exposome data from the database
    FIX_VAR = ['ZIP_9', 'YEAR']
    query = f"SELECT {', '.join(SELECT_VAR + FIX_VAR)} FROM {exposome_type}"
    df_exposome = pd.read_sql(query, engine)
    
    # Process the address data
    # Ensure the patient identifier column matches between DataFrames; here we rename PAT to PATID.
    all_periods = address_wrangling(df_address, TARGET_START, TARGET_END)
    df_new = pd.DataFrame(all_periods, columns=['PATID', 'ZIP_9', 'YEAR', 'DAYS'])
    #df_new.loc[:, 'ZIP_9'] = pd.to_numeric(df_new['ZIP_9'])
    
    # Cast to prevent typing issues during comparison
    df_new['ZIP_9'] = df_new['ZIP_9'].astype(str)
    df_exposome['ZIP_9'] = df_exposome['ZIP_9'].astype(str)
    df_new['YEAR'] = df_new['YEAR'].astype(int)
    df_exposome['YEAR'] = df_exposome['YEAR'].astype(int)


    # Perform the join in Python using pandas.merge on the common keys 'ZIP_9' and 'YEAR'
    df_merge = pd.merge(df_new, df_exposome, how='left', on=['ZIP_9', 'YEAR'])


    # Calculate the accumulator for each variable in SELECT_VAR
    for var in SELECT_VAR:
        df_merge[f"{var}_accu"] = df_merge['DAYS'] * df_merge[var]
    
    # Group by PATID and sum the accumulators and DAYS
    df_final = df_merge.groupby('PATID').agg({
        **{f"{var}_accu": 'sum' for var in SELECT_VAR},
        'DAYS': 'sum'
    }).reset_index()
    
    # Compute the weighted average for each variable
    for var in SELECT_VAR:
        df_final[var] = df_final[f"{var}_accu"] / df_final['DAYS']
    
    # Select the desired final columns
    out_var = ['PATID']
    df_final = df_final[out_var + SELECT_VAR]
    
    engine.dispose()
    return df_final



def save_linked_exposome_unique(df, output_url, project_name, exposome_type):

    # Define the output directory and base file name
    output_path = os.path.join(output_url, project_name)
    os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists

    # Base file name
    base_file_name = f'linked_{exposome_type}.csv'
    file_name = base_file_name

    # Initialize counter
    counter = 0
    while os.path.exists(os.path.join(output_path, file_name)):
        counter += 1
        file_name = f'linked_{exposome_type}_{counter}.csv'

    # Construct the full file path
    file_path = os.path.join(output_path, file_name)

    # Save the DataFrame
    df.to_csv(file_path, index=False)
    print(f"File saved as: {file_path}")

    return file_path

def sample_ids(df_address, sample_size):
    
    sub_ID = np.random.choice(df_address['PATID'], sample_size, replace=False)
    return df_address[df_address['PATID'].isin(sub_ID)]


# Main execution
def main(cleaned_lds_url, db_url, output_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR, _project_name):
    # Ececution
    start_time = time.time()  # Start timer to track performance
    
    # preprocess_exposome_url = '/home/cwang6/exposome/data/output_data/temp/preprocess_ucr_sub.csv' #for demo 
    # csv_to_db(preprocess_exposome_url, db_url, exposome_type) #for demo
    
    df_address = pd.read_csv(cleaned_lds_url) 
    print("Converting dates...")
    df_address.loc[:, 'ADDRESS_PERIOD_START'] = pd.to_datetime(df_address['ADDRESS_PERIOD_START'])
    df_address.loc[:, 'ADDRESS_PERIOD_END'] = pd.to_datetime(df_address['ADDRESS_PERIOD_END'])
   
    df_address = sample_ids(df_address, 100)

    print("Performing linkage...")
    start_db_join = time.time()
    df_final = db_linkage(df_address, db_url, exposome_type ,TARGET_START, TARGET_END, SELECT_VAR)
    end_db_join = time.time()
    print(df_final.head())


    print("Performing python based linkage")
    start_py_join = time.time()
    df_final_py = db_linkage_python(df_address, db_url, exposome_type ,TARGET_START, TARGET_END, SELECT_VAR)
    end_py_join = time.time()
    
    save_linked_exposome_unique(df_final, output_url, _project_name, exposome_type)
    
    end_time = time.time()  # Start timer to track performance
    print(f'DB Join linkage: {end_db_join - start_py_join}')
    print(f'Python Join linkage: {end_py_join - start_py_join}')
    print("Runing time:", end_time-start_time) 
    
if __name__ == '__main__':
    
    # pre-set parameter, will fetch from frontend in the future
    project_name = 'test' 
    cleaned_lds_url = '/blue/bianjiang/cwang6/exposome/original/address/HIV_Young_ZH_ldsz9_cleaned.csv'
    db_url = '/blue/bianjiang/cwang6/exposome/output/exposome.db'
    output_url = '/blue/bianjiang/cwang6/exposome/output/'
    TARGET_START = pd.Timestamp('2020-02-21')
    TARGET_END = pd.Timestamp('2022-5-30')
    SELECT_VAR = ['pop', 'murder', 'fso', 'robbery', 'assault', 'burglary'] 
    exposome_type = 'ucr'
    main(cleaned_lds_url, db_url, output_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR)