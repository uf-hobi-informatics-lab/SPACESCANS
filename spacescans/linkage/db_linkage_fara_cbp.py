import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time

def rename_exposome(df):
    df_new = df.rename(columns={'zip9': 'ZIP_9', 'year': 'YEAR'})
    return df_new

def csv_to_db(preprocess_exposome_url, db_url, exposome_type):    
    
    df_exposme = pd.read_csv(preprocess_exposome_url) # preprocess_exposome_url = '/home/cwang6/exposome/data/output_data/temp/preprocess_ucr_sub.csv'
    #df_exposme = rename_exposome(df_exposme) #for demo
    engine = create_engine(f"sqlite:///{db_url}") # db_url = '/blue/bianjiang/cwang6/exposome/output/exposome.db'
    df_exposme.to_sql(exposome_type, engine, if_exists='replace', index=False)
    engine.dispose()
    print(f"Data from {preprocess_exposome_url} has been successfully imported into {db_url} in the table {exposome_type}.")

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
    
    return pd.DataFrame(all_periods, columns=['ID', 'ZIP_9', 'YEAR', 'DAYS'])

def db_linkage(df_address, db_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR):
    
    engine = create_engine(f"sqlite:///{db_url}")  #db_url = '/blue/bianjiang/cwang6/exposome/output/exposome.db'
    FIX_VAR = ['ZIP_9', 'YEAR']
    query = f"SELECT {', '.join(SELECT_VAR + FIX_VAR)} FROM {exposome_type}"
    df_exposme_sub = pd.read_sql(query, engine)
    
    all_periods = address_wrangling(df_address, TARGET_START, TARGET_END)
    df_new = pd.DataFrame(all_periods, columns=['ID', 'ZIP_9', 'YEAR', 'DAYS'])
    df_new.to_sql('df_new_temp', engine, if_exists='replace', index=False)
    
    query = f"""
        SELECT
            df_new_temp.ID,
            df_new_temp.ZIP_9,
            df_new_temp.YEAR,
            df_new_temp.DAYS,
            {', '.join([f'{exposome_type}.{var}' for var in SELECT_VAR])}
        FROM
            df_new_temp
        LEFT JOIN
            {exposome_type}
        ON
            df_new_temp.ZIP_9 = {exposome_type}.ZIP_9 AND df_new_temp.YEAR = {exposome_type}.YEAR
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


# Main execution
def main(cleaned_lds_url, db_url, output_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR):
    # Ececution
    start_time = time.time()  # Start timer to track performance
    
    #preprocess_exposome_url = '/home/cwang6/exposome/data/output_data/temp/preprocess_ucr_sub.csv' #for demo
    preprocess_exposome_url = '/blue/bianjiang/cwang6/exposome/original/spacescan/preprocess_fara_zip9.csv' #for demo
    
    csv_to_db(preprocess_exposome_url, db_url, exposome_type) #for demo
    
    df_address = pd.read_csv(cleaned_lds_url) 
    df_address = sample_ids(df_address, 100) #for demo, Just subselect for quick check

    
    df_final = db_linkage(df_address, db_url, exposome_type ,TARGET_START, TARGET_END, SELECT_VAR)
    print(df_final.head())
    
    save_linked_exposome_unique(df_final, output_url, project_name, exposome_type)
    
    end_time = time.time()  # Start timer to track performance
    print("Runing time:", end_time-start_time) 
    
if __name__ == '__main__':
    # pre-set parameter, will fetch from frontend in the future
    project_name = 'test' 
    cleaned_lds_url = '/blue/bianjiang/cwang6/exposome/original/address/HIV_Young_ZH_ldsz9_cleaned.csv'
    db_url = '/blue/bianjiang/cwang6/exposome/output/exposome.db'
    output_url = '/blue/bianjiang/cwang6/exposome/data/output/'
    TARGET_START = pd.Timestamp('2020-02-21')
    TARGET_END = pd.Timestamp('2022-5-30')
    #SELECT_VAR = ['pop', 'murder', 'fso', 'robbery', 'assault', 'burglary'] 
    SELECT_VAR = ['LA1AND20', 'LASENIORS20', 'LASENIORSHALFSHARE', 'LAHISP20SHARE', 'LASNAP20SHARE', 'LALOWI05_10', 'LATRACTS10', 'LAKIDS10SHARE', 'LAPOP1SHARE', 'LATRACTS_HALF', 'LAHUNV1SHARE', 'LAAIAN20SHARE', 'LASNAPHALFSHARE', 'LABLACKHALFSHARE', 'LASNAP1SHARE', 'TRACTAIAN', 'LAPOPHALFSHARE', 'TRACTASIAN', 'LAHISP1SHARE', 'LABLACK20SHARE', 'LANHOPI1', 'LATRACTS1', 'LAAIAN10', 'LAHISP20', 'TRACTNHOPI', 'LATRACTSVEHICLE_20', 'LASNAP10', 'LAPOP10SHARE', 'LAAIAN1', 'TRACTHISPANIC', 'LAHISP10', 'TRACTLOWI', 'LAWHITEHALF', 'LAHUNV20', 'LAPOP1_10', 'URBAN', 'LAPOP1_20', 'LAWHITE10', 'LAASIAN10SHARE', 'LALOWI20SHARE', 'LAWHITE20SHARE', 'LAOMULTIR1', 'LABLACK1SHARE', 'LANHOPI10', 'LAHISP1', 'LAPOP05_10', 'LASENIORS1', 'LAASIAN20', 'PCTGQTRS', 'LAOMULTIR10SHARE', 'LAKIDS10', 'LAPOPHALF', 'LALOWIHALFSHARE', 'LAAIAN10SHARE', 'LAHISPHALFSHARE', 'LAWHITEHALFSHARE', 'LALOWI20', 'LALOWI10SHARE', 'LANHOPIHALFSHARE', 'LAHUNV1', 'LAHUNVHALF', 'LAOMULTIR20SHARE', 'LABLACK10SHARE', 'LAWHITE1SHARE', 'LAKIDSHALFSHARE', 'LAASIANHALFSHARE', 'LAPOP1', 'LAASIAN10', 'LASENIORSHALF', 'POVERTYRATE', 'LALOWI1_20', 'MEDIANFAMILYINCOME', 'LALOWI1_10', 'OHU2010', 'LABLACK1', 'TRACTSNAP', 'LA1AND10', 'LALOWIHALF', 'LAHISPHALF', 'GROUPQUARTERSFLAG', 'LASNAPHALF', 'LAAIAN20', 'LALOWI1', 'LAHALFAND10', 'LANHOPI20', 'LASENIORS1SHARE', 'LAOMULTIR10', 'LAPOP10', 'LAKIDSHALF', 'LILATRACTS_VEHICLE', 'LAPOP20SHARE', 'LILATRACTS_1AND10', 'LANHOPIHALF', 'LAKIDS20SHARE', 'LANHOPI1SHARE', 'LAHUNV20SHARE', 'LAASIAN20SHARE', 'LAHUNV10SHARE', 'LAOMULTIRHALF', 'LAHUNVHALFSHARE', 'LANHOPI10SHARE', 'TRACTKIDS', 'TRACTBLACK', 'LASENIORS10', 'LASENIORS10SHARE', 'LILATRACTS_1AND20', 'TRACTWHITE', 'LAWHITE10SHARE', 'LILATRACTS_HALFAND10', 'LAKIDS1', 'LASNAP1', 'LAASIAN1SHARE', 'LAHUNV10', 'LALOWI1SHARE', 'LALOWI10', 'TRACTOMULTIR', 'LASNAP20', 'LATRACTS20', 'LAAIAN1SHARE', 'TRACTHUNV', 'LAOMULTIR20', 'LAAIANHALFSHARE', 'LOWINCOMETRACTS', 'LAASIAN1', 'NUMGQTRS', 'LAPOP20', 'LAOMULTIR1SHARE', 'LAAIANHALF', 'LAOMULTIRHALFSHARE', 'LAKIDS1SHARE', 'LABLACK10', 'LAKIDS20', 'LASENIORS20SHARE', 'LAHISP10SHARE', 'LAWHITE1', 'TRACTSENIORS', 'HUNVFLAG', 'LANHOPI20SHARE', 'LASNAP10SHARE', 'LAASIANHALF', 'LAWHITE20', 'LABLACK20']
    #SELECT_VAR = ['AMS_RES'] exposome_type = 'hud'
    exposome_type = 'fara'
    main(cleaned_lds_url, db_url, output_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR)