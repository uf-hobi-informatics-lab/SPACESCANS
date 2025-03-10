import os
import time
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor


def csv_linkage(df_address, exposome_dir, exposome_type, TARGET_START, TARGET_END, SELECT_VAR):

    exposome_type = exposome_type.upper()

    # Determine time period type from temporal_dictionary
    period_type = temporal_dictionary.get(exposome_type, "YEAR")  # Default to YEAR if missing
    
    # Adjust FIX_VAR dynamically based on time granularity
    time_var_mapping = {
        "YEAR": ["YEAR"],
        "MONTH": ["YEAR", "MONTH"],
        "QUARTER": ["YEAR", "QUARTER"]
    }
    FIX_VAR = ["ZIP_9"] + time_var_mapping.get(period_type, [])

    # Load .csv formated exposome data 
    df_exposome_sub = pd.read_csv(exposome_dir)
    print("Loaded exposome data.")
    
    # Convert time columns to string for safe merging
    df_exposome_sub[FIX_VAR] = df_exposome_sub[FIX_VAR].astype(str)

    # Process address periods
    df_new = _address_wrangling(df_address, TARGET_START, TARGET_END, exposome_type)

    # Convert time columns to string for merging
    df_new[FIX_VAR] = df_new[FIX_VAR].astype(str)

    # Perform in-memory merge based on ZIP_9 and the corresponding time variables
    merge_addr_expo = df_new.merge(df_exposome_sub, on=FIX_VAR, how='left')

    # Compute time-weighted averages
    if SELECT_VAR is None:
        SELECT_VAR = df_exposome_sub.columns.drop(FIX_VAR)

    df_final = _time_weighted_averages(merge_addr_expo, SELECT_VAR)

    return df_final




def db_linkage(df_address, db_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR):

    engine = create_engine(f"sqlite:///{db_url}") 
    exposome_type = exposome_type.upper()

    # Determine time period type from temporal_dictionary
    period_type = temporal_dictionary.get(exposome_type, "YEAR")  # Default to YEAR if missing
    
    # Adjust FIX_VAR dynamically based on time granularity
    time_var_mapping = {
        "YEAR": ["YEAR"],
        "MONTH": ["YEAR", "MONTH"],
        "QUARTER": ["YEAR", "QUARTER"]
    }
    FIX_VAR = ["ZIP_9"] + time_var_mapping[period_type]

    # Execute SQL query
    query = f"SELECT {', '.join(SELECT_VAR + FIX_VAR)} FROM {exposome_type}"
    df_exposome_sub = pd.read_sql(query, engine)
    
    # Convert time columns to string for safe merging
    df_exposome_sub[FIX_VAR] = df_exposome_sub[FIX_VAR].astype(str)

    # Process address periods
    df_new = _address_wrangling(df_address, TARGET_START, TARGET_END, exposome_type)

    # Convert time columns to string for merging
    df_new[FIX_VAR] = df_new[FIX_VAR].astype(str)

    # Perform in-memory merge based on ZIP_9 and the corresponding time variables
    merge_addr_expo = df_new.merge(df_exposome_sub, on=FIX_VAR, how='left')

    # Compute time-weighted averages
    if SELECT_VAR is None:
        SELECT_VAR = df_exposome_sub.columns.drop(FIX_VAR)
    df_final = _time_weighted_averages(merge_addr_expo, SELECT_VAR)

    engine.dispose()
    return df_final

def parallel_db_linkage(groups, db_url, df_address, TARGET_START, TARGET_END):

    def process_group(group):
        exposome_type = group["exposome_type"]
        SELECT_VAR = group["SELECT_VAR"]
        return exposome_type, db_linkage(df_address, db_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR)

    results = {}
    with ThreadPoolExecutor(max_workers=len(groups)) as executor:
        futures = {executor.submit(process_group, group): group["exposome_type"] for group in groups}
        for future in futures:
            exposome_type, result = future.result()
            results[exposome_type] = result

    return results

def parallel_linkage_from_json(json_file, db_url, df_address, TARGET_START, TARGET_END):
    """
    Loads linkage groups from a JSON file and runs parallel db_linkage.
    """
    # Load JSON input
    with open(json_file, "r") as f:
        linkage_groups = json.load(f)

    # Run parallel linkage
    results = parallel_db_linkage(linkage_groups, db_url, df_address, TARGET_START, TARGET_END)

    # View results
    for exposome, df in results.items():
        print(f"{exposome} Linked Data:")
        print(df.head())

    return results


temporal_dictionary = {
    "NATA": "YEAR",
    "CACES": "YEAR",
    "WI": "YEAR",
    "FARA": "YEAR",
    "ACS": "YEAR",
    "UCR": "YEAR",
    "ZBP": "YEAR",
    "CBP": "YEAR",
    "NDI": "YEAR",
    "ACAG": "MONTH",
    "HUD": "QUARTER",
}

def csv_to_db(preprocess_exposome_url, db_url, exposome_type):    
    
    df_exposme = pd.read_csv(preprocess_exposome_url) 
    engine = create_engine(f"sqlite:///{db_url}") 
    df_exposme.to_sql(exposome_type, engine, if_exists='replace', index=False)
    engine.dispose()
    print(f"Data from {preprocess_exposome_url} has been successfully imported into {db_url} in the table {exposome_type}.")

# Adjust address periods
def _adjust_dates(df_address, target_start, target_end):
    df_address['ADDRESS_PERIOD_START'] = pd.to_datetime(df_address['ADDRESS_PERIOD_START'])
    df_address['ADDRESS_PERIOD_END'] = pd.to_datetime(df_address['ADDRESS_PERIOD_END'])
    df_address['ADDRESS_PERIOD_START'] = df_address['ADDRESS_PERIOD_START'].apply(lambda x: max(x, target_start))
    df_address['ADDRESS_PERIOD_END'] = df_address['ADDRESS_PERIOD_END'].apply(lambda x: min(x, target_end))
    return df_address[df_address['ADDRESS_PERIOD_START'] <= df_address['ADDRESS_PERIOD_END']]


# Helper functions to split periods by year, month, quarter
def _split_periods_by_year(row):
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

def _split_periods_by_month(row):
    start = row['ADDRESS_PERIOD_START']
    end = row['ADDRESS_PERIOD_END']
    periods = []
    current = start
    while current <= end:
        month_end = min(datetime(current.year, current.month, calendar.monthrange(current.year, current.month)[1]), end)
        days = (month_end - current).days + 1
        periods.append([row['PATID'], row['ADDRESS_ZIP9'], current.year, current.month, days])
        current = month_end + timedelta(days=1)
    return periods

def _split_periods_by_quarter(row):
    start = row['ADDRESS_PERIOD_START']
    end = row['ADDRESS_PERIOD_END']
    periods = []
    current = start
    while current <= end:
        quarter_end_month = ((current.month - 1) // 3 + 1) * 3
        quarter_end = min(datetime(current.year, quarter_end_month, calendar.monthrange(current.year, quarter_end_month)[1]), end)
        days = (quarter_end - current).days + 1
        quarter = (current.month - 1) // 3 + 1
        periods.append([row['PATID'], row['ADDRESS_ZIP9'], current.year, quarter, days])
        current = quarter_end + timedelta(days=1)
    return periods

# Split periods by time (YEAR, MONTH, QUARTER)
def _split_periods(df_address, exposome_type):
    period_type = temporal_dictionary.get(exposome_type.upper())

    period_functions = {
        'YEAR': _split_periods_by_year,
        'MONTH': _split_periods_by_month,
        'QUARTER': _split_periods_by_quarter
    }

    if period_type not in period_functions:
        raise ValueError('Invalid time period. Choose "YEAR", "MONTH", or "QUARTER".')

    return [period for _, row in df_address.iterrows() for period in period_functions[period_type](row)]

            
# Address wrangling function
def _address_wrangling(df_address, TARGET_START, TARGET_END, exposome_type):
    exposome_type = exposome_type.upper()

    # Adjust dates first
    df_address_sub_adjusted = _adjust_dates(df_address, TARGET_START, TARGET_END)

    # Split periods
    periods = _split_periods(df_address_sub_adjusted, exposome_type)

    # Determine period type from temporal_dictionary
    period_type = temporal_dictionary.get(exposome_type, "YEAR")  # Default to YEAR

    # Define column structure based on period type
    period_columns = {
        "YEAR": ["PATID", "ZIP_9", "YEAR", "DAYS"],
        "MONTH": ["PATID", "ZIP_9", "YEAR", "MONTH", "DAYS"],
        "QUARTER": ["PATID", "ZIP_9", "YEAR", "QUARTER", "DAYS"]
    }

    # Validate that the number of returned columns matches the expected column structure
    expected_columns = period_columns[period_type]

    # Ensure _split_periods returns correctly formatted data
    if periods and len(periods[0]) != len(expected_columns):
        raise ValueError(f"Expected {len(expected_columns)} columns but received {len(periods[0])}. "
                         f"Check _split_periods output for {exposome_type}.")

    # Create DataFrame
    df_periods = pd.DataFrame(periods, columns=expected_columns)

    return df_periods

def _time_weighted_averages(df, selected_vars):

    for var in selected_vars:
        df[f"{var}_accu"] = df['DAYS'] * df[var]
    
    # Aggregate by 'PATID'
    df_final = df.groupby('PATID').agg(
        {**{f"{var}_accu": 'sum' for var in selected_vars}, 'DAYS': 'sum'}
    ).reset_index()

    # Compute weighted averages
    for var in selected_vars:
        df_final[var] = df_final[f"{var}_accu"] / df_final['DAYS']

    # Drop intermediate accumulation columns
    df_final.drop(columns=[f"{var}_accu" for var in selected_vars], inplace=True)

    return df_final[['PATID'] + selected_vars]

def _save_linked_exposome_unique(df, output_url, project_name, exposome_type):

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
    
    # preprocess_exposome_url = '/home/cwang6/exposome/data/output_data/temp/preprocess_ucr_sub.csv' #for demo
      
    # csv_to_db(preprocess_exposome_url, db_url, exposome_type) #for demo
    
    df_address = pd.read_csv(cleaned_lds_url) 
    df_address = df_address.head(10000) # For demo
    df_address.rename(columns = {'ID':'PATID'}, inplace = True)

   
    df_final = db_linkage(df_address, db_url, exposome_type ,TARGET_START, TARGET_END, SELECT_VAR)
    print(df_final.head())
    
    _save_linked_exposome_unique(df_final, output_url, project_name, exposome_type)
    
    end_time = time.time()  # Start timer to track performance
    print("Runing time:", end_time-start_time) 
    
if __name__ == '__main__':
    
    # pre-set parameter, will fetch from frontend in the future
    project_name = 'test' 
    cleaned_lds_url = '/home/cwang6/data/original/HIV_Young_ZH_ldsz9_cleaned.csv'
    db_url = '/data/exposome_db/zip9_exposomes.db'
    output_url = '/home/cwang6/data/output/'
    TARGET_START = pd.Timestamp('2012-02-21')
    TARGET_END = pd.Timestamp('2023-5-30')
    SELECT_VAR = ['WALKABILITY'] 
    exposome_type = 'WI'
    main(cleaned_lds_url, db_url, output_url, exposome_type, TARGET_START, TARGET_END, SELECT_VAR)