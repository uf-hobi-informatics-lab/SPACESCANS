'''
    To link diverse of exposome data analysis ready to individuals in zip+4 level 
'''
import pandas as pd
from datetime import datetime, timedelta
import calendar
import concurrent.futures
from tqdm import tqdm  
import time

# File paths and other global variables
ADDR_PATH = '/home/cwang6/exposome/data/original_data/address/lds_test.csv'  #Change this path
EXPOSOME_PATH = '/home/cwang6/exposome/data/output_data/temp/'               #Change this path

# Load exposome datasets 
df_hud   = pd.read_csv('../df_hud.csv')   #Change this path
df_nata  = pd.read_csv('../df_nata.csv')  #Change this path
df_fara  = pd.read_csv('../df_fara.csv')  #Change this path
df_caces = pd.read_csv('../df_caces.csv') #Change this path
df_ucr   = pd.read_csv('../df_ucr.csv')   #Change this path

df_acs, df_acag, df_cbp, df_zbp, df_wi = [], [], [], [] # Remain to be verified

# Variable sets categorized by exposome type
acag_vars = {'BC', 'NH4', 'NIT', 'OM', 'SO4', 'SOIL', 'SS'}
caces_vars = {'O3', 'CO', 'SO2', 'NO2', 'PM10', 'PM25'}
ucr_vars = {'p_assault', 'p_burglary', 'p_fso', 'p_larceny', 'p_murder', 'p_mvt', 'p_rob'}
wi_vars = {'WALKABILITY'}
cbp_vars = {'religious', 'civic', 'business', 'political', 'professional', 'labor', 'bowling', 'recreational', 'golf', 'sports'}
acs_vars = {} # Remaind to be done
hud_vars  = {'AMS_RES', 'AMS_BUS', 'AMS_OTH', 'RES_VAC', 'BUS_VAC',
       'OTH_VAC', 'AVG_VAC_R', 'AVG_VAC_B', 'AVG_VAC_O', 'VAC_3_RES',
       'VAC_3_BUS', 'VAC_3_OTH', 'VAC_3_6_R', 'VAC_3_6_B', 'VAC_3_6_O',
       'VAC_6_12R', 'VAC_6_12B', 'VAC_6_12O', 'VAC_12_24R', 'VAC_12_24B',
       'VAC_12_24O', 'VAC_24_36R', 'VAC_24_36B', 'VAC_24_36O', 'VAC_36_RES',
       'VAC_36_BUS', 'VAC_36_OTH', 'PQV_IS_RES', 'PQV_IS_BUS', 'PQV_IS_OTH',
       'PQV_NS_RES', 'PQV_NS_BUS', 'PQV_NS_OTH', 'NOSTAT_RES', 'NOSTAT_BUS',
       'NOSTAT_OTH', 'AVG_NS_RES', 'AVG_NS_BUS', 'AVG_NS_OTH', 'NS_3_RES',
       'NS_3_BUS', 'NS_3_OTH', 'NS_3_6_RES', 'NS_3_6_BUS', 'NS_3_6_OTH',
       'NS_6_12_R', 'NS_6_12_B', 'NS_6_12_O', 'NS_12_24_R', 'NS_12_24_B',
       'NS_12_24_O', 'NS_24_36_R', 'NS_24_36_B', 'NS_24_36_O', 'NS_36_RES',
       'NS_36_BUS', 'NS_36_OTH', 'PQNS_IS_R', 'PQNS_IS_B', 'PQNS_IS_O',
       'NEW_ADDR_R', 'NEW_ADDR_B', 'NEW_ADDR_O', 'DROP_ADD_R', 'DROP_ADD_B',
       'DROP_ADD_O'} # example not complete, remain to be replaced
fara_vars = {'LA1AND20', 'LASENIORS20', 'LASENIORSHALFSHARE',
       'LAHISP20SHARE', 'LASNAP20SHARE', 'LALOWI05_10', 'LATRACTS10',
       'LAHISP10SHARE', 'LAWHITE1', 'TRACTSENIORS', 'HUNVFLAG',
       'LANHOPI20SHARE', 'LASNAP10SHARE', 'LAASIANHALF', 'LAWHITE20',
       'LABLACK20', 'LABLACKHALF'} # example not complete, remain to be replaced
nata_vars = {
    'POPULATION', 'TOTAL CANCER RISK (PER MILLION)',
    '1,1,2-TRICHLOROETHANE', '1,2-DIBROMO-3-CHLOROPROPANE',
    '1,2-DIPHENYLHYDRAZINE', '1,2,3,4,5,6-HEXACHLOROCYCLYHEXANE',
    '1,3-BUTADIENE', '1,3-DICHLOROPROPENE', '1,3-PROPANE SULTONE',
    '1,4-DICHLOROBENZENE', '2-ACETYLAMINOFLUORENE', '2-NITROPROPANE',
    '2,4-DINITROTOLUENE', '2,4-TOLUENE DIISOCYANATE',
    '2,4,6-TRICHLOROPHENOL', "3,3'-DICHLOROBENZIDINE",
    '4-DIMETHYLAMINOAZOBENZENE', "4,4'-METHYLENE BIS(2-CHLOROANILINE)",
    "4,4'-METHYLENEDIANILINE", 'ACETALDEHYDE', 'ACETAMIDE', 'ACRYLAMIDE',
    'ACRYLONITRILE', 'ALLYL CHLORIDE', 'ANILINE',
    'ARSENIC COMPOUNDS(INORGANIC INCLUDING ARSINE)', 'BENZENE', 'BENZIDINE',
    'BENZYL CHLORIDE', 'BERYLLIUM COMPOUNDS',
    'BIS(2-ETHYLHEXYL)PHTHALATE (DEHP)', 'BIS(CHLOROMETHYL) ETHER',
    'BROMOFORM', 'CHROMIUM VI (HEXAVALENT)', 'CADMIUM COMPOUNDS',
    'CARBON TETRACHLORIDE', 'CHLORDANE', 'CHLOROBENZILATE', 'CHLOROPRENE',
    'COKE OVEN EMISSIONS', 'DICHLOROETHYL ETHER (BIS[2-CHLOROETHYL]ETHER)',
    'EPICHLOROHYDRIN', 'ETHYLBENZENE',
    'ETHYL CARBAMATE (URETHANE) CHLORIDE (CHLOROETHANE)',
    'ETHYLENE DIBROMIDE (DIBROMOETHANE)',
    'ETHYLENE DICHLORIDE (1,2-DICHLOROETHANE)', 'ETHYLENE OXIDE',
    'ETHYLENE THIOUREA', 'ETHYLIDENE DICHLORIDE (1,1-DICHLOROETHANE)',
    'FORMALDEHYDE', 'HEPTACHLOR', 'HEXACHLOROBENZENE',
    'HEXACHLOROBUTADIENE', 'HYDRAZINE', 'METHYL TERT-BUTYL ETHER',
    'METHYLENE CHLORIDE', 'N-NITROSODIMETHYLAMINE', 'N-NITROSOMORPHOLINE',
    'NICKEL COMPOUNDS', 'NAPHTHALENE', 'NITROBENZENE',
    'POLYCHLORINATED BIPHENYLS (AROCLORS)', 'PENTACHLOROPHENOL',
    'PROPYLENE OXIDE', 'TETRACHLOROETHYLENE', '2,4-TOLUENE DIAMINE',
    'TOXAPHENE (CHLORINATED CAMPHENE)', 'TRICHLOROETHYLENE',
    'VINYL BROMIDE', 'VINYL CHLORIDE', 'O-TOLUIDINE', '1,4-DIOXANE',
    'PAHPOM'
}


# Adjust address periods
def adjust_dates(df_address, target_start, target_end):
    df_address['ADDRESS_PERIOD_START'] = pd.to_datetime(df_address['ADDRESS_PERIOD_START'])
    df_address['ADDRESS_PERIOD_END'] = pd.to_datetime(df_address['ADDRESS_PERIOD_END'])
    df_address['ADDRESS_PERIOD_START'] = df_address['ADDRESS_PERIOD_START'].apply(lambda x: max(x, target_start))
    df_address['ADDRESS_PERIOD_END'] = df_address['ADDRESS_PERIOD_END'].apply(lambda x: min(x, target_end))
    return df_address[df_address['ADDRESS_PERIOD_START'] <= df_address['ADDRESS_PERIOD_END']]

# Helper functions to split periods by year, month, quarter
def split_periods_by_year(row):
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

def split_periods_by_month(row):
    start = row['ADDRESS_PERIOD_START']
    end = row['ADDRESS_PERIOD_END']
    periods = []
    current = start
    while current <= end:
        month_end = min(datetime(current.year, current.month, calendar.monthrange(current.year, current.month)[1]), end)
        days = (month_end - current).days + 1
        periods.append([row['ID'], row['ADDRESS_ZIP9'], current.year, current.month, days])
        current = month_end + timedelta(days=1)
    return periods

def split_periods_by_quarter(row):
    start = row['ADDRESS_PERIOD_START']
    end = row['ADDRESS_PERIOD_END']
    periods = []
    current = start
    while current <= end:
        quarter_end_month = ((current.month - 1) // 3 + 1) * 3
        quarter_end = min(datetime(current.year, quarter_end_month, calendar.monthrange(current.year, quarter_end_month)[1]), end)
        days = (quarter_end - current).days + 1
        quarter = (current.month - 1) // 3 + 1
        periods.append([row['ID'], row['ADDRESS_ZIP9'], current.year, quarter, days])
        current = quarter_end + timedelta(days=1)
    return periods

# Split periods by time (YEAR, MONTH, QUARTER)
def split_periods(df_address, time):
    if time == 'YEAR':
        all_periods = []
        for _, row in df_address.iterrows():
            all_periods.extend(split_periods_by_year(row))
        return all_periods
    elif time == 'MONTH':
        all_periods = []
        for _, row in df_address.iterrows():
            all_periods.extend(split_periods_by_month(row))
        return all_periods
    elif time == 'QUARTER':
        all_periods = []
        for _, row in df_address.iterrows():
            all_periods.extend(split_periods_by_quarter(row))
        return all_periods
    else:
        raise ValueError('Invalid time period. Choose "YEAR", "MONTH", or "QUARTER".')

# Helper function to calculate time-weighted averages
def time_weighted_averages(df, selected_vars):
    for var in selected_vars:
        df[f"{var}_accu"] = df['DAYS'] * df[var]
    df_final = df.groupby('ID').agg({**{f"{var}_accu": 'sum' for var in selected_vars}, 'DAYS': 'sum'}).reset_index()
    for var in selected_vars:
        df_final[var] = df_final[f"{var}_accu"] / df_final['DAYS']
    df_final.drop(columns=[f"{var}_accu" for var in selected_vars], inplace=True)
    return df_final[['ID'] + selected_vars]

# Merge address with exposome data
def merge_addr_expo(df_address, df_expo, time):
    merge_on = ['ZIP_9', 'YEAR']
    if time == 'MONTH':
        merge_on.append('MONTH')
    elif time == 'QUARTER':
        merge_on.append('QUARTER')
    return pd.merge(df_address, df_expo, on=merge_on, how='inner')

# Generate linkage for each category
def generate_linkage_for_category(df_address, category_name, df_category, time_period, selected_vars, target_start, target_end):
    if not selected_vars:
        return None
    
    # Create the new DataFrame with the filtered columns    
    fix_var = ['ZIP_9', 'YEAR', 'MONTH', 'QUARTER', 'YEAR-QUARTER']
    column_var = df_category.columns
    inner_var = [v for v in fix_var if v in column_var]
    df_category_new = df_category[inner_var + selected_vars]
    
    df_address_adjusted = adjust_dates(df_address, target_start, target_end)
    periods = split_periods(df_address_adjusted, time_period)
    df_periods = pd.DataFrame(periods, columns=['ID'] + inner_var + ['DAYS'])
    df_merged = merge_addr_expo(df_periods, df_category_new, time_period)
    linkage = time_weighted_averages(df_merged, selected_vars)
    print(f"{category_name} linkage completed.")
    return linkage

# Categorize and generate linkages in parallel with progress bar
def categorize_and_link_parallel(df_address, selected_vars, target_start, target_end):
    futures = {}
    category_vars = [
        ('NATA', df_nata, nata_vars, 'YEAR'),
        ('CACES', df_caces, caces_vars, 'YEAR'),
        ('WI', df_wi, wi_vars, 'YEAR'),
        ('FARA', df_fara, fara_vars, 'YEAR'),
        ('ACS', df_acs, acs_vars, 'YEAR'),
        ('UCR', df_ucr, ucr_vars, 'YEAR'),
        ('ACAG', df_acag, acag_vars, 'MONTH'),
        ('CBP', df_cbp, cbp_vars, 'MONTH'),
        ('HUD', df_hud, hud_vars, 'QUARTER')
    ]

    # Initialize progress bar
    with tqdm(total=len(category_vars), desc="Processing categories") as pbar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for category_name, df_category, vars_set, period in category_vars:
                selected_vars_cat = [v for v in selected_vars if v in vars_set]
                futures[category_name] = executor.submit(generate_linkage_for_category, df_address, category_name, df_category, period, selected_vars_cat, target_start, target_end)
                pbar.update(1)

    all_linkages = []
    for category, future in futures.items():
        try:
            result = future.result()
            if result is not None:
                all_linkages.append(result)
        except Exception as e:
            print(f"Error in {category}: {e}")

    if all_linkages:
        final_linkage = all_linkages[0]
        for linkage in all_linkages[1:]:
            final_linkage = pd.merge(final_linkage, linkage, on='ID', how='outer')
        return final_linkage
    else:
        return None

# Main function to run the linkage process
def main():
    start_time = time.time()  # Start timer to track performance
    df_address = pd.read_csv(ADDR_PATH)

    # User input for the project and time range
    proj_name = input('Type project name: ')
    target_start = pd.Timestamp(input('Type project start time (YYYY-MM-DD): '))
    target_end = pd.Timestamp(input('Type project end time (YYYY-MM-DD): '))

    print('Example variables: PM25, BC, WALKABILITY, religious, p_assault, LA1AND20, COKE OVEN EMISSIONS, AMS_RES')
    input_vars = input('Select your variable list (comma-separated): ')
    selected_vars = [var.strip() for var in input_vars.split(',')]

    # Run linkage in parallel
    final_linkage = categorize_and_link_parallel(df_address, selected_vars, target_start, target_end)

    if final_linkage is not None:
        final_linkage.to_csv(f'{proj_name}_linkage.csv', index=False)
        print(f"Linkage file saved as {proj_name}_linkage.csv")
        print(final_linkage.head())
    else:
        print("No linkages were generated.")
        
    # Print runtime
    print(f"Total runtime: {time.time() - start_time:.2f} seconds")
    
if __name__ == '__main__':
    main()
