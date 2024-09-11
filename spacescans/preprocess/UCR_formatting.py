import pandas as pd
import re

def extract_year_from_sheet_name(sheet_name):
    match = re.search(r'(\d{4})', sheet_name)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"No year found in the sheet name: {sheet_name}")

def standardize_column_names(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def rename_and_combine_columns(df):
    column_mapping = {
        'rape ^': 'fso',
        'aggravated assault^^': 'assault',
        'motor vehicle theft': 'mvt',
        'population': 'pop',
        'forcible sex offenses': 'fso',
    }
    df = df.rename(columns=column_mapping)
    
    # Combine 'rape ^' and 'forcible sex offenses' into a single 'fso' column if both exist
    if 'fso' in df.columns and 'forcible sex offenses' in df.columns:
        df['fso'] = df[['fso', 'forcible sex offenses']].bfill(axis=1).iloc[:, 0]
        df = df.drop(columns=['forcible sex offenses'])
    return df

def read_exposome_from_all_sheets(file_path):
    sheet_names = pd.ExcelFile(file_path).sheet_names
    all_data = []

    for sheet_name in sheet_names:
        year = extract_year_from_sheet_name(sheet_name)
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl', skiprows=2)
        df = df.drop(df.index[0])  #there is an empy row before the county data starts that needs to be removed for processing.
        df = df.head(67)  # Limit to the first 67 rows - this is due to the way the excel file is formatted in raw format dirctly from UCR site as a download

        df = standardize_column_names(df)
        df = rename_and_combine_columns(df)

        # Add the year column after the county column
        df.insert(df.columns.get_loc('county') + 1, 'YEAR', year)
        
        # Select only the necessary columns (this is based on the currently formatted ucr exposome file)
        cols_to_keep = [
            'county', 'YEAR', 'pop', 'murder', 'fso', 'robbery', 'assault', 
            'burglary', 'larceny', 'mvt'
        ]

        # Check if all required columns are present - this is not necessary after this formatter is done. Only for testing. 
        missing_cols = [col for col in cols_to_keep if col not in df.columns]
        if missing_cols:
            print(f"Missing columns in sheet '{sheet_name}': {missing_cols}")
            continue

        df = df[cols_to_keep]
        all_data.append(df)
    
    combined_data = pd.concat(all_data, ignore_index=True)
    return combined_data

def add_total_column(df):
    df['total'] = df[['murder', 'fso', 'robbery', 'assault', 'burglary', 'larceny', 'mvt']].sum(axis=1)
    return df

def add_percentage_columns(df):
     # Ensure the columns are numeric
    df['pop'] = pd.to_numeric(df['pop'], errors='coerce')
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    df['murder'] = pd.to_numeric(df['murder'], errors='coerce')
    df['fso'] = pd.to_numeric(df['fso'], errors='coerce')
    df['robbery'] = pd.to_numeric(df['robbery'], errors='coerce')
    df['assault'] = pd.to_numeric(df['assault'], errors='coerce')
    df['burglary'] = pd.to_numeric(df['burglary'], errors='coerce')
    df['larceny'] = pd.to_numeric(df['larceny'], errors='coerce')
    df['mvt'] = pd.to_numeric(df['mvt'], errors='coerce')

    # this is to avoid division by zero by replacing zero or missing values in 'pop' with NaN
    # however, this shouldn't be needed because pop should not be zero.
    df.replace({'pop': 'Nan'}, inplace=True)
    
    # Calculate the p_total percentage
    df['p_total'] = (df['total'] / df['pop']) * 100

    # Calculate the p_murder percentage
    df['p_murder'] = (df['murder'] / df['pop']) * 100

    # Calculate the p_fso percentage
    df['p_fso'] = (df['fso'] / df['pop']) * 100

    # Calculate the p_robbery percentage
    df['p_rob'] = (df['robbery'] / df['pop']) * 100

    # Calculate the p_assault percentage
    df['p_assault'] = (df['assault'] / df['pop']) * 100

    # Calculate the p_burglary percentage
    df['p_burglary'] = (df['burglary'] / df['pop']) * 100

    # Calculate the p_larceny percentage
    df['p_larceny'] = (df['larceny'] / df['pop']) * 100

    # Calculate the p_mvt percentage
    df['p_mvt'] = (df['mvt'] / df['pop']) * 100
    return df

def fips_mapping_dictionary(df):
    df['county'] = df['county'].str.replace('county', '', case=False, regex=True)

    
    county_to_fips = {
        'alachua'	:	'12001',
        'baker'	    :	'12003',
        'bay'	    :	'12005',
        'bradford'	:	'12007',
        'brevard'	:	'12009',
        'broward'	:	'12011',
        'calhoun'	:	'12013',
        'charlotte'	:	'12015',
        'citrus'	:	'12017',
        'clay'	    :	'12019',
        'collier'	:	'12021',
        'columbia'	:	'12023',
        'desoto'	:	'12027',
        'dixie'	    :	'12029',
        'duval'	    :	'12031',
        'escambia'	:	'12033',
        'flagler'	:	'12035',
        'franklin'	:	'12037',
        'gadsden'	:	'12039',
        'gilchrist'	:	'12041',
        'glades'	:	'12043',
        'gulf'	    :	'12045',
        'hamilton'	:	'12047',
        'hardee'	:	'12049',
        'hendry'	:	'12051',
        'hernando'	:	'12053',
        'highlands'	:	'12055',
        'hillsborough'	:	'12057',
        'holmes'	    :	'12059',
        'indian river'	:	'12061',
        'jackson'	    :	'12063',
        'jefferson'	    :	'12065',
        'lafayette'	    :	'12067',
        'lake'	        :	'12069',
        'lee'	        :	'12071',
        'leon'	        :	'12073',
        'levy'	        :	'12075',
        'liberty'	    :	'12077',
        'madison'	    :	'12079',
        'manatee'	    :	'12081',
        'marion'	    :	'12083',
        'martin'	    :	'12085',
        'miami-dade'	:	'12086',
        'monroe'	    :	'12087',
        'nassau'	    :	'12089',
        'okaloosa'	    :	'12091',
        'okeechobee'	:	'12093',
        'orange'	    :	'12095',
        'osceola'	    :	'12097',
        'osecola'       :   '12097',
        'palm beach'	:	'12099',
        'pasco'	        :	'12101',
        'pinellas'	    :	'12103',
        'polk'	        :	'12105',
        'putnam'	    :	'12107',
        'st. johns'	    :	'12109',
        'st. lucie'	    :	'12111',
        'santa rosa'	:	'12113',
        'sarasota'	    :	'12115',
        'seminole'	    :	'12117',
        'sumter'	    :	'12119',
        'suwannee'	    :	'12121',
        'taylor'	    :	'12123',
        'union'	        :	'12125',
        'volusia'	    :	'12127',
        'wakulla'	    :	'12129',
        'walton'	    :	'12131',
        'washington'	:	'12133',
        
    }
    
    # Standardize county names to lowercase for mapping
    df['county'] = df['county'].str.lower().str.strip()
    
    # Map counties to FIPS codes
    df['FIPS'] = df['county'].map(county_to_fips)

   # Drop the 'county' column
    df = df.drop(['county'], axis=1)

    # Move the 'fips' column to the beginning
    fips_column = df.pop('FIPS')
    df.insert(0, 'FIPS', fips_column)
    
    return df



def save_combined_exposome(dataframe, output_path):
    dataframe.to_csv(output_path, index=False)

def main():
    file_path = '/Users/allison.burns/Desktop/exposome/UCR/Total_Index_Crime_by_County.xlsx'
    output_path = '/Users/allison.burns/Desktop/exposome/UCR/formatted_ucr.csv'

    combined_data = read_exposome_from_all_sheets(file_path)
    combined_data = add_total_column(combined_data)
    combined_data = add_percentage_columns(combined_data)
    combined_data = fips_mapping_dictionary(combined_data)
    save_combined_exposome(combined_data, output_path)

if __name__ == '__main__':
    main()
